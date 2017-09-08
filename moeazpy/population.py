from zmq.eventloop import ioloop, zmqstream
import zmq
from .core import ZmqServer, ReplyMessageHandler, ReplyMessage
from random import Random
import uuid
import json
import logging

logger = logging.getLogger(__name__)


class InsufficientPopulationEntries(Exception):
    pass


class UniqueIDAlreadyExists(Exception):
    pass

class UniqueIDNotFound(Exception):
    pass



class PopulationEntry:
    def __init__(self, variables, uid=None, parents=None):
        self.variables = variables
        if uid is None:
            uid = uuid.uuid4().hex
        self.uid = uid

        self.parents = parents

    def to_dict(self):
        """ Serialise to a dictionary """
        data = {
            'variables': self.variables,
            'uid': self.uid,
            'parents': self.parents,
        }
        return data

    def to_json(self):
        return json.dumps(self.to_dict())

    def transform_to_member(self, objectives, constraints):
        return PopulationMember(self.variables, objectives, constraints, uid=self.uid, parents=self.parents)


class PopulationMember(PopulationEntry):
    def __init__(self, variables, objectives, constraints, **kwargs):
        super().__init__(variables, **kwargs)
        self.objectives = objectives
        self.constraints = constraints



class Population:
    """ A representation of a Population of candidate solutions


    """
    def __init__(self, capacity=100, seed=None):
        self.capacity = capacity
        self.adults = {}
        self.adolescents = {}
        self.children = {}

        # Initialise and seed the random number generator
        # Note we use a stateful instance of `Random` rather than the
        # global random module. This means all
        self.random = Random()
        self.random.seed(seed)

    def fetch_random_adults(self, n=1):
        """ Return a list of a random sample of adults from the population """
        if len(self.adults) < n:
            raise InsufficientPopulationEntries('Population contains {:d} adults when {:d} '
                                                'adults were requested.'.format(len(self.children), n))

        uids = self.adults.keys()
        return [self.adults[uid] for uid in self.random.sample(uids, n)]

    def fetch_random_adolescents(self, n=1):
        """ Return a list of a random sample of adolescents from the population """
        if len(self.adults) < n:
            raise InsufficientPopulationEntries('Population contains {:d} adolescents when {:d} '
                                                'adolescents were requested.'.format(len(self.children), n))

        uids = self.adolescents.keys()
        return [self.adolescents[uid] for uid in self.random.sample(uids, n)]

    def fetch_random_children(self, n=1):
        """ Return a generator of a random sample of children from the population """
        if len(self.children) < n:
            raise InsufficientPopulationEntries('Population contains {:d} children when {:d} '
                                                'children were requested.'.format(len(self.children), n))

        uids = self.children.keys()
        return [self.children[uid] for uid in self.random.sample(uids, n)]

    def insert_child(self, variables, uid=None, parents=None):
        """ Insert a new child in to the population """

        new_child = PopulationEntry(variables, uid=uid, parents=parents)

        if new_child.uid in self.children:
            raise UniqueIDAlreadyExists('UID already exists in population: "{}"'.format(new_child.uid))

        self.children[new_child.uid] = new_child
        return new_child

    def grow_child(self, uid, objectives, constraints):
        """ Transition a child to an adolescent """

        # Convert the child from `PopulationEntry` to `PopulationMember`
        child = self.children.pop(uid)
        adolescent = child.transform_to_member(objectives, constraints)

        # If there is capacity in the adult population then add
        # otherwise it becomes an adolescent and must compete to enter the
        # adult population
        if len(self.adults) < self.capacity:
            self.adults[uid] = adolescent
        else:
            self.adolescents[uid] = adolescent
        return adolescent

    def dominate_adults(self, adolescent_uid, dominated_uids):

        # Check parents exist
        for uid in dominated_uids:
            if uid not in self.adults:
                raise UniqueIDNotFound('Unique ID "{}" not found in the adult population.'.format(uid))

        # Check adolescent exists
        if adolescent_uid not in self.adolescents:
            raise UniqueIDNotFound('Unique ID "{}" not found in the adolescent population.'.format(adolescent_uid))

        # Remove dominated adults
        for uid in dominated_uids:
            self.adults.pop(uid)

        # Promote the adolescent
        adolescent = self.adolescents.pop(adolescent_uid)
        self.adults[adolescent_uid] = adolescent

    def dominate_adolescent(self, adolescent_uid):

        # Check adolescent exists
        if adolescent_uid not in self.adolescents:
            raise UniqueIDNotFound('Unique ID "{}" not found in the adolescent population.'.format(adolescent_uid))

        # Simply remove the adolescent
        self.adolescents.pop(adolescent_uid)


class PopulationReply(ReplyMessage):
    @property
    def broker(self):
        return self.obj


class PopulationChildReply(PopulationReply):
    def __init__(self, pop, request_message, child, *args, **kwargs):
        super().__init__(pop, request_message, *args, **kwargs)
        self.child = child

    def make_message_content(self):
        return {
            'status': 'OK',
            'child': self.child.to_dict(),
        }

class PopulationInsufficientEntriesReply(PopulationReply):
    def make_message_content(self):
        return {
            'status': 'insufficient entries'
        }


class PopulationMessageHandler(ReplyMessageHandler):
    """ Handle reply messages for :class:`PopulationServer` """
    def __init__(self, stream, population):
        super().__init__(stream)
        self.population = population

    def capacity(self, **kwargs):
        """ Return the population's current capacity """
        self._send(kwargs['type'], self.population.capacity)

    def fetch_random_child(self, *args):
        sender, empty, msg = args
        assert empty == b''

        content = msg['content']  # content isn't used for this message

        try:
            child = self.population.fetch_random_children(n=1)[0]
        except InsufficientPopulationEntries:
            reply = PopulationInsufficientEntriesReply(self, msg)
        else:
            reply = PopulationChildReply(self, msg, child)

        self._reply(sender, reply)





class PopulationServer(ZmqServer):
    __server_name__ = 'population'

    def __init__(self, population):
        super().__init__()
        self.population = population

        # Server variables; initialised in `setup`
        self.publisher = None
        self.broadcaster = None
        self.frontend = None

    def setup(self, address):
        super().setup()

        publisher = self.context.socket(zmq.PUB)
        publisher.bind("ipc://{}.publisher".format(address))
        self.publisher = zmqstream.ZMQStream(publisher, self.loop)

        # Setup broadcast callback for status messages
        self.broadcaster = ioloop.PeriodicCallback(self.broadcast, 1000, self.loop)

        # Setup a reply socket for managing information about the population
        frontend = self.context.socket(zmq.REP)
        frontend.bind("ipc://{}.frontend".format(address))
        self.frontend = zmqstream.ZMQStream(frontend, self.loop)
        self.frontend.on_recv(PopulationMessageHandler(self.frontend, self.population))

    def start(self):
        self.broadcaster.start()

        # Special case to start the event loop last
        super().start()

    def broadcast(self):

        logging.info("Broadcasting population status message ...")

        msg = {
            'type': 'status',
            'originator': self.uid,
            'content': {
                'capacity': self.population.capacity,
                'adults': len(self.population.adults),
                'adolescents': len(self.population.adolescents),
                'children': len(self.population.children),
            }
        }

        self.publisher.send_json(msg)
