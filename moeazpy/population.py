import logging

import zmq
from zmq.eventloop import ioloop, zmqstream

from moeazpy.clients.population import InsufficientPopulationEntries
from .core import ZmqServer, ReplyMessageHandler, ReplyMessage, OKReply, ErrorReply

logger = logging.getLogger(__name__)


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

    def insert_child(self, *args):
        """ Request for a new child to be inserted in to the population """
        #sender, empty, msg = args
        #assert empty == b''
        msg = args[-1]

        content = msg['content']  # content isn't used for this message
        try:
            child = content['child']
        except KeyError:
            reply = ErrorReply(self, msg)
        else:
            self.population.insert_child(child)
            reply = OKReply(self, msg)

        self._reply(reply)


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
