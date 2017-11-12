import json
import uuid
from random import Random

from ..core import ZmqClient


class PopulationClient(ZmqClient):
    pass


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