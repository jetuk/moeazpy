from zmq.eventloop import ioloop, zmqstream
import zmq
import json
from .core import ZmqServer, MessageHandler, Request
import time
import uuid
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


class OperatorRequest(Request):
    __message_type__ = 'request_service'

    def __init__(self, timeout, population_uid, min_parents=1, max_parents=None, **kwargs):
        super().__init__(timeout, **kwargs)
        self.population_uid = population_uid
        self.min_parents = min_parents
        self.max_parents = max_parents

    def make_message_content(self):
        return {
            'service_name': 'operate.*',
            'service_attributes': {
                'max_parents': self.max_parents,
                'min_parents': self.min_parents,
                'population_uid': self.population_uid
            }
        }


class EvaluationRequest(Request):
    __message_type__ = 'request_service'

    def __init__(self, timeout, population_uid, child_uid=None, **kwargs):
        super().__init__(timeout, **kwargs)
        self.population_uid = population_uid
        self.child_uid = child_uid

    def make_message_content(self):
        return {
            # TODO the propagator should be more specific about how its children are evaluated?
            'service_name': 'evaluate.*',
            'service_attributes': {
                'child_uid': self.child_uid,
                'population_uid': self.population_uid
            }
        }



class Propagator:
    """

    """
    def __init__(self, target_child_proportion=0.1, max_concurrent_child_evaluations=2):
        self.target_child_proportion = target_child_proportion
        self.max_concurrent_child_evaluations = max_concurrent_child_evaluations

        self.population_status = {}
        self.requested_operations = defaultdict(dict)
        self.requested_evaluations = defaultdict(dict)

    def set_population_status(self, population_id, capacity, members, children):
        logger.info('Updating population status: {}: {}, {}, {}'.format(population_id, capacity, members, children))
        self.population_status[population_id] = {
            'capacity': capacity,
            'adults': members,
            'children': children,
        }

    def generate_requests(self):
        """ Generate requests for each population """

        for pop_id, info in self.population_status.items():
            logger.info('Determining requests for population: {}'.format(pop_id))
            # Current operations are creating more children in the population
            current_operations = self.requested_operations[pop_id]
            # Current evaluations are turning children in to adults of the population
            current_evaluations = self.requested_evaluations[pop_id]

            capacity = info['capacity']
            adults = info['adults']
            children = info['children']

            # Future size of the population is estimated as current size plus children, plus
            # children being created and children being evaluated
            # TODO this inherently assumes that only a single :class:`Propagator` is operating
            # on the population. Not unreasonable for the time being, but might go a bit haywire if this
            # is not the case.

            future_size = adults + children + len(current_operations) + len(current_evaluations)

            max_parents = None
            min_parents = 1
            children_to_generate = 0
            children_to_evaluate = 0

            if len(current_evaluations) < self.max_concurrent_child_evaluations:
                children_to_evaluate = self.max_concurrent_child_evaluations - len(current_evaluations)

            if future_size < capacity:
                # We estimate the future size to be less than the population capacity
                # Attempt to generate enough children to fill the population
                # Less any that already exists or are about to exist
                children_to_generate = capacity - children - len(current_operations)

                if adults == 0:
                    # This is the beginning of the population.
                    # So lets use operators that can create children with no parents
                    max_parents = 0
            else:
                # Population is already at or above capacity
                # Try to keep a fixed proportion of the population turning over
                # as children
                expected_children = capacity * self.target_child_proportion
                # Calculate expected future child population
                future_children = children + len(current_operations)

                if future_children < expected_children:
                    # Not enough children; create some more
                    children_to_generate = expected_children - future_children

            logger.info('Generating {} child evaluation requests for population: {}'.format(children_to_evaluate, pop_id))

            for _ in range(children_to_evaluate):
                # Create a request for each evaluation required
                req = EvaluationRequest(5, pop_id, )
                self.requested_evaluations[pop_id][req.uid] = req
                yield req

            logger.info('Generating {} child operation requests for population: {}'.format(children_to_generate, pop_id))

            for _ in range(children_to_generate):
                # Create a request for each child required
                req = OperatorRequest(5, pop_id, min_parents=min_parents, max_parents=max_parents)
                self.requested_operations[pop_id][req.uid] = req
                yield req

    def check_request_timeouts(self):

        for pop_id, requests in self.requested_operations.items():
            requests_to_delete = []
            for req_uid, req in requests.items():
                if req.is_timed_out:
                    requests_to_delete.append(req_uid)

            for req_uid in requests_to_delete:
                requests.pop(req_uid)


class PropagatorMessageHandler(MessageHandler):
    """ Handler for messages from subscription to :class:`Population` broadcasts

    """
    def __init__(self, request_stream, propagator):
        self.request_stream = request_stream
        self.propagator = propagator

    def status(self, *args):
        # messages must have a content keyword
        msg = args[-1]
        content = msg.get('content')
        originator = msg.get('originator')
        logger.info('Handling status message from: {}'.format(originator))

        capacity = content['capacity']  # maximum capacity of the population
        members = content['adults']  # current number of adults
        children = content['children']  # current number of children

        # Update internal state for this population
        self.propagator.set_population_status(originator, capacity, members, children)


class PropagatorBrokerMessageHandler(MessageHandler):
    def __init__(self, request_stream, propagator):
        self.request_stream = request_stream
        self.propagator = propagator

    def request_service_reply(self, *args):
        msg = args[-1]

        logger.info(msg)


class PropagatorServer(ZmqServer):
    def __init__(self, propagator):
        super().__init__()

        self.propagator = propagator

        # Server variables; initialised in `setup`
        self.population_subscription = None
        self.broker_connection = None

    def setup(self, pop_server_address, broker_server_address):
        super().setup()

        pop_subscription = self.context.socket(zmq.SUB)
        pop_subscription.connect("ipc://{}".format(pop_server_address))
        logger.info('Connected to population subscription: {}'.format(pop_server_address))
        pop_subscription.setsockopt(zmq.SUBSCRIBE, b"")

        self.population_subscription = zmqstream.ZMQStream(pop_subscription, self.loop)
        self.population_subscription.on_recv(PropagatorMessageHandler(None, self.propagator))

        # Connection to broker
        broker_connection = self.context.socket(zmq.REQ)
        broker_connection.connect("ipc://{}".format(broker_server_address))
        logger.info('Connected to broker: {}'.format(broker_server_address))
        self.broker_connection = zmqstream.ZMQStream(broker_connection, self.loop)
        self.broker_connection.on_recv(PropagatorBrokerMessageHandler(self.broker_connection, self.propagator))

        # Setup tick callback
        self.ticker = ioloop.PeriodicCallback(self.tick, 1000, self.loop)

    def start(self):
        self.ticker.start()
        super().start()

    def tick(self):
        """ Tick the propagator along """

        logger.debug('Ticking propagator ...')
        self.propagator.check_request_timeouts()

        for req in self.propagator.generate_requests():
            logger.debug('Sending request on population: {}.'.format(req.population_uid))
            self.broker_connection.send_json(req.make_message())

        logger.debug('Tick complete!')



