from .population import PopulationServer, Population
from .propagator import Propagator, PropagatorServer
from .broker import Broker, BrokerServer
from .worker import Worker, WorkerServer
import threading
import logging

logger = logging.getLogger('moeazpy')


def start_population_server():
    logging.basicConfig(level=logging.DEBUG)

    pop = Population()
    server = PopulationServer(pop)

    server.setup('moeazpy.population')
    server.start()


def start_propagator_server():
    logging.basicConfig(level=logging.DEBUG)

    prop = Propagator()
    server = PropagatorServer(prop)

    server.setup('moeazpy.population.publisher', 'moeazpy.broker.frontend')
    server.start()


def start_broker_server():
    logging.basicConfig(level=logging.DEBUG)

    broker = Broker()

    server = BrokerServer(broker)

    server.setup('moeazpy.broker')
    server.start()


def start_worker_server():
    logging.basicConfig(level=logging.DEBUG)

    worker = Worker()

    server = WorkerServer(worker)

    server.setup('moeazpy.population.publisher', 'moeazpy.broker.frontend')
    server.start()


def start_local_algorithm():
    logging.basicConfig(level=logging.DEBUG)

    # Create the population
    pop = Population(capacity=10)
    pop_server = PopulationServer(pop)
    pop_server.setup('moeazpy.population')
    pop_thread = threading.Thread(target=pop_server.start)

    # Create the broker
    broker = Broker()
    broker_server = BrokerServer(broker)
    broker_server.setup('moeazpy.broker')
    broker_thread = threading.Thread(target=broker_server.start)

    # Create the propagator
    prop = Propagator()
    prop_server = PropagatorServer(prop)
    prop_server.setup('moeazpy.population.publisher', 'moeazpy.broker.frontend')
    prop_thread = threading.Thread(target=prop_server.start)

    # Finally start the threads
    pop_thread.start()
    broker_thread.start()
    prop_thread.start()




