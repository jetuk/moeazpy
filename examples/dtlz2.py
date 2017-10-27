""" An example of using moeazpy on the DTLZ2 function """
from moeazpy.worker import Worker, service, WorkerInsertChildRequest, WorkerFetchChildRequest
import numpy as np
import logging
logger = logging.getLogger(__name__)


class DTLZ2Worker(Worker):
    """ This worker provides services to compute the DLTZ2 function """

    @service('evaluate.dtlz2')
    def dtlz2(self):

        req = WorkerFetchChildRequest()
        self.service_population_requests.append(req)


        k = 1


        nx = len(x)
        nz = nx - k + 1
        z = np.empty(nz)

        g = 0.0

        for i in range(nx-k, nx):
            g += (x[i] - 0.5)**2

        for i in range(nz):
            z[i] = 1.0 + g
            for j in range(nz-i-1):
                z[i] *= np.cos(0.5 * np.pi * x[j])
            if i != 0:
                z[i] *= np.sin(0.5 * np.pi * x[nz-i-1])

        return z

    @service('operate.random_uniform')
    def random_uniform(self, nx=10):
        logger.info('Creating uniform random candidate ...')
        child = np.random.rand(nx).tolist()

        # Add the child to service requests to update the population
        # TODO make this a convenience method
        req = WorkerInsertChildRequest(5, self, child)

        self.service_population_requests.append(req)



if __name__ == '__main__':
    from moeazpy.population import PopulationServer, Population
    from moeazpy.propagator import Propagator, PropagatorServer
    from moeazpy.broker import Broker, BrokerServer
    from moeazpy.worker import WorkerServer
    import threading
    import logging
    logging.basicConfig(level=logging.DEBUG)

    # Run the example

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

    # Create some workers
    worker_threads = []
    for i in range(1):
        worker = DTLZ2Worker()
        worker_server = WorkerServer(worker)
        worker_server.setup('moeazpy.population', 'moeazpy.broker')
        worker_thread = threading.Thread(target=worker_server.start)
        worker_threads.append(worker_thread)


    # Create the propagator
    prop = Propagator()
    prop_server = PropagatorServer(prop)
    prop_server.setup('moeazpy.population.publisher', 'moeazpy.broker.frontend')
    prop_thread = threading.Thread(target=prop_server.start)


    # Finally start the threads
    pop_thread.start()
    broker_thread.start()

    for wt in worker_threads:
        wt.start()


    prop_thread.start()


