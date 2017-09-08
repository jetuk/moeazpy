from zmq.eventloop import ioloop, zmqstream
import zmq
import json
from .core import ZmqServer, MessageHandler, Request, make_message
import random
import time
import uuid
from threading import Thread
import logging

logger = logging.getLogger(__name__)


class WorkerRequest(Request):
    def __init__(self, timeout, worker, **kwargs):
        super().__init__(timeout, **kwargs)
        self.worker = worker


class WorkerRegisterRequest(WorkerRequest):
    __message_type__ = "worker_register"

    def make_message_content(self):
        return {
            'services': list(self.worker.services)
        }


class WorkerReadyRequest(WorkerRequest):
    __message_type__ = "worker_ready"


class WorkerTaskUpdateRequest(WorkerRequest):
    __message_type__ = "worker_task_update"

    def make_message_content(self):
        return {
            'status': 'running'
        }


class WorkerTaskFinishedRequest(WorkerRequest):
    __message_type__ = "worker_task_finished"

    def make_message_content(self):
        return {
            'status': 'complete'
        }


class WorkerClass(type):
    """ Metaclass for Worker which adds magic for registering services """
    def __new__(metacls, name, bases, namespace, **kwds):
        obj = type.__new__(metacls, name, bases, namespace, **kwds)
        obj._services = {attr._service: attr for attr in namespace.values() if hasattr(attr, '_service')}
        return obj


def service(name):
    def inner(func):
        func._service = name
        return func
    return inner


class Worker(metaclass=WorkerClass):
    def __init__(self):
        self.registered = False
        self.working = False
        self.thread = None

    def generate_request(self, ):

        # If we're not registered then try to register.
        if not self.registered:
            # If not registered
            logger.debug('Generating registration request.')
            req = WorkerRegisterRequest(5, self)
            return req

        if not self.working:
            # We're registered but not doing any work.
            # Create request to get work from the broker
            logger.debug('Generating ready request.')
            req = WorkerReadyRequest(5, self)
            return req

        # Registered and doing working
        # TODO handle self.thread not be set correctly
        if self.thread.is_alive():
            # Worker is still working; send an update to the broker
            logger.debug('Generating task update request.')
            req = WorkerTaskUpdateRequest(5, self)
            return req
        else:
            # Worker thread has finished.
            logger.debug('Generating task finished request.')
            req = WorkerTaskFinishedRequest(5, self)

            # Tidy up worker thread
            self.thread = None
            self.working = False

            return req

    def dispatch(self, service_name, args, kwargs):
        """  Dispatch a new service """
        self.working = True

        func = self._services[service_name]
        # Create the service Thread
        thread = func(self, *args, **kwargs)
        logger.info('Starting worker thread: "{}"'.format(service_name))
        thread.start()
        self.thread = thread

    @property
    def services(self):
        return self._services.keys()

    @service('operate.uniform_random')
    def uniform_random(self):
        import random
        class MyThread(Thread):
            def __init__(self, n, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.n = n

            def run(self):
                for i in range(self.n):
                    logger.info("MyThread is running and working hard!")
                    time.sleep(1)

        return MyThread(random.randint(1, 10))



class WorkerMessageHandler(MessageHandler):
    def __init__(self, stream, worker, server):
        self.stream = stream
        self.worker = worker
        self.server = server

    def worker_register_reply(self, *args):
        # Current request has been replied to
        self.server.current_request = None

        msg = args[-1]
        content = msg['content']
        if content['status'] == 'OK':
            logger.info("Successfully registered with the broker!")
            self.worker.registered = True
        else:
            logger.warning('Failed to register with the broker: "{}"'.format(content['status']))
            self.worker.registered = False

    def worker_ready_reply(self, *args):
        self.server.current_request = None

        msg = args[-1]
        content = msg['content']

        # TODO make this constants
        if content['status'] == 'new task':
            logger.info('New task received from broker for service: "{}"'.format(content['service']))
            print(content)
            self.worker.dispatch(content['service'], content['args'], content['kwargs'])
        elif content['status'] == 'no tasks':
            logger.info('No work available from the broker.')
            self.worker.working = False
        else:
            logger.warning('Broker reply not understood: "{}"'.format(content['status']))
            self.worker.working = False

    def worker_task_update_reply(self, *args):
        self.server.current_request = None

        msg = args[-1]
        content = msg['content']
        if content['status'] == 'OK':
            logger.info("Successfully updated task with broker.")
        else:
            logger.warning('Failed to update task with the broker: "{}"'.format(content['status']))

    def worker_task_finished_reply(self, *args):
        self.server.current_request = None

        msg = args[-1]
        content = msg['content']
        if content['status'] == 'OK':
            logger.info("Successfully finished task with broker.")
        else:
            # TODO try to send finish request again?
            logger.warning('Failed to finish task with the broker: "{}"'.format(content['status']))



class WorkerServer(ZmqServer):
    def __init__(self, worker, ):
        super().__init__()
        self.worker = worker

        self.broker_connection = None
        self.ticker = None
        self.current_request = None

    def setup(self, pop_server_address, broker_server_address):
        super().setup()

        # Connection to broker
        broker_connection = self.context.socket(zmq.REQ)
        broker_connection.connect("ipc://{}".format(broker_server_address))
        logger.info('Connected to broker: {}'.format(broker_server_address))
        self.broker_connection = zmqstream.ZMQStream(broker_connection, self.loop)
        self.broker_connection.on_recv(WorkerMessageHandler(self.broker_connection, self.worker, self))

        # Setup tick callback
        self.ticker = ioloop.PeriodicCallback(self.tick, 1000, self.loop)

    def start(self):
        self.ticker.start()
        super().start()

    def tick(self):

        # TODO check broker connection timeout

        if self.current_request is not None:
            # Currently processing a request
            # TODO check timeout.
            return

        req = self.worker.generate_request()

        if req is not None:
            logger.debug('Sending request to broker: "{}"'.format(req.__message_type__))
            self.broker_connection.send_json(req.make_message())
            self.current_request = req
