from zmq.eventloop import ioloop, zmqstream
import zmq
import json
from .core import ZmqServer, MessageHandler, Request, ReplyMessage
import random
import time
import uuid
from collections import defaultdict
import logging
import re

logger = logging.getLogger(__name__)


class ForwardedRequest(Request):
    pass


class NoRegisteredWorkers(RuntimeError):
    pass


class NoServicesFoundError(RuntimeError):
    pass


class NoFreeWorkers(RuntimeError):
    pass


class NoPendingServiceRequests(RuntimeError):
    pass


class WorkerNotRegistered(RuntimeError):
    pass


class WorkerRegistrationError(RuntimeError):
    pass


class BrokerReply(ReplyMessage):
    @property
    def broker(self):
        return self.obj


class BrokerOKReply(BrokerReply):
    def make_message_content(self):
        return {
            'status': 'OK'
        }


class BrokerErrorReply(BrokerReply):
    def make_message_content(self):
        return {
            'status': 'error'
        }


class BrokerWorkerNotRegisteredReply(BrokerReply):
    def make_message_content(self):
        return {
            'status': 'worker not registered'
        }


class BrokerNoPendingServiceRequestsReply(BrokerReply):
    def make_message_content(self):
        return {
            'status': 'no tasks'
        }


class BrokerTaskReply(BrokerReply):
    def __init__(self, broker, request_message, service_request, *args, **kwargs):
        super().__init__(broker, request_message, *args, **kwargs)
        self.service_request = service_request

    def make_message_content(self):
        return dict(status='new task', **self.service_request.make_message_content())


class BrokerServiceRequest:
    def __init__(self, client, name, attributes, args=None, kwargs=None, start_time=None):
        self.client = client
        self.name = name
        self.attributes = attributes
        self.args = args
        self.kwargs = kwargs

        if start_time is None:
            start_time = time.time()
        self.start_time = start_time

        self.uid = uuid.uuid4().hex

    @property
    def is_timed_out(self):
        return (time.time() - self.start_time) > self.timeout

    def make_message_content(self):
        return {
            'service_name': self.name,
            'service_attributes': self.attributes,
            'service_args': self.args,
            'service_kwargs': self.kwargs,
            'uid': self.uid
        }


class BrokerService:
    def __init__(self, broker, name):
        self.broker = broker
        self.name = name
        self._registered_workers = set()
        self.waiting_service_requests = []
        self.pending_service_requests = []
        self.complete_service_requests = []

    def register_worker(self, worker_id):
        self._registered_workers.add(worker_id)

    def unregister_worker(self, worker_id):
        if worker_id not in self._registered_workers:
            raise ValueError('Worker is not registered for this service: "{}"'.format(worker_id))

        self._registered_workers.remove(worker_id)

    @property
    def available_workers(self):
        return self._registered_workers.intersection(self.broker._free_workers)

    def append_service_request(self, client, attributes, args=None, kwargs=None):

        if args is None:
            args = []
        if kwargs is None:
            kwargs = {}

        service_request = BrokerServiceRequest(client, self.name, attributes, args=args, kwargs=kwargs)
        self.waiting_service_requests.append(service_request)

    def find_worker_task(self, worker_id):

        if worker_id not in self._registered_workers:
            raise WorkerNotRegistered()

        try:
            # Find a task and return
            req = self.waiting_service_requests.pop(0)
        except IndexError:
            # No pending requests
            raise NoPendingServiceRequests()
        else:
            # Register as a pending request
            self.pending_service_requests.append(req)
        return req

    def complete_task(self):
        pass



class Broker:
    """ Service broker for managing client requests to workers

    """
    # TODO add some more stuff the docstring
    def __init__(self, ):
        self._registered_workers = set()
        self.services = {}

    def ensure_service(self, service_name):
        """ Fetch (or create and fetch) a :class:`BrokerService` instance for the corresponding `service_name`

        """
        try:
            s = self.services[service_name]
        except KeyError:
            # Create new service
            s = BrokerService(self, service_name)
            self.services[service_name] = s
            logger.info('Created new service: "{}"'.format(service_name))
        return s

    def find_services(self, service_name):
        """ Find existing services using regex pattern matching.

        """
        for name in self.services:
            if re.match(service_name, name):
                yield name

    def register_worker(self, worker_id, services):
        """ Register a new worker """
        if len(services) == 0:
            raise WorkerRegistrationError('Must register at least one service.')

        for service_name in services:
            s = self.ensure_service(service_name)
            s.register_worker(worker_id)

        self._registered_workers.add(worker_id)
        logger.info('Registered new worker with id: "{}"'.format(worker_id))

    def unregister_worker(self, worker_id, ):
        """ Unregister a worker """

        if worker_id not in self._registered_workers:
            raise ValueError('Worker is not registered on this broker: "{}"'.format(worker_id))

        # Remove from services first
        for s in self.services.values():
            try:
                s.unregister_worker(worker_id)
            except ValueError:
                # Don't worry if it wasn't registered
                pass

        # Finally remove from broker
        self._registered_workers.remove(worker_id)

    def request_service(self, client_id, service_name, service_attributes, service_args=None, service_kwargs=None):

        # Match available services
        feasible_services = list(s for s in self.find_services(service_name))

        if len(feasible_services) == 0:
            raise NoServicesFoundError('No registered services matching name: "{}"'.format(service_name))

        # Pick one service at random
        s = random.choice(feasible_services)
        s = self.services[s]

        if len(s._registered_workers) == 0:
            raise NoRegisteredWorkers('No workers registered to provide this service: "{}"'.format(service_name))

        # register the service request
        s.append_service_request(client_id, service_attributes, args=service_args, kwargs=service_kwargs)

    @property
    def waiting_service_requests(self):
        """ A generator for pending requests from all services """
        for service_name, s in self.services.items():
            for req in s.waiting_service_requests:
                yield service_name, req

    @property
    def pending_service_requests(self):
        """ A generator for pending requests from all services """
        for service_name, s in self.services.items():
            for req in s.pending_service_requests:
                yield service_name, req

    def find_worker_task(self, worker_id):
        """ Search pending requests for task for this worker

        Simply returns the first task that worker is capable of undertaking
        """
        # Simple flag so we can return whether there are tasks or if this
        # worker is simply not registered with any services.
        is_registered = False
        for s in self.services.values():

            try:
                return s.find_worker_task(worker_id)
            except WorkerNotRegistered:
                pass  # Worker can't do this service
            except NoPendingServiceRequests:
                is_registered = True  # worker can do this service

        # No tasks found!
        if is_registered:
            raise NoPendingServiceRequests()
        else:
            raise WorkerNotRegistered()


class BrokerMessageHandler(MessageHandler):
    def __init__(self, stream, broker, server):
        super().__init__()
        self.stream = stream
        self.broker = broker
        self.server = server

    def _reply(self, sender, reply):

        # TODO fix originator
        msg = reply.make_message()
        msg = json.dumps(msg).encode('utf-8')

        self.stream.send_multipart([sender, b'', msg])

    def request_service(self, *args):
        sender, empty, msg = args
        assert empty == b''
        content = msg['content']

        service_name = content.get('service_name')
        service_attributes = content.get('service_attributes', {})
        service_args = content.get('service_args', [])
        service_kwargs = content.get('service_kwargs', {})

        try:
            self.broker.request_service(sender, service_name, service_attributes, service_args, service_kwargs)
        except RuntimeError:
            # TODO handle the expected exceptions better
            logger.exception('Exception when processing service request: "{}""'.format(service_name))
            self._reply(sender, BrokerErrorReply(self, msg))
        else:
            self._reply(sender, BrokerOKReply(self, msg))

    def worker_register(self, *args):
        """ Handle request to register a worker """
        sender, empty, msg = args
        assert empty == b''

        content = msg['content']
        # TODO add error checking
        self.broker.register_worker(sender, content['services'])

        reply = BrokerOKReply(self, msg)

        self._reply(sender, reply)

    def worker_ready(self, *args):
        sender, empty, msg = args
        assert empty == b''

        try:
            # Find a task for this worker
            req = self.broker.find_worker_task(sender)
            logger.info('Sending new task to worker: {}'.format(req))
            reply = BrokerTaskReply(self, msg, req)
        except WorkerNotRegistered:
            reply = BrokerWorkerNotRegisteredReply(self, msg)
        except NoPendingServiceRequests:
            reply = BrokerNoPendingServiceRequestsReply(self, msg)

        self._reply(sender, reply)

    def worker_task_update(self, *args):
        sender, empty, msg = args
        assert empty == b''

        content = msg['content']

        logger.info('Received update for task with status: "{}"'.format(content['status']))
        # Now send reply.
        reply = BrokerOKReply(self, msg)
        self._reply(sender, reply)

    def worker_task_finished(self, *args):
        sender, empty, msg = args
        assert empty == b''
        content = msg['content']
        print(content)
        logger.info('Task finished with status: "{}"'.format(content['status']))
        # Now send reply.
        reply = BrokerOKReply(self, msg)
        self._reply(sender, reply)


class BrokerServer(ZmqServer):
    def __init__(self, broker):
        super().__init__()
        self.broker = broker

        self.frontend = None

    def setup(self, address):
        super().setup()

        frontend = self.context.socket(zmq.ROUTER)
        frontend.bind("ipc://{}.frontend".format(address))
        self.frontend = zmqstream.ZMQStream(frontend, self.loop)
        self.frontend.on_recv(BrokerMessageHandler(self.frontend, self.broker, self))


