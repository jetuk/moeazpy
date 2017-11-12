from zmq.eventloop import ioloop, zmqstream
import zmq
import multiprocessing
import threading
import json
import uuid
import time
import logging
from .state import StateMachine

logger = logging.getLogger(__name__)


class ClientServiceManager:
    """ Manager for clients providing a particular service. """
    def __init__(self, server, name, clients=None):
        self.server = server
        # Use a private variable and property to prevent accidental renaming
        self._name = name
        # Default to empty set of clients
        if clients is None:
            clients = set()
        self.clients = clients
        # Service request
        self.requests = {}

    @property
    def name(self):
        return self._name

    @property
    def queued_requests(self):
        for req in self.requests.values():
            if req.status == 'queueing':
                yield req

    def sorted_queued_requests(self):
        return sorted(self.queued_requests, key=lambda req: req.creation_time)

    def next_queued_request(self):
        for req in self.sorted_queued_requests():
            return req

    def add_service_request(self, client, args=None, kwargs=None):
        """ Add a new service request to the manager from client """
        req = ClientServiceRequest(self, client, args=args, kwargs=kwargs)
        self.requests[req.uid] = req
        return req

    def get_service_request_status(self, uid):
        """ Return the status of a service request """
        req = self.requests[uid]
        return req.status


class ClientServiceRequest:
    def __init__(self, manager, client, args=None, kwargs=None, creation_time=None, worker=None):
        self.manager = manager
        self.client = client
        self.worker = worker
        self.args = args
        self.kwargs = kwargs

        if creation_time is None:
            creation_time = time.time()
        self.creation_time = creation_time
        self.dispatch_time = None
        self.complete_time = None

        # Unique ID for this request
        self.__uid = uuid.uuid4().hex

    @property
    def uid(self):
        return self.__uid

    @property
    def is_timed_out(self):
        return (time.time() - self.start_time) > self.timeout

    @property
    def status(self):
        """ Return current status string """
        if self.complete_time is not None:
            return 'complete'
        elif self.dispatch_time is not None:
            return 'running'
        else:
            return 'queueing'


class ZmqServer(multiprocessing.Process):
    """ Base class for all zmq classes """
    def __init__(self, protocol, address, state_machine_factory=None):
        super().__init__()
        self.uid = uuid.uuid4().hex
        if state_machine_factory is None:
            # TODO make this using the generic parent class
            from .state import MoeaServerStateMachine
            state_machine_factory = MoeaServerStateMachine
        self.state_machine_factory = state_machine_factory
        self.protocol = protocol
        self.address = address

        # Server variables; initialised in `setup`
        self.context = None
        self.loop = None

        # Client states
        self._client_states = {}
        # Client service managers
        self._client_service_managers = {}

    def setup(self):
        """ Setup the zmq server

        This must be done before calling `start()`


        """
        logger.info('Setting up server at address: "{}"'.format(self.address))
        self.context = zmq.Context()
        self.loop = ioloop.IOLoop()

        frontend = self.context.socket(zmq.ROUTER)
        frontend.bind("ipc://{}".format(self.address))
        self.frontend = zmqstream.ZMQStream(frontend, self.loop)
        self.frontend.on_recv(self._handle_message)

    def run(self):
        self.setup()

        logger.info('Starting {} server with uid: {}'.format(self.__module__ + "." + self.__class__.__name__, self.uid))
        # Start the event loop
        self.loop.make_current()
        try:
            self.loop.start()
        except KeyboardInterrupt:
            pass

    def stop(self):
        logger.info('Stopping {} server with uid: {}'.format(self.__module__ + "." + self.__class__.__name__, self.uid))
        self.loop.stop()

    def _ensure_client_state_machine(self, client):
        """ Return (by fetch or create) a state machine for the given client """

        try:
            sm = self._client_states[client]
        except KeyError:
            # Create a new state machine to manage this client
            sm = self.state_machine_factory(self, client, self.protocol)
            self._client_states[client] = sm
        return sm

    def _ensure_client_service_manager(self, service_name):
        """ Return (by fetch or create) a client service manager for the given service """
        try:
            sm = self._client_service_managers[service_name]
        except KeyError:
            sm = ClientServiceManager(self, service_name)
            self._client_service_managers[service_name] = sm
        return sm

    def _handle_message(self, frames):
        """ Handle a received message from a client """
        client, empty, request = frames  # We only expect to receive single frame messages
        logger.debug('Handling message from client: "{}"'.format(client))
        assert empty == b""  # Second frame should be empty

        # Find the corresponding internal state machine for this client
        sm = self._ensure_client_state_machine(client)

        # Handle the message inside the corresponding state machine
        # TODO this should probably be asynchronous using a process pool perhaps?
        request = json.loads(request.decode('utf-8'))
        sm.process_request(request)

        reply = sm.reply_message
        if reply is None:
            reply = self.protocol.build_reply('error')

        # Now reply to the client
        self.frontend.send_multipart([client, empty, reply])

    def register_client_service(self, client, service_name):
        """ Register client as providing a service """
        service_manager = self._ensure_client_service_manager(service_name)
        service_manager.clients.add(client)

    def unregister_client_service(self, client, service_name):
        """ Unregister client as providing a service """
        service_manager = self._ensure_client_service_manager(service_name)
        service_manager.clients.remove(client)

    def request_client_service(self, client, service_name, args=None, kwargs=None):
        """ Request a client service """
        service_manager = self._ensure_client_service_manager(service_name)
        # Create a new service request
        service_request = service_manager.add_service_request(client, args=args, kwargs=kwargs)
        return service_request

    def request_client_service_status(self, client, service_name, uid):
        """ Return the status for a given request uid """
        service_manager = self._ensure_client_service_manager(service_name)
        # TODO any client can request the status of any request
        return service_manager.get_service_request_status(uid)

    def dispatch_client_task(self, client):
        """ Find a task for a client to undertake """

        for service_name, service_manager in self._client_service_managers.items():
            if not client in service_manager.clients:
                # Only find work for services the client is registered to perform
                continue

            req = service_manager.next_queued_request()
            if req is not None:
                # This client, requesting a task, is going to be the worker for this task
                req.worker = client
                req.dispatch_time = time.time()
                return req

    def complete_task(self, uid):
        """ Register a task as complete """
        for service_manager in self._client_service_managers.values():
            try:
                req = service_manager.requests[uid]
            except KeyError:
                continue
            req.complete_time = time.time()
            return

        raise ValueError('Request with UID "{}" not found.'.format(uid))


def service(name):
    """ Decorator to label a func as a service """
    def inner(func):
        func._service = name
        return func
    return inner


class ZmqClientClass(type):
    """ Metaclass for Worker which adds magic for registering services """
    def __new__(metacls, name, bases, namespace, **kwds):
        obj = type.__new__(metacls, name, bases, namespace, **kwds)
        obj._services = {attr._service: attr for attr in namespace.values() if hasattr(attr, '_service')}
        return obj


class ZmqClient(multiprocessing.Process, metaclass=ZmqClientClass):
    def __init__(self, protocol, address):
        super().__init__()
        self.uid = uuid.uuid4().hex
        self.protocol = protocol
        self.address = address

        self.server_connection = None
        self._connected = False
        self._working = False
        self._working_thread = None
        self._task_uid = None
        self._services_registered = None
        self._current_request = None

        # Variables; initialised in `setup`
        self.context = None
        self.loop = None
        self.ticker = None

    @property
    def connected(self):
        return self._connected

    @property
    def working(self):
        return self._working

    @property
    def current_request_type(self):
        if self._current_request is not None:
            return self.protocol.get_message_type(self._current_request)

    @property
    def current_request_body(self):
        if self._current_request is not None:
            return self.protocol.get_message_body(self._current_request)

    def setup(self):

        self.context = zmq.Context()
        self.loop = ioloop.IOLoop()

        # Connection to broker
        connection = self.context.socket(zmq.REQ)
        connection.connect("ipc://{}".format(self.address))
        logger.info('Connected to broker: {}'.format(self.address))
        self.server_connection = zmqstream.ZMQStream(connection, self.loop)
        self.server_connection.on_recv(self._handle_message)

        # Setup the callback
        self.ticker = ioloop.PeriodicCallback(self.tick, 1000, self.loop)
        # Initialise to no services registered
        self._services_registered = set()
        self._connected = False
        self._working = False
        self._working_thread = None
        self._task_uid = None
        self._current_request = None

    def _handle_message(self, frames):
        """ Handle a received reply from server """
        reply = frames[-1]  # We only expect to receive single frame messages
        logger.debug('Handling reply from server.')

        reply = json.loads(reply.decode('utf-8'))

        action = '{}_reply'.format(self.current_request_type)
        attr = getattr(self, action)

        try:
            attr(reply)
        except Exception:
            logger.exception('Handling reply from server for "{}" caused an exception.'.format(self.current_request_type))
        finally:
            # Request-reply complete
            self._current_request = None

    def send_request(self, message_type, data=None):
        logger.info('Sending new "{}" request to the server.'.format(message_type))
        msg = self.protocol.build_request(message_type, data=data, encode=False)
        self._current_request = msg
        # Now encode
        encoded_msg = self.protocol.encode_message(msg)
        self.server_connection.send(encoded_msg)

    def run(self):
        self.setup()
        logger.info('Starting {} client with uid: {}'.format(self.__module__ + "." + self.__class__.__name__, self.uid))
        # Start the event loop
        self.loop.make_current()
        self.ticker.start()
        try:
            self.loop.start()
        except KeyboardInterrupt:
            pass

    def stop(self):
        self.loop.stop()

    def tick(self):
        """ Periodic callback """
        logger.debug('Ticking client "{}".'.format(self.uid))
        if self._current_request is not None:
            # We're waiting for a reply from the server so don't initiate any more requests
            return

        request = None
        data = None

        if not self.connected:
            # Not connected so we should issue a request to connect to the server
            request = 'connect'
            data = None

        elif not self.working:
            # First check all services have been registered
            # Register in alphabetical order
            for service_name, func in sorted(self._services.items()):
                if service_name not in self._services_registered:
                    logger.debug('Service "{}" not registered; generating registration request.'.format(service_name))
                    # Service not registered; generate a request to fix this
                    request = 'register_service'
                    data = {
                        'service_name': service_name
                    }
                    break  # only create one request

            if request is None:
                # No service registration to be done; but we're also not working
                # So fetch some work if it exists
                request = 'request_task'
                data = None
        else:
            # We are working first check to see if work has completed.
            if self._working_thread.is_alive():
                logger.info('Service "{}" running task: {}'.format('TBC', self._task_uid))
                # Still working; send an update
                # TODO get some information here
                request = 'task_update'
                data = {'uid': self._task_uid}
            else:
                logger.info('Service "{}" completed task: {}'.format('TBC', self._task_uid))
                request = 'task_complete'
                data = {'uid': self._task_uid}


        # Finally issue the request
        if request is not None:
            self.send_request(request, data=data)

    def connect_reply(self, reply):
        msg_type = self.protocol.get_message_type(reply)

        if msg_type == 'success':
            self._connected = True
        elif msg_type == 'error':
            self._connected = False
        else:

            raise ValueError('Reply type "{}" not recognised for "{}" request.'.format(msg_type, self.current_request_type))

    def register_service_reply(self, reply):
        msg_type = self.protocol.get_message_type(reply)

        # Get the name of the service that was requested to be registered
        current_req_body = self.current_request_body
        service_name = current_req_body['service_name']

        if msg_type == 'success':
            logger.info('Successfully registered "{}" with the server.'.format(service_name))
            self._services_registered.add(service_name)
        elif msg_type == 'error':
            logger.error('Error registering service "{}" with the server.'.format(service_name))
        else:
            raise ValueError('Reply type "{}" not recognised for "{}" request.'.format(msg_type, self.current_request_type))

    def request_task_reply(self, reply):
        msg_type = self.protocol.get_message_type(reply)

        if msg_type == 'new_task':
            body = self.protocol.get_message_body(reply)
            service_name = body['service_name']
            task_uid = body.get('uid')
            logger.info('Received new task for service "{}" with uid: "{}"'.format(service_name, task_uid))

            # Get the service function and arguments
            attr = self._services[service_name]
            args = body.get('args', [])
            kwargs = body.get('kwargs', {})
            logger.debug('Calling service function with {:d} args and {:d} kwargs.'.format(len(args), len(kwargs)))

            self._working = True
            # Call service function
            thread = threading.Thread(target=attr, args=[self]+args, kwargs=kwargs)
            self._working_thread = thread
            self._task_uid = task_uid
            thread.start()

        elif msg_type == 'no_task':
            # No tasks to undertake
            self._working = False
        else:
            raise ValueError(
                'Reply type "{}" not recognised for "{}" request.'.format(msg_type, self.current_request_type))

    def task_update_reply(self, reply):
        """ Reply from server for task update """
        msg_type = self.protocol.get_message_type(reply)

        if msg_type == 'success':
            logger.info('Successfully updated the server about progress of task: {}'.format(self._task_uid))
        elif msg_type == 'error':
            logger.error('Error updating the server about progress of task: {}.'.format(self._task_uid))
        else:
            raise ValueError('Reply type "{}" not recognised for "{}" request.'.format(msg_type, self.current_request_type))

    def task_complete_reply(self, reply):
        """ Reply from server for task update """
        msg_type = self.protocol.get_message_type(reply)

        if msg_type == 'success':
            logger.info('Successfully updated the server about completion of task: {}'.format(self._task_uid))
            # Reset internal state
            self._working_thread = None
            self._working = False
            self._task_uid = None
        elif msg_type == 'error':
            logger.error('Error updating the server about completion of task: {}.'.format(self._task_uid))
        else:
            raise ValueError('Reply type "{}" not recognised for "{}" request.'.format(msg_type, self.current_request_type))