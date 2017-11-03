from zmq.eventloop import ioloop, zmqstream
import zmq
import json
import uuid
import time
import logging

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

    def add_service_request(self, client, args=None, kwargs=None):
        """ Add a new service request to the manager from client """
        req = ClientServiceRequest(client, args=args, kwargs=kwargs)
        self.requests[req.uid] = req
        return req


class ClientServiceRequest:
    def __init__(self, client, args=None, kwargs=None, creation_time=None, worker=None):
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


class ZmqServer:
    """ Base class for all zmq classes """
    def __init__(self, protocol, state_machine_factory=None):
        self.uid = uuid.uuid4().hex
        if state_machine_factory is None:
            # TODO make this using the generic parent class
            from .state import MoeaServerStateMachine
            state_machine_factory = MoeaServerStateMachine
        self.state_machine_factory = state_machine_factory
        self.protocol = protocol

        # Server variables; initialised in `setup`
        self.context = None
        self.loop = None

        # Client states
        self._client_states = {}
        # Client service managers
        self._client_service_managers = {}

    def setup(self, address):
        """ Setup the zmq server

        This must be done before calling `start()`


        """
        self.context = zmq.Context()
        self.loop = ioloop.IOLoop()

        frontend = self.context.socket(zmq.ROUTER)
        frontend.bind("ipc://{}.frontend".format(address))
        self.frontend = zmqstream.ZMQStream(frontend, self.loop)
        self.frontend.on_recv(self._handle_message)

    def start(self):
        logger.info('Starting {} server with uid: {}'.format(self.__module__ + "." + self.__class__.__name__, self.uid))
        # Start the event loop
        self.loop.make_current()
        try:
            self.loop.start()
        except KeyboardInterrupt:
            pass

    def stop(self):
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
        sm.process_request(request)

        reply = sm.reply_message
        if reply is None:
            reply = self.protocol.build_reply('error')

        # Now reply to the client
        print(self.frontend, [client, empty, reply])
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





def make_message(message_type, originator, content):
    """ Make a standard message dictionary
    """
    msg = {
        'type': message_type,
        'originator': originator,
        'content': content
    }
    return msg


class MessageHandler:
    def __call__(self, msg):
        decoded_msg = msg[-1].decode('utf-8')
        decoded_msg = json.loads(decoded_msg)

        msg_type = decoded_msg['type']
        logger.debug('Handling message type: "{}"'.format(msg_type))

        if msg_type.startswith('_'):
            raise ValueError('Invalid message type. Message type starts with a "_".')

        msg_type = msg_type.replace(".", "_")
        getattr(self, msg_type)(*msg[:-1] + [decoded_msg,])


class EchoMessageHandler(MessageHandler):
    def __call__(self, msg):
        logger.debug('Handling message ...')
        decoded_msg = msg[-1].decode('utf-8')
        decoded_msg = json.loads(decoded_msg)

        #logger.debug(decoded_msg)


class ReplyMessageHandler(MessageHandler):
    def __init__(self, stream):
        self.stream = stream

    def _reply(self, reply):

        # TODO fix originator
        msg = reply.make_message()
        msg = json.dumps(msg).encode('utf-8')

        self.stream.send(msg)


class Message:
    __message_type__ = None

    def __init__(self):
        # Each message is given a unique ID
        self.uid = uuid.uuid4().hex

    @property
    def message_type(self):
        return self.__message_type__

    def make_message_content(self):
        return None  # default to no content

    def make_message(self):

        if self.message_type is None:
            raise NotImplementedError('Class attribute "__message_type__" must be defined for all subclasses.')

        msg = {
            'type': self.message_type,
            'uid': self.uid,
            'content': self.make_message_content()
        }
        return msg


class Request(Message):
    """ A generic Request base class """
    def __init__(self, timeout, start_time=None):
        super().__init__()
        self.timeout = timeout

        if start_time is None:
            start_time = time.time()
        self.start_time = start_time

    @property
    def is_timed_out(self):
        return (time.time() - self.start_time) > self.timeout


class ReplyMessage(Message):
    def __init__(self, obj, request_message, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.obj = obj
        self.request_message = request_message

    @property
    def message_type(self):
        return '{}.{}'.format(self.request_message['type'], 'reply')


class OKReply(ReplyMessage):
    def make_message_content(self):
        return {
            'status': 'OK'
        }


class ErrorReply(ReplyMessage):
    def make_message_content(self):
        return {
            'status': 'error'
        }
