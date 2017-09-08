from zmq.eventloop import ioloop, zmqstream
import zmq
import json
import uuid
import time
import logging

logger = logging.getLogger(__name__)


class ZmqServer:
    """ Base class for all zmq classes """
    def __init__(self):
        self.uid = uuid.uuid4().hex
        # Server variables; initialised in `setup`
        self.context = None
        self.loop = None

    def setup(self):
        self.context = zmq.Context()
        self.loop = ioloop.IOLoop()

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
        logger.debug('Handling message ...')
        decoded_msg = msg[-1].decode('utf-8')
        decoded_msg = json.loads(decoded_msg)

        msg_type = decoded_msg['type']

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

    def _send(self, message_type, originator, content):
        self.stream.send_json(make_message(message_type, originator, content))


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