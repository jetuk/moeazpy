import json
import logging
logger = logging.getLogger(__name__)


class MessageProtocol:
    def __init__(self, protocol):
        self.protocol = protocol

    @property
    def name(self):
        return self.protocol['name']

    @property
    def version(self):
        return self.protocol['version']

    @property
    def headers(self):
        for v in self.protocol['header']:
            yield v

    @property
    def requests(self):
        return self.protocol['requests']

    @property
    def replies(self):
        return self.protocol['replies']

    def validate_message(self, msg):
        """ Check the frames of a message correspond to this protocol. """
        for i, (header, frame) in enumerate(zip(self.headers, msg)):
            if header['name'] == 'empty':
                if frame != '':
                    raise ValueError('Frame {:d} is not empty!')
            elif header['name'] == 'protocol_name':
                # Check msg is from this protocol
                if frame != self.name:
                    raise ValueError('Message for a different protocol: "{}" instead of "{}".'.format(frame, self.name))
            elif header['name'] == 'protocol_version':
                # Check msg is from this protocol version
                if frame != self.version:
                    raise ValueError('Message for a different protocol version: "{}" instead of "{}".'.format(frame, self.version))
            elif header['name'] == 'type':
                # Check that the given message type is valid for this protocol
                if frame not in self.requests and frame not in self.replies:
                    raise ValueError('Message is not a valid type for this protocol: "{}"'.format(frame))
            else:
                raise ValueError('Header name "{}" in position {:d} not recognised'.format(header['name'], i))

    def get_message_type(self, msg, validate=True):
        if validate:
            self.validate_message(msg)

        for header, frame in zip(self.headers, msg):
            if header['name'] == 'type':
                return frame

    def get_message_body(self, msg, validate=True):
        if validate:
            self.validate_message(msg)

        return json.loads(msg[-1])

    def _build_header(self, message_type):
        """ Create a generic message header on this protocol """
        msg = []

        for i, header in enumerate(self.headers):
            if header['name'] == 'empty':
                msg.append('')
            elif header['name'] == 'protocol_name':
                msg.append(self.name)
            elif header['name'] == 'protocol_version':
                msg.append(self.version)
            elif header['name'] == 'type':
                msg.append(message_type)
            else:
                raise ValueError('Header name "{}" in position {:d} not recognised'.format(header['name'], i))

        return msg

    def build_reply(self, message_type, data=None, encode=True):
        """ Create a reply message based on this protocol """
        return self._build_message(message_type, self.replies, data=data, encode=encode)

    def build_request(self, message_type, data=None, encode=True):
        """ Create a request message based on this protocol """
        return self._build_message(message_type, self.requests, data=data, encode=encode)

    def encode_message(self, message):
        return json.dumps(message).encode('utf-8')

    def _build_message(self, message_type, definitions, data=None, encode=True):
        """ Helper method to build a generic message from a given dict of definitions """

        # Fetch the message definition for this message type
        definition = definitions[message_type]

        msg = self._build_header(message_type)
        try:
            fields = definition['fields']
        except KeyError:
            fields = {}  # No fields defined

        if data is None:
            data = {}  # No data given

        # Perform some checks that the message data matches the protocol's field definition
        for k, attrs in fields.items():
            try:
                optional = attrs['optional']
            except KeyError:
                optional = False

            if k not in data and not optional:
                raise ValueError('Field "{}" for message type "{}" not defined in message data.'.format(k, message_type))

        for k in data.keys():
            if k not in fields:
                # TODO this could be a warning?
                raise ValueError('Data provided for field "{}", but this field is not required for message type "{}".'.format(k, message_type))

        # Now build the message using JSON serialisation and encode
        msg.append(json.dumps(data))
        if encode:
            return self.encode_message(msg)
        else:
            return msg










class MoeaMessageProtocol(MessageProtocol):
    def __init__(self):
        from .protocol import MOEA_MSG_PROTOCOL
        super().__init__(MOEA_MSG_PROTOCOL)


