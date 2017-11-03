import pytest
from moeazpy.message_protocol import MessageProtocol


@pytest.fixture()
def simple_message_protocol():
    defn = {
        "name": "simple",
        "version": "1.0",
        "header": [
            {"name": "empty", "value": ""},
            {"name": "protocol_name"},
            {"name": "protocol_version"},
            {"name": "type"}
        ],
        "requests": {
            "connect": {},
            "disconnect": {},
            "ping": {},
        },
        "replies": {
            "error": {},
            "success": {}
        }
    }
    return MessageProtocol(defn)


class TestMessageProtocol:

    def test_init(self, simple_message_protocol):

        assert isinstance(simple_message_protocol, MessageProtocol)