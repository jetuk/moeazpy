from moeazpy.state import StateMachine, MoeaServerStateMachine
from moeazpy.message_protocol import MoeaMessageProtocol, MessageProtocol
import pytest
from test_protocol import simple_message_protocol


@pytest.fixture()
def simple_state_definition():

    return {
        "start": {
            "hello": {
                "action": "check_credentials",
                "next": "authenticating"
            }
        },
        "authenticating": {
            "auth-ok": {
                "action": "reply",
                "message": "success",
                "next": "ready"
            },
            "auth-error": {
                "action": "reply",
                "message": "error",
                "next": "start"
            }
        },
        "ready": {}
    }


class SimpleStateMachine(StateMachine):
    def check_credentials(self, ):
        if True:
            self.event_queue.append('auth-ok')
        self.event_queue.append('auth-error')


def test_simple_state_machine(simple_state_definition, simple_message_protocol):

    sm = SimpleStateMachine(simple_state_definition, 'start', simple_message_protocol)

    assert sm.current_state == 'start'
    # Trigger the authentication
    sm.event_queue.append('hello')
    sm._process_event_queue()

    # Model goes straight through to authentication
    assert sm.current_state == 'ready'


@pytest.fixture()
def moea_sm():
    # TODO mock the server here instead of using `None`
    return MoeaServerStateMachine(None, MoeaMessageProtocol())


class TestMoeaServerStateMachine:
    def test_init(self, moea_sm):
        assert moea_sm.current_state == 'start'

    def test_action_compliance(self, moea_sm):
        moea_sm.check_action_compliance()



