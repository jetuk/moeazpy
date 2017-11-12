import pytest
import time
from moeazpy.core import ZmqClient, service
from moeazpy.message_protocol import MoeaMessageProtocol

from helpers import assert_message_call_args


@pytest.fixture()
def client(mocker):
    mocker.patch('zmq.eventloop.zmqstream.ZMQStream')
    mocker.patch('zmq.eventloop.ioloop.IOLoop')
    mocker.patch('zmq.eventloop.ioloop.PeriodicCallback')
    mocker.patch('zmq.Context')

    client = ZmqClient(MoeaMessageProtocol(), 'server')
    client.setup()
    return client


class TestZmqClient:
    """ Test the `ZmqClient` class """
    def test_start(self, client):
        protocol = client.protocol

        assert not client.connected
        client.start()

        # We have to fake the tick because of mocking the loop
        client.tick()
        # Starting the client should initiate a connect message with the server
        expected_request = protocol.build_request('connect')
        assert_message_call_args(client.server_connection.send, expected_request)

    def test_stop(self, client):
        client.start()
        client.stop()


class ExampleClient(ZmqClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.arg1 = None
        self.arg2 = 0
        self.keyword1 = None
        self._finished = False

    @service('the-best-service')
    def the_best_service(self, arg1, arg2, keyword1='default'):
        self.arg1 = arg1
        self.arg2 += arg2
        self.keyword1 = keyword1

    @service('must-be-poked-to-finish')
    def must_be_poked_to_finish(self, ):
        self._finished = False
        while not self._finished:
            pass


@pytest.fixture()
def example_client(mocker):
    mocker.patch('zmq.eventloop.zmqstream.ZMQStream')
    mocker.patch('zmq.eventloop.ioloop.IOLoop')
    mocker.patch('zmq.eventloop.ioloop.PeriodicCallback')
    mocker.patch('zmq.Context')

    client = ExampleClient(MoeaMessageProtocol(), 'server')
    client.setup()
    return client


@pytest.fixture()
def example_client_connected(example_client):
    """ The ExampleClient but with mocked calls to connect and register all services """
    c = example_client
    protocol = c.protocol

    # Connect
    c.tick()
    c._handle_message([protocol.build_reply('success')])

    # Register both services
    for i in range(2):
        c.tick()
        c._handle_message([protocol.build_reply('success')])

    return c


class TestExampleClient:

    def test_initial_ticks(self, example_client):
        """ Test the first few ticks of the client """
        c = example_client
        protocol = c.protocol

        assert not c.connected
        c.tick()
        # First tick should create a connection request
        assert c.server_connection.send.call_count == 1
        assert_message_call_args(c.server_connection.send, protocol.build_request('connect'))

        # Recognise this as successful
        c._handle_message([protocol.build_reply('success')])
        assert c.connected  # Now connected

        c.tick()
        # Second tick should register the first service
        req_msg = protocol.build_request('register_service', data={
            'service_name': 'must-be-poked-to-finish',
        })
        assert c.server_connection.send.call_count == 2
        assert_message_call_args(c.server_connection.send, req_msg)
        c._handle_message([protocol.build_reply('success')])
        assert 'must-be-poked-to-finish' in c._services_registered

        c.tick()
        # Third tick should register the third service
        req_msg = protocol.build_request('register_service', data={
            'service_name': 'the-best-service',
        })
        assert c.server_connection.send.call_count == 3
        assert_message_call_args(c.server_connection.send, req_msg)
        c._handle_message([protocol.build_reply('success')])
        assert 'the-best-service' in c._services_registered

        c.tick()
        # The fourth tick should be looking for work
        req_msg = protocol.build_request('request_task')
        assert c.server_connection.send.call_count == 4
        assert_message_call_args(c.server_connection.send, req_msg)
        c._handle_message([protocol.build_reply('no_task')])


    def test_service_call(self, example_client):
        c = example_client
        protocol = c.protocol

        c.send_request('request_task')

        assert c.server_connection.send.call_count == 1

        reply_msg = protocol.build_reply('new_task', data={
            'service_name': 'the-best-service',
            'args': ['a', 1],
            'kwargs': {'keyword1': 'value'},
            'uid': '123456'
        })

        c._handle_message([reply_msg])

        # TODO This task should finish pretty instantly, but is there
        # a risk c.arg1 is not set before the assert is called?

        assert c.arg1 == 'a'
        assert c.arg2 == 1
        assert c.keyword1 == 'value'

        # Call it again
        c.send_request('request_task')
        assert c.server_connection.send.call_count == 2
        c._handle_message([reply_msg])

        assert c.arg1 == 'a'
        assert c.arg2 == 2
        assert c.keyword1 == 'value'

    def test_progress_call(self, example_client_connected):
        c = example_client_connected
        initial_send_call_count = c.server_connection.send.call_count
        protocol = c.protocol

        c.tick()
        assert c.server_connection.send.call_count == initial_send_call_count + 1
        assert_message_call_args(c.server_connection.send, protocol.build_request('request_task'))

        reply_msg = protocol.build_reply('new_task', data={
            'service_name': 'must-be-poked-to-finish',
            'uid': '123456'
        })

        c._handle_message([reply_msg])

        assert c.working
        assert c._working_thread is not None

        # Ticking should now update the server with progress
        c.tick()
        assert c.server_connection.send.call_count == initial_send_call_count + 2
        req_msg = protocol.build_request('task_update', data={
            'uid': '123456'
        })
        assert_message_call_args(c.server_connection.send, req_msg)
        c._handle_message([protocol.build_reply('success')])

        # Trigger the work to complete
        c._finished = True

        c._working_thread.join()

        # Ticking should now update the server with progress
        c.tick()
        assert c.server_connection.send.call_count == initial_send_call_count + 3
        req_msg = protocol.build_request('task_complete', data={
            'uid': '123456'
        })
        assert_message_call_args(c.server_connection.send, req_msg)
        c._handle_message([protocol.build_reply('success')])