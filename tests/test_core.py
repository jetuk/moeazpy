import zmq
from zmq.eventloop import ioloop
import unittest.mock as mock
import pytest
import zmq

from moeazpy.message_protocol import MoeaMessageProtocol
from moeazpy.core import ZmqServer


@pytest.fixture()
def simple_server(mocker):
    """ Simple server used for testing purposes """
    mocker.patch('zmq.eventloop.zmqstream.ZMQStream')
    mocker.patch('zmq.eventloop.ioloop.IOLoop')
    mocker.patch('zmq.Context')

    zs = ZmqServer(MoeaMessageProtocol())
    zs.setup('moeazpy.test')

    # mock the internal zmq objects
    return zs


class TestZmqServer:
    """ Test :class:`ZmqServer` base class"""

    def test_start(self, simple_server):

        simple_server.start()
        assert simple_server.loop.start.call_count == 1

    def test_stop(self, simple_server):

        simple_server.start()
        simple_server.stop()
        assert simple_server.loop.start.call_count == 1
        assert simple_server.loop.stop.call_count == 1

    def test_handle_message(self, simple_server):

        protocol = simple_server.protocol
        connect_msg = protocol.build_request('connect')

        # re-setup the server with the patched ZMQStream
        simple_server.setup('moeazpy.test')

        # Fake a message from a client
        simple_server._handle_message([b'client1', b'', connect_msg])
        expected_reply = protocol.build_reply('success')
        assert simple_server.frontend.send_multipart.call_args == (([b'client1', b'', expected_reply], ), {})
        assert simple_server._client_states[b'client1'].current_state == 'ready'

    def test_simple_client_service_registration(self, simple_server):

        protocol = simple_server.protocol
        connect_msg = protocol.build_request('connect')

        # Fake a message from a client
        simple_server._handle_message([b'client1', b'', connect_msg])
        expected_reply = protocol.build_reply('success')

        assert simple_server.frontend.send_multipart.call_count == 1
        assert simple_server.frontend.send_multipart.call_args == (([b'client1', b'', expected_reply], ), {})
        assert simple_server._client_states[b'client1'].current_state == 'ready'

        # Now register a service
        register_service_msg = protocol.build_request('register_service',
                                                      data={
                                                          'service_name': 'the-best-service',
                                                          'args': (), 'kwargs': {},
                                                      })
        simple_server._handle_message([b'client1', b'', register_service_msg])

        # This should be successful
        assert simple_server.frontend.send_multipart.call_count == 2
        assert simple_server.frontend.send_multipart.call_args == (([b'client1', b'', expected_reply],), {})

        # Now check the service has been registered on the client
        service_manager = simple_server._client_service_managers['the-best-service']
        assert b'client1' in service_manager.clients

    def test_simple_client_service_request(self, simple_server):

        s = simple_server
        protocol = s.protocol

        # First connect two clients
        connect_msg = protocol.build_request('connect')

        s._handle_message([b'client1', b'', connect_msg])
        s._handle_message([b'worker1', b'', connect_msg])

        # Now register a service
        register_service_msg = protocol.build_request('register_service',
                                                      data={
                                                          'service_name': 'the-best-service',
                                                          'args': (), 'kwargs': {},
                                                      })
        s._handle_message([b'worker1', b'', register_service_msg])

        # Now check the service has been registered on the client
        service_manager = simple_server._client_service_managers['the-best-service']
        assert b'worker1' in service_manager.clients

        # Now let's try to use the service from client1
        request_service_msg = protocol.build_request('request_service',
                                                     data={
                                                         'service_name': 'the-best-service',
                                                     })

        s._handle_message([b'client1', b'', request_service_msg])

        # There should now be a single request
        assert len(service_manager.requests) == 1

        for req_uid, req in service_manager.requests.items():
            assert req.client == b'client1'
            assert req.worker is None
            assert req_uid == req.uid
            # Reply from server should include the generate UID for the request
            expected_reply = protocol.build_reply('request_success', data={'uid': req_uid})
            assert simple_server.frontend.send_multipart.call_args == (([b'client1', b'', expected_reply],), {})





