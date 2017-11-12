import pytest
from moeazpy.message_protocol import MoeaMessageProtocol
from moeazpy.core import ZmqServer, ZmqClient

from helpers import assert_message_call_args, assert_multipart_message_call_args

@pytest.fixture()
def server(mocker):
    """ Simple server used for testing purposes """
    mocker.patch('zmq.eventloop.zmqstream.ZMQStream')
    mocker.patch('zmq.eventloop.ioloop.IOLoop')
    mocker.patch('zmq.Context')

    zs = ZmqServer(MoeaMessageProtocol(), 'moeazpy.test')

    # mock the internal zmq objects
    return zs


class TestZmqServer:
    """ Test :class:`ZmqServer` base class"""

    def test_start(self, server):

        server.run()  # Don't run in subprocess
        assert server.loop.start.call_count == 1

    def test_stop(self, server):

        server.run()
        server.stop()
        assert server.loop.start.call_count == 1
        assert server.loop.stop.call_count == 1

    def test_handle_message(self, server):

        server.run()  # Don't run in subprocess
        protocol = server.protocol
        connect_msg = protocol.build_request('connect')

        # Fake a message from a client
        server._handle_message([b'client1', b'', connect_msg])
        expected_reply = protocol.build_reply('success')
        assert server.frontend.send_multipart.call_args == (([b'client1', b'', expected_reply], ), {})
        assert server._client_states[b'client1'].current_state == 'ready'

    def test_simple_client_service_registration(self, server):

        server.run()  # Don't run in subprocess
        protocol = server.protocol
        connect_msg = protocol.build_request('connect')

        # Fake a message from a client
        server._handle_message([b'client1', b'', connect_msg])
        expected_reply = protocol.build_reply('success')

        assert server.frontend.send_multipart.call_count == 1
        assert server.frontend.send_multipart.call_args == (([b'client1', b'', expected_reply], ), {})
        assert server._client_states[b'client1'].current_state == 'ready'

        # Now register a service
        register_service_msg = protocol.build_request('register_service',
                                                      data={
                                                          'service_name': 'the-best-service',
                                                          'args': (), 'kwargs': {},
                                                      })
        server._handle_message([b'client1', b'', register_service_msg])

        # This should be successful
        assert server.frontend.send_multipart.call_count == 2
        assert server.frontend.send_multipart.call_args == (([b'client1', b'', expected_reply],), {})

        # Now check the service has been registered on the client
        service_manager = server._client_service_managers['the-best-service']
        assert b'client1' in service_manager.clients

    def test_simple_client_service_request(self, server):

        s = server
        s.run()  # Don't run in subprocess
        protocol = s.protocol

        # First connect two clients
        connect_msg = protocol.build_request('connect')

        s._handle_message([b'client1', b'', connect_msg])
        s._handle_message([b'worker1', b'', connect_msg])

        # Now register a service
        register_service_msg = protocol.build_request('register_service', data={
            'service_name': 'the-best-service', 'args': (), 'kwargs': {},
        })
        s._handle_message([b'worker1', b'', register_service_msg])

        # Now check the service has been registered on the client
        service_manager = server._client_service_managers['the-best-service']
        assert b'worker1' in service_manager.clients

        # Now lets try to use the service from client1
        request_service_msg = protocol.build_request('request_service', data={
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
            assert_multipart_message_call_args(s.frontend.send_multipart, b'client1', expected_reply)

        # Now lets try to get status update about our new request
        request_service_status_msg = protocol.build_request('request_service_status', data={
            'service_name': 'the-best-service', 'uid': req.uid
        })

        s._handle_message([b'client1', b'', request_service_status_msg])

        # This request should be queueing because no workers have asked for any work yet.
        expected_reply = protocol.build_reply('request_status', data={'uid': req_uid, 'status': 'queueing'})
        assert_multipart_message_call_args(s.frontend.send_multipart, b'client1', expected_reply)

        # Worker now asks for any work
        request_work = protocol.build_request('request_task')
        s._handle_message([b'worker1', b'', request_work])

        # We expect the server to return a job to complete the previously add request
        expected_reply = protocol.build_reply('new_task', data={
            'service_name': 'the-best-service', 'uid': req_uid, 'args': [], 'kwargs': {},
        })

        assert_multipart_message_call_args(s.frontend.send_multipart, b'worker1', expected_reply)
        # The request should now be assigned to the worker ...
        assert req.worker == b'worker1'
        # .. and not in the queue
        assert len(list(service_manager.queued_requests)) == 0

        # Client now requests a status update again ...
        s._handle_message([b'client1', b'', request_service_status_msg])
        # ... the request should now be running
        expected_reply = protocol.build_reply('request_status', data={'uid': req_uid, 'status': 'running'})
        assert_multipart_message_call_args(s.frontend.send_multipart, b'client1', expected_reply)

        # Worker now completes the task
        request_task_complete = protocol.build_request('task_complete', data={
            'uid': req_uid
        })
        s._handle_message([b'worker1', b'', request_task_complete])

        expected_reply = protocol.build_reply('success')
        assert_multipart_message_call_args(s.frontend.send_multipart, b'worker1', expected_reply)

        # Client now requests a status update again ...
        s._handle_message([b'client1', b'', request_service_status_msg])
        # ... the request should now be complete!
        expected_reply = protocol.build_reply('request_status', data={'uid': req_uid, 'status': 'complete'})
        assert_multipart_message_call_args(s.frontend.send_multipart, b'client1', expected_reply)
