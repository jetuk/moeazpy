import pytest
from moeazpy.broker import *

class TestBroker:

    def test_init(self):
        """ Test :class:`Broker` initialisation """

        b = Broker()

        assert len(b.services) == 0
        assert len(b._registered_workers) == 0

    def test_register_worker(self):
        """ Test registering workers with the broker """
        b = Broker()

        worker_id = 'a-worker'
        worker_services = [
            'the-best-service-ever',
            'this-other-cool-service',
        ]

        b.register_worker(worker_id, worker_services)

        assert worker_id in b._registered_workers
        for name, s in b.services.items():
            assert name in worker_services
            assert worker_id in s._registered_workers

    def test_register_worker_no_services(self):

        b = Broker()

        with pytest.raises(WorkerRegistrationError):
            b.register_worker('a-worker', [])


    def test_unregister_worker(self):
        """ Test unregistering workers with the broker """
        b = Broker()

        worker_id = 'a-worker'
        b.register_worker(worker_id, ['a.service'])
        b.register_worker('another-worker', ['a.service'])
        b.unregister_worker(worker_id)

        assert worker_id not in b._registered_workers
        assert 'another-worker' in b._registered_workers

    def test_request_service(self):
        """ Test requesting a service from a client """

        b = Broker()

        with pytest.raises(NoRegisteredWorkers):
            # Service doesn't exist on the broker
            b.request_service('my-client', 'a.service', {'args': [], 'kwargs': {}})

        s = b.ensure_service('a.service')
        assert len(s.pending_service_requests) == 0
        assert len(s.waiting_service_requests) == 0
        assert len(list(b.pending_service_requests)) == 0
        assert len(list(b.waiting_service_requests)) == 0

        b.register_worker('my-worker', ['a.service', ])

        # This should now work.
        b.request_service('my-client', 'a.service', {'args': [], 'kwargs': {}})
        s = b.ensure_service('a.service')

        assert len(s.pending_service_requests) == 0
        assert len(s.waiting_service_requests) == 1
        assert len(list(b.pending_service_requests)) == 0
        assert len(list(b.waiting_service_requests)) == 1
        assert s.waiting_service_requests[0]['client'] == 'my-client'

    def test_find_task(self):
        """ Test fetching a task for a worker  """

        b = Broker()

        with pytest.raises(WorkerNotRegistered):
            b.find_worker_task('a-worker')

        b.register_worker('a-worker', ['a.service'])

        with pytest.raises(NoPendingServiceRequests):
            b.find_worker_task('a-worker')

        b.request_service('my-client', 'a.service', {'some': 'content'})

        assert len(list(b.pending_service_requests)) == 0
        assert len(list(b.waiting_service_requests)) == 1

        req = b.find_worker_task('a-worker')

        assert len(list(b.pending_service_requests)) == 1
        assert len(list(b.waiting_service_requests)) == 0

        assert req == {'client': 'my-client', 'contents': {'some': 'content'}}

        # No pending requests again
        with pytest.raises(NoPendingServiceRequests):
            b.find_worker_task('a-worker')










