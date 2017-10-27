from zmq.eventloop import ioloop
import unittest.mock as mock
import pytest
import zmq

from moeazpy.core import ZmqServer


class TestZmqServer:
    """ Test :class:`ZmqServer` base class"""
    def test_setup(self):

        zs = ZmqServer()

        assert zs.context is None
        assert zs.loop is None

        zs.setup()

        assert isinstance(zs.context, zmq.Context)
        assert isinstance(zs.loop, ioloop.IOLoop)

    def test_start(self):

        zs = ZmqServer()
        zs.setup()
        # mock the internal zmq objects
        zs.context = mock.Mock(spec_set=zmq.Context)
        zs.loop = mock.Mock(spec_set=ioloop.IOLoop)
        zs.start()

        assert zs.loop.start.call_count == 1

    def test_stop(self):

        zs = ZmqServer()
        zs.setup()
        # mock the internal zmq objects
        zs.context = mock.Mock(spec_set=zmq.Context)
        zs.loop = mock.Mock(spec_set=ioloop.IOLoop)
        zs.stop()

        assert zs.loop.stop.call_count == 1


