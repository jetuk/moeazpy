from moeazpy.message_protocol import MoeaMessageProtocol
from moeazpy.core import ZmqServer, ZmqClient


def test_client_server():

    protocol = MoeaMessageProtocol()
    address = 'moeazpy.test'

    s = ZmqServer(protocol, address)
    c = ZmqClient(protocol, address)

    s.start()
    import time
    time.sleep(1)
    c.start()

    print('moo')

    #s.stop()


def setup_logging():
    import logging

    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger = logging.getLogger('moeazpy')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)


if __name__ == '__main__':

    setup_logging()
    test_client_server()
    print('Done')
