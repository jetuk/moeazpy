import os
import json
import logging
logger = logging.getLogger(__name__)


def load_moea_protocol(component):
    logger.info('Loading MOEA protocol component: "{}"'.format(component))
    fn = 'moea_{}.json'.format(component)
    fn = os.path.join(os.path.dirname(__file__), fn)
    logger.debug('Loading component from: "{}"'.format(fn))
    with open(fn) as fh:
        protocol = json.load(fh)
    return protocol


# Load MOEA protocol components
MOEA_SERVER_STATES = load_moea_protocol('server_state')
MOEA_MSG_PROTOCOL = load_moea_protocol('message_protocol')
