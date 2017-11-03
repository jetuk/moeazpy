"""
This module contains base classes for the implementation of state machines.

"""
import logging
logger = logging.getLogger(__name__)


class StateMachine:
    """ A simple object that transitions between predefined states based on events

    """
    def __init__(self, states_definitions, initial_state, message_protocol):
        self.states_definition = states_definitions
        self.current_state = initial_state
        self.message_protocol = message_protocol
        self.event_queue = []
        self.request = None
        self.reply_message = None

    def _dispatch_event(self, event):

        # Available events for the current state
        try:
            state_events = self.states_definition[self.current_state]
        except KeyError:
            logger.exception('Current state "{}" not found in states definition.'.format(self.current_state))
            return

        try:
            # Fetch the response for this event
            response = state_events[event]
        except KeyError:
            logger.error('Event "{}" is not a valid event for the current state "{}".'.format(event, self.current_state))
            return

        action = response['action']
        # Special case for "send"
        if action == "reply":
            msg = response['message']
            self.reply(msg)
        else:
            # Get the method to call to perform the action
            attr = getattr(self, action)
            # Call action
            attr()

        # Update the current state if necessary
        try:
            next_state = response['next']
        except KeyError:
            pass
        else:
            self.current_state = next_state

    def _process_event_queue(self):
        """ Process all waiting events """
        while self.event_queue:
            evt = self.event_queue.pop(0)
            self._dispatch_event(evt)

    def process_request(self, request):
        """ Process a request using this state machine.

        This should result in a new reply message to return to the sender.
        """
        logger.debug('Processing request ...')
        self.request = request
        self.reply_message = None
        # Process the request type as a new event
        evt = self.message_protocol.get_message_type(request)
        self.event_queue.append(evt)
        # Now process this event and any subsequent event chains
        self._process_event_queue()

    @property
    def current_request_body(self):
        return self.message_protocol.get_message_body(self.request)

    def reply(self, message_type, data=None):
        logger.info('Replying with message type: "{}".'.format(message_type))
        self.reply_message = self.message_protocol.build_reply(message_type, data=data)

    def check_action_compliance(self):
        """ Checks that methods exist for each of the actions defined in the state definitions """

        for state, events in self.states_definition.items():
            for name, data in events.items():
                logger.debug('Checking action "{}" in event "{}" from state "{}" has method defined.'.format(data['action'], name, state))
                assert hasattr(self, data['action'])


class ServerStateMachine(StateMachine):
    def __init__(self, server, client, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.server = server
        self.client = client


class MoeaServerStateMachine(ServerStateMachine):
    def __init__(self, server, client, protocol):
        from .protocol import MOEA_SERVER_STATES
        super().__init__(server, client, MOEA_SERVER_STATES, 'start', protocol)

    def register_client(self):
        # TODO some authentication
        self.event_queue.append('success')

    def unregister_client(self):
        # Disconnecting;, remove any remaining events
        while self.event_queue:
            self.event_queue.pop(0)

        # TODO this doesn't delete itself?

    def register_service(self):
        """ Register a new service that is provided by this client """
        msg = self.current_request_body
        self.server.register_client_service(self.client, msg['service_name'])
        self.reply('success')

    def request_service(self):
        """ Request a new service that is provided by the server """
        msg = self.current_request_body

        args = msg.get('args', [])
        kwargs = msg.get('kwargs', {})
        # Create the new service request
        req = self.server.request_client_service(self.client, msg['service_name'], args=args, kwargs=kwargs)

        self.reply('request_success', data={'uid': req.uid})

    def request_service_status(self):
        """ Request the status of a previously issue service request """
        msg = self.current_request_body
        uid = msg['uid']
        status = self.server.request_client_service_status(self.client, msg['service_name'], uid)

        self.reply('request_status', data={'uid': uid, 'status': status})

    def request_task(self):
        """ Request a new task for this client """
        task = self.server.dispatch_client_task(self.client)

        if task is None:
            # There is no new work!
            self.reply('no_task')
        else:
            self.reply('new_task', data={
                'service_name': task.manager.name,
                'uid': task.uid,
                'args': task.args,
                'kwargs': task.kwargs,
            })

    def task_complete(self):
        """ Successfully complete a task """
        msg = self.current_request_body

        uid = msg['uid']
        self.server.complete_task(uid)
        self.reply('success')


