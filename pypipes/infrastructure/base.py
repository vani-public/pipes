import logging

from pypipes.context import ContextPath, apply_context_to_kwargs, LazyContextCollection
from pypipes.events import EVENT_START, EVENT_STOP
from pypipes.exceptions import RetryMessageException, DropMessageException, ExtendedException
from pypipes.infrastructure.response.listener import ListenerResponseHandler
from pypipes.message import FrozenMessage

logger = logging.getLogger(__name__)


class Infrastructure(object):

    def __init__(self, context=None):
        self._context = context or {}
        self._programs = {}

    @property
    def context(self):
        return LazyContextCollection(self._context)

    @property
    def programs(self):
        return self._programs

    def get_program(self, program_id):
        """
        Get loaded program by id
        :param program_id: program id
        :return: Program object or None
        """
        return self._programs.get(program_id)

    def load(self, program):
        """
        Prepare an infrastructure to process a program
        Infrastructure should control that each unique program is loaded only once.
        program.id could be used as an unique id.
        :param program: Program object
        """
        if program.id not in self._programs:
            self._programs[program.id] = program
        else:
            raise ValueError('program {} is already loaded'.format(program.id))

    def is_loaded(self, program):
        """
        Check if program is loaded
        :param program: Program object
        :return: True if programs is loaded
        """
        return program.id in self._programs

    def start(self, program):
        """
        Send start event to pipeline program.
        :param program: Program object.
        """
        if not self.is_loaded(program):
            raise ValueError('program {} is not loaded'.format(program.id))
        elif not self.try_start_program(program):
            raise ValueError('program {} is already started'.format(program.id))

    def stop(self, program):
        """
        Send stop event to pipeline program.
        :param program: Program object.
        """
        if not self.is_loaded(program):
            raise ValueError('program {} is not loaded'.format(program.id))
        elif not self.try_stop_program(program):
            raise ValueError('program {} is not started'.format(program.id))

    def try_start_program(self, program):
        self.send_event(program, event_name=EVENT_START)
        return True

    def try_stop_program(self, program):
        self.send_event(program, event_name=EVENT_STOP)
        return True

    def process_message(self, program, processor_id, message_dict):
        """
        Process one message. Is typically used as a base message listener.
        :param program: Program object
        :type program: pypipes.program.Program
        :param processor_id: processor id
        :param message_dict: message dictionary
        """
        message_dict = message_dict or {}
        logger.debug('Process %s message %s', processor_id, message_dict)

        context = self.get_message_context(program, processor_id, message_dict)
        response_handler = self.get_response_handler(program, processor_id, message_dict)
        context['response'] = response_handler
        try:
            try:
                program.run_processor(processor_id, context)
            except RetryMessageException as e:
                # retry message
                response_handler.emit_retry_message(message_dict, _retry_in=e.retry_in)
            except DropMessageException as e:
                logger.warning('Processor %s dropped a message: %s', processor_id, e)
            response_handler.flush()
        except ExtendedException as e:
            logger.exception('Processor %s failed message processing: %s',
                             processor_id, message_dict, extra=e.extra)
            raise
        except Exception:
            logger.exception('Processor %s failed message processing: %s',
                             processor_id, message_dict)
            raise

    def get_program_context(self, program):
        context = LazyContextCollection(self.context)
        context['infrastructure'] = self
        context['program'] = program
        context['program_id'] = program.id
        return context

    def get_processor_context(self, program, processor_id):
        context = self.get_program_context(program)
        context['processor_id'] = processor_id
        context['next_processor_id'] = program.get_next_processor(processor_id)
        return context

    def get_message_context(self, program, processor_id, message_dict):
        context = self.get_processor_context(program, processor_id)
        context['message'] = FrozenMessage(message_dict)
        if program.message_mapping:
            # Extract mapped message parts from message dictionary
            # and update context
            context.update(apply_context_to_kwargs(program.message_mapping,
                                                   {'message': message_dict},
                                                   ContextPath('message')))
        return context

    def get_response_handler(self, program, processor_id, message_dict):
        """
        Build a response object that will be available as a `response` injection in processor.
        This object is usually a proxy between processor and infrastructure.
        :param program: Program object
        :param processor_id: processor id
        :param message_dict: message dictionary
        :return: processor response handler
        :rtype: BaseResponseHandler
        """
        raise NotImplementedError

    def send_event(self, program, event_name, processor=None, message=None):
        """
        Sends a message to all processors that are monitoring for this event
        or to specified processor
        :param program: Program object
        :param event_name: event name
        :param processor: processor id.
        :param message: message dict
        """
        raise NotImplementedError()

    def send_message(self, program, processor_id, message, start_in=None, priority=None):
        """
        Send direct message to one processor
        :param program: Program object
        :param processor_id: processor id
        :param message: message
        """
        raise NotImplementedError()

    def add_scheduler(self, program, scheduler_id, processor_id, message,
                      start_time=None, repeat_period=None):
        """
        Request infrastructure to send a `message` to `processor_id` at `start_time`
        and repeat the message periodically if needed.
        Updates existing scheduler if scheduler_id is the same.
        """
        raise NotImplementedError()

    def remove_scheduler(self, program, scheduler_id):
        raise NotImplementedError()


class ISchedulerCommands(object):
    def list_schedulers(self, program):
        """
        List program schedulers
        :param program:
        """
        raise NotImplementedError()

    def trigger_scheduler(self, program, scheduler_id):
        """
        Request to send the scheduled `message` asap.
        :param program: Program object
        :param scheduler_id: scheduler id
        :return True if scheduler was successfully activated, otherwise False
        """
        raise NotImplementedError()


class ListenerInfrastructure(Infrastructure):
    """
    Abstract infrastructure that implements event sending and message retry
    like processing of a regular message
    """
    def get_response_handler(self, program, processor_id, message_dict):
        return ListenerResponseHandler(self, program, processor_id, message_dict)

    def get_message_context(self, program, processor_id, message_dict):
        """
        Extract message parts from message dictionary
        """
        event = None
        if '_event' in message_dict:
            message_dict = dict(message_dict)
            event = message_dict.pop('_event')
        context = super(ListenerInfrastructure, self).get_message_context(
            program, processor_id, message_dict)
        context.update(event=event)
        return context

    def send_event(self, program, event_name, processor=None, message=None):
        """
        Find all processors that are monitoring for this event
        and send an event to them like a regular message
        """
        message = dict(message or {})
        message['_event'] = event_name
        for processor_id in program.get_listeners(event_name, processor):
            self.send_message(program, processor_id, dict(message))
