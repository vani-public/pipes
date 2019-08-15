from pypipes.infrastructure.response.base import BaseResponseHandler


class ListenerResponseHandler(BaseResponseHandler):

    def __init__(self, infrastructure, program, processor_id, original_message):
        super(ListenerResponseHandler, self).__init__(original_message)
        self.__infrastructure = infrastructure
        self.__program = program
        self.__processor_id = processor_id
        self.__next_processor_id = program.get_next_processor(processor_id)

    def emit_message(self, _message=None, _start_in=None, _priority=None, **kwargs):
        message_dict = dict(_message, **kwargs) if _message else dict(kwargs)
        self.send_message(self.__next_processor_id,
                          start_in=_start_in, priority=_priority,
                          message=message_dict)

    def emit_retry_message(self, _message=None, _retry_in=None, _priority=None, **kwargs):
        message_dict = dict(_message, **kwargs) if _message else dict(kwargs)
        self.send_message(self.__processor_id,
                          start_in=_retry_in, priority=_priority,
                          message=message_dict)

    def schedule_message(self, message, scheduler_id=None, target_id=None,
                         start_time=None, period=None):
        assert start_time or period
        scheduler_id = scheduler_id or self.__processor_id
        target_id = target_id or self.__next_processor_id
        if target_id:
            # ignore the scheduler if it's a last processor in a pipeline.
            self.__infrastructure.add_scheduler(self.__program, scheduler_id, target_id,
                                                dict(message),
                                                start_time=start_time, repeat_period=period)

    def stop_scheduler(self, scheduler_id=None):
        scheduler_id = scheduler_id or self.__processor_id
        self.__infrastructure.remove_scheduler(self.__program, scheduler_id)

    def send_event(self, event_name, processor=None, message=None, apply_filters=False):
        message = dict(message) if message else {}
        if apply_filters:
            message = self._filter_message(message)
            if message is None:
                # some filter stopped the message processing
                return
        self.__infrastructure.send_event(self.__program, event_name, processor=processor,
                                         message=message)

    def send_message(self, processor_id, message, start_in=None, priority=None):
        message = self._filter_message(dict(message))
        if message is not None and processor_id:
            # ignore messages if it's a last processor in a pipeline
            self.__infrastructure.send_message(self.__program, processor_id, message,
                                               start_in=start_in,
                                               priority=priority)
