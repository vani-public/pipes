
class IResponseHandler(object):

    @property
    def message(self):
        """
        This message that will be emitted to next processor if not empty
        :return: message
        :rtype: pypipes.message.Message
        """
        raise NotImplementedError()

    @property
    def retry_message(self):
        """
        Processor job will be retried if retry_message was updated by processor.
        You may update retry message values like this:
            retry_message.key = updated_value
        Set retry_message = True if you want to retry input message as is.
        :rtype: pypipes.message.MessageUpdate
        """
        raise NotImplementedError()

    @retry_message.setter
    def retry_message(self, value):
        # Only True value is acceptable
        raise NotImplementedError()

    def add_message_filter(self, filter_func):
        """
        Add a new message filter
        Filter callable example:

        def filter(message):
            # some message operations here
            return message
        :param filter_func: filter callable
        :type filter_func: (dict) -> dict
        """
        raise NotImplementedError()

    def remove_message_filter(self, filter_func):
        """
        Remove message filter from filters
        :param filter_func: function
        :type filter_func: (dict) -> dict
        """
        raise NotImplementedError()

    def flush(self):
        """
        Flush response data.
        This is a last operation that program does before deleting the response handler object
        """
        raise NotImplementedError()

    def emit_message(self, _message=None, _start_in=None, _priority=None, **kwargs):
        """
        Emit a message for next processor in pipeline
        :type _message: dict
        :param _priority: message priority
        :param _start_in: message processing delay
        :param kwargs: additional message values. Override message in _message parameter
        """
        raise NotImplementedError()

    def emit_retry_message(self, _message=None, _retry_in=None, _priority=None, **kwargs):
        """
        Emit a message for this processor
        :param _retry_in: process message some later. Delay in seconds.
        :type _message: dict
        :param _priority: message priority
        :param kwargs: additional message values. Override message in _message parameter
        """
        raise NotImplementedError()

    def schedule_message(self, message, scheduler_id=None, target_id=None,
                         start_time=None, period=None):
        """
        Create a scheduler that will send a message to next processor periodically or with delay
        This scheduler could be stopped later by stop_scheduler method
        :param message: message dict
        :param scheduler_id: scheduler id. If None - start default scheduler for current processor
        :param target_id: message target id. If None - next processor in pipeline
        :param start_time: start time
        :param period: send message periodically this period
        """
        raise NotImplementedError()

    def stop_scheduler(self, scheduler_id=None):
        """
        Stop scheduler
        :param scheduler_id: scheduler id. If None - stop default scheduler for current processor
        """
        raise NotImplementedError()

    def send_event(self, event_name, processor=None, message=None, apply_filters=False):
        """
        Send event to all event listeners or directly to specified processor
        :param event_name: event name
        :param processor: processor id. Send an event to specified processor
        :param message: message dict
        :param apply_filters: filter event message like regular message
        """
        raise NotImplementedError()

    def send_message(self, processor_id, message, start_in=None, priority=None):
        """
        Send a message directly to specified processor
        :param processor_id: processor id
        :param message: message
        :param start_in: start delay
        :param priority: message priority
        """
        raise NotImplementedError()

    def extend_flush(self, flush_extension):
        """
        Extend a functionality of original response flushing or override it.
        :param flush_extension: a function that adds an additional flush functionality.
            Extension receives original flush method as a parameter.
        :type flush_extension: (() -> None) -> None
        """
        raise NotImplementedError()

    def set_property(self, name, getter=None, setter=None):
        """
        Context manager could use this method to setup a custom response property
        :param name: property name
        :param getter: property getter
        :type getter: () -> object
        :param setter: property setter
        :type setter: (object) -> None
        """
        raise NotImplementedError()
