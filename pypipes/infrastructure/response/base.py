from functools import partial

from pypipes.infrastructure.response import IResponseHandler
from pypipes.message import Message, MessageUpdate


class BaseResponseHandler(IResponseHandler):
    __properties = None

    def __init__(self, original_message):
        self.__properties = {}
        self.__original_message = original_message
        self.__message_filters = []
        self._message = Message()
        self._retry_message_update = MessageUpdate()

    @property
    def message(self):
        return self._message

    @property
    def retry_message(self):
        return self._retry_message_update

    @retry_message.setter
    def retry_message(self, value):
        assert value is True
        self._retry_message_update = True

    def add_message_filter(self, filter_func):
        """
        Add a new message filter
        Filter callable example:

        def filter(message):
            # some message operations here
            return message
        :param filter_func: filter callable
        """
        self.__message_filters.append(filter_func)

    def remove_message_filter(self, filter_func):
        self.__message_filters.remove(filter_func)

    def _filter_message(self, message):
        """
        If processor added a message filter, any message emited by processor
        has to pass this filter check before sending
        :param message: message to filter
        :return: updated message or None if this message should be blocked
        """
        for message_filter in reversed(self.__message_filters):
            message = message_filter(message)
            if message is None:
                # message has been filtered out
                break
        return message

    def flush(self):
        """
        Emit self.message and restart message if any
        """
        if self.message:
            self.emit_message(self.message)
        if self.retry_message is True:
            # retry input message as is
            self.emit_retry_message(self.__original_message)
        elif self.retry_message:
            # retry input message with some updates
            self.emit_retry_message(self.retry_message.merge_with_message(self.__original_message))

        self._message = Message()
        self._retry_message_update = MessageUpdate()

    def extend_flush(self, flush_extension):
        # override response flush
        self.flush = partial(flush_extension, self.flush)

    def set_property(self, name, getter=None, setter=None):
        self.__properties[name] = getter, setter

    def __getattr__(self, item):
        if self.__properties and item in self.__properties:
            return self.__properties[item][0]()
        raise AttributeError(item)

    def __setattr__(self, item, value):
        if self.__properties and item in self.__properties:
            return self.__properties[item][1](value)
        super(BaseResponseHandler, self).__setattr__(item, value)
