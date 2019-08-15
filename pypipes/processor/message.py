# Message processors are needed generally for program debugging purpose
# They may update input message, extend it with new values or replace it with another message

from pypipes.context import apply_context_to_kwargs

from pypipes.processor import pipe_processor


class Message(object):
    @staticmethod
    def send(message=None, **kwargs):
        @pipe_processor
        def send_message(injections):
            return apply_context_to_kwargs(dict(message or {}, **kwargs), injections)
        return send_message

    @staticmethod
    def extend(extension=None, **kwargs):
        """
        Extend an input message with additional values
        """
        @pipe_processor
        def extend_message(message, injections):
            update_values = apply_context_to_kwargs(dict(extension or {}, **kwargs), injections)
            return dict(message, **update_values)
        return extend_message

    @staticmethod
    def log(comment='Message'):
        @pipe_processor
        def log(logger, message):
            """
            Transparent processor that just log input message
            """
            logger.info('{}: %s'.format(comment), dict(message))
            return message
        return log
