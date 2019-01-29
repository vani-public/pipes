from pipes.config import Config, merge as merge_configs
from pipes.context import apply_context_to_kwargs
from pipes.context.manager import pipe_contextmanager
from pipes.exceptions import RetryMessageException, DropMessageException


def define(**kwargs):
    @pipe_contextmanager
    def _define(injections):
        yield apply_context_to_kwargs(kwargs, injections)
    return _define


def define_config(new_config, merge=False):
    @pipe_contextmanager
    def _define_config(config=None):
        if merge and config:
            _config = merge_configs(dict(config), new_config)
        else:
            _config = new_config
        yield {'config': Config(_config)}
    return _define_config


@pipe_contextmanager
def prevent_message_retry(processor_id):
    try:
        yield {}
    except RetryMessageException:
        raise DropMessageException(
            'Message retry is disabled for processor: {}'.format(processor_id))
    except (AssertionError, DropMessageException):
        raise
    except Exception as e:
        raise DropMessageException(e)
