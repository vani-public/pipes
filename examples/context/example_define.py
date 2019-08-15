# This example demonstrate how you may redefine existing context for some processor
import logging

from pypipes.config import Config
from pypipes.context.define import define, define_config
from pypipes.context.pagination import pagination
from pypipes.infrastructure.inline import RunInline
from pypipes.processor import pipe_processor
from pypipes.processor.event import Event
from pypipes.processor.message import Message
from pypipes.service.logger import logger_context

from pypipes.program import Program


# lets assume we have a processor with some predefined context
# we use pagination contextmanager as a context factory in this example


@pagination
@pipe_processor
def log_page(logger, page=0, updated=False):
    # pagination contextmanager creates a page context for the processor, page=0 by default
    # also this processor expects an `updated` context but no one generates the context for now.
    logger.info('Current page: %s, updated: %s', page, updated)
    return {}


# And here we create another processor that is based on previous one
# but redefine the `page` context generated by `pagination` and add a new context `updated`
# Also here we redefine an infrastructure context `logger` with new custom logger factory
new_logger_factory = logger_context('custom')
log_fixed_page = define(page=10, updated=True, logger=new_logger_factory)(log_page)


# In same way you may redefine default config but it's useful to use
# a special context manager `define_config` for this
@pipe_processor
def log_config(logger, config):
    logger.info('Config: key1=%s, key2=%s', config.key1, config.key2)
    return {}


log_replaced_config = define_config({'key1': 'UPDATED'})(log_config)
log_merged_config = define_config({'key2': {'key2_1': 'MERGED'}}, merge=True)(log_config)


paging_pipeline = Event.on_start >> Message.extend(_page=1) >> log_page >> log_fixed_page
config_pipeline = Event.on_start >> log_config >> log_replaced_config >> log_merged_config


program = Program(name='test',
                  pipelines={'paging': paging_pipeline,
                             'config': config_pipeline
                             })

default_config = {
    'key1': 'value1',
    'key2': {'key2_1': 'value2.1',
             'key2_2': 'value2.2'}
}

context = {'logger': logger_context('default'),
           'config': Config(default_config)}
infrastructure = RunInline(context)
infrastructure.load(program)

logging.basicConfig(level=logging.INFO, format='Logger: %(name)s > %(message)s')

if __name__ == '__main__':
    # The program will be started immediately as only start event is sent
    infrastructure.start(program)
