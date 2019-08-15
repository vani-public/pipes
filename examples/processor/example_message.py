# This example demonstrates several Message processor use-cases
# that might be used to debug a pipeline program

import logging

from pypipes.context import context
from pypipes.infrastructure.inline import RunInline
from pypipes.processor.event import Event
from pypipes.processor.message import Message

from pypipes.program import Program

logging.basicConfig(level=logging.INFO)


message_pipeline = (
    Event.on_start >> Message.log('Empty on start') >>
    Message.send({'key0': 12345,
                  'key1': context.context1}) >> Message.log('Initialized with context') >>
    Message.extend(key1='updated value',
                   key2=context.context2) >> Message.log('Updated and extended') >>
    Message.send(key3='replaced') >> Message.log('Replaced with new message') >>
    Message.send() >> Message.log('Cleared'))

program = Program(name='test',
                  pipelines={'message': message_pipeline})

services = {'logger': logging.getLogger(__name__),
            'context1': 'context_value1',
            'context2': 'context_value2'}
infrastructure = RunInline(services)
infrastructure.load(program)

if __name__ == '__main__':
    infrastructure.start(program)
