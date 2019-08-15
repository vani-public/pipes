import pytest
from pypipes.context.define import define
from mock import Mock

from pypipes.processor import Processor, pipe_processor, pipe_attachment, event_processor


class EmitProcessor(Processor):
    def _do_process(self, injections):
        injections['response'].emit_message({'key': 'value'})


class ReturnProcessor(Processor):
    def _do_process(self, injections):
        return {'key': 'value'}


class YieldProcessor(Processor):
    def _do_process(self, injections):
        yield {'key': 'value'}


@pipe_processor
def emit_processor(response, a, b):
    response.emit_message({'result': a + b})


@pipe_processor
def return_processor(a, b):
    return {'result': a + b}


@pipe_processor
def yield_processor(a, b):
    yield {'result': a + b}


@pipe_attachment
def processor_attachment(processor):
    processor.updated_by_attached = True
    return processor


@pytest.mark.parametrize('processor', [
    EmitProcessor(),
    ReturnProcessor(),
    YieldProcessor()], ids=['emit', 'return', 'yield'])
def test_processor_result(processor):
    response_mock = Mock()
    injections = {'response': response_mock}
    processor.process(injections)

    response_mock.emit_message.assert_called_once_with({'key': 'value'})


def test_processor_monitor_events():
    processor = Processor()
    assert processor.monitor_events == []

    processor.add_monitor('event1', 'event2')
    processor.add_monitor('event3')
    assert processor.monitor_events == ['event1', 'event2', 'event3']


def test_processor_clone():
    processor = Processor()
    processor.add_monitor('event1', 'event2')
    assert processor.monitor_events == ['event1', 'event2']

    processor2 = processor.clone()

    assert isinstance(processor2, Processor)
    assert processor2.monitor_events == ['event1', 'event2']
    assert processor != processor2


@pytest.mark.parametrize('processor', [
    emit_processor,
    return_processor,
    yield_processor], ids=['emit', 'return', 'yield'])
def test_pipe_processor(processor):
    response_mock = Mock()
    injections = {'response': response_mock,
                  'a': 2, 'b': 3}
    processor.process(injections)
    response_mock.emit_message.assert_called_once_with({'result': 5})
    response_mock.emit_message.reset_mock()

    # apply contextmanager
    define_contextmanager1 = define(a=3)
    processor = define_contextmanager1(processor)
    assert len(processor.context_managers) == 1
    processor.process(injections)
    response_mock.emit_message.assert_called_once_with({'result': 6})
    response_mock.emit_message.reset_mock()

    define_contextmanager2 = define(a=5, b=5)  # this should override another context values
    processor = define_contextmanager2(processor)
    assert len(processor.context_managers) == 2
    processor.process(injections)
    response_mock.emit_message.assert_called_once_with({'result': 10})


def test_pipe_processor_direct_call():
    assert return_processor(2, 3) == {'result': 5}
    for result in yield_processor(a=2, b=3):
        assert result == {'result': 5}


def test_pipe_attachment():
    pipeline = processor_attachment >> return_processor
    assert len(pipeline) == 1
    assert pipeline[0].updated_by_attached
    assert not hasattr(return_processor, 'updated_by_attached')


def test_event_processor():
    @event_processor('event1', 'event2')
    def processor():
        return {}

    assert processor.monitor_events == ['event1', 'event2']
