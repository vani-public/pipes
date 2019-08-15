import pytest
from mock import Mock
from pypipes.context.manager import pipe_contextmanager
from pypipes.processor import pipe_processor


def test_contextmanager():

    @pipe_contextmanager
    def context_factory1():
        yield {'context1': 'value1'}

    @pipe_contextmanager
    def context_factory2():
        yield {'context2': 'value2'}

    @pipe_contextmanager
    def context_factory3():
        yield {'context3': 'value3'}

    @context_factory1
    @context_factory2
    @context_factory3
    @pipe_processor
    def processor(context1, context2, context3):
        assert context1 == 'value1'
        assert context2 == 'value2'
        assert context3 == 'value3'
        return {}

    response_mock = Mock()
    processor.process({'response': response_mock})

    response_mock.emit_message.assert_called_once_with({})


def test_contextmanager_replace_context():
    def context_factory_with_value(value):
        @pipe_contextmanager
        def context_factory():
            yield {'context': value}
        return context_factory

    @context_factory_with_value('outer')
    @context_factory_with_value('inner')
    @pipe_processor
    def processor(context):
        assert context == 'outer'  # outer context has higher priority
        return {}

    response_mock = Mock()
    processor.process({'response': response_mock})
    response_mock.emit_message.assert_called_once_with({})


def test_contextmanager_handle_error():
    @pipe_contextmanager
    def error_handler():
        with pytest.raises(ValueError):
            yield {'context': 'value'}

    @pipe_processor
    def processor(context):
        assert context == 'value'
        raise ValueError('TEST')

    # run without error handler
    with pytest.raises(ValueError):
        processor.process({'context': 'value'})

    # run processor with error handler context manager
    processor = error_handler(processor)
    processor.process({})


def test_contextmanager_reraise_error():

    @pipe_contextmanager
    def error_handler():
        try:
            yield {'context': 'value'}
            assert False
        except Exception:
            raise

    @error_handler
    @pipe_processor
    def processor(context):
        assert context == 'value'
        raise ValueError('TEST')

    with pytest.raises(ValueError):
        processor.process({})


def test_contexmanager_with_extra_context():
    @pipe_contextmanager
    def context_factory1(input_context):
        yield {'context': input_context + ['value1']}

    @context_factory1  # uses context from context_factory1
    @pipe_contextmanager
    def context_factory2(context):
        yield {'context': context + ['value2']}

    @context_factory2  # uses context from context_factory2
    @pipe_contextmanager
    def context_factory3(context):
        yield {'context': context + ['value3']}

    @context_factory3
    @pipe_processor
    def processor(context):
        # assert context is accumulated from all contextmanagers
        assert context == ['input_value', 'value1', 'value2', 'value3']
        return {}

    response_mock = Mock()
    processor.process({'response': response_mock,
                       'input_context': ['input_value']})
    response_mock.emit_message.assert_called_once_with({})
