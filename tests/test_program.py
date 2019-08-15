import pytest
from pypipes.context.define import define
from pypipes.processor import pipe_processor, event_processor
from mock import Mock

from pypipes.program import Program


@event_processor('event1')
def processor1():
    pass


@event_processor('event2')
def processor2():
    pass


@pipe_processor
def processor3(service, a, b):
    service.call(a + b)


@pytest.fixture
def pipeline1():
    return processor1 >> processor2 >> processor3


@pytest.fixture
def pipeline2():
    return processor2 >> processor3


@pytest.fixture
def program(pipeline1, pipeline2):
    program = Program(
        name='test',
        version='1.0',
        pipelines={
            'pipeline1': pipeline1,
            'pipeline2': pipeline2
        }
    )
    return program


def test_program_mapping(program, pipeline1, pipeline2):
    assert program.id == 'test=1.0'
    assert program.processors == {
        'pipeline1.processor1': pipeline1[0],
        'pipeline1.processor2': pipeline1[1],
        'pipeline1.processor3': pipeline1[2],
        'pipeline2.processor2': pipeline2[0],
        'pipeline2.processor3': pipeline2[1]
    }

    assert program.events == {
        'event1': ['pipeline1.processor1'],
        'event2': ['pipeline1.processor2', 'pipeline2.processor2']
    }

    # append one more pipeline into the program
    program.add_pipeline('new', pipeline2)
    assert program.processors == {
        'pipeline1.processor1': pipeline1[0],
        'pipeline1.processor2': pipeline1[1],
        'pipeline1.processor3': pipeline1[2],
        'pipeline2.processor2': pipeline2[0],
        'pipeline2.processor3': pipeline2[1],
        'new.processor2': pipeline2[0],
        'new.processor3': pipeline2[1]
    }

    assert program.events == {
        'event1': ['pipeline1.processor1'],
        'event2': ['pipeline1.processor2', 'pipeline2.processor2', 'new.processor2']
    }


def test_program_get_processor(program, pipeline1):
    assert program.get_processor('pipeline1.processor1') == pipeline1[0]
    assert program.get_processor('unknown') is None


def test_program_get_next_processor(program):
    assert program.get_next_processor('pipeline1.processor1') == 'pipeline1.processor2'
    # check last in pipeline
    assert program.get_next_processor('pipeline1.processor3') is None


def test_program_get_listeners(program):
    assert program.get_listeners('event1') == ['pipeline1.processor1']
    assert program.get_listeners('event2') == ['pipeline1.processor2', 'pipeline2.processor2']
    assert program.get_listeners('unknown') == []


def test_program_run(program):
    service_mock = Mock()
    response_mock = Mock()
    injections = {
        'response': response_mock,
        'service': service_mock,
        'a': 2, 'b': 3}
    program.run_processor('pipeline2.processor3', injections)
    service_mock.call.assert_called_once_with(5)
    service_mock.call.reset_mock()

    program.add_contextmanager(define(a=3))
    program.run_processor('pipeline2.processor3', injections)
    service_mock.call.assert_called_once_with(6)
    service_mock.call.reset_mock()

    program.add_contextmanager(define(a=5, b=5))
    program.run_processor('pipeline2.processor3', injections)
    service_mock.call.assert_called_once_with(10)


def test_program_run_unknown(program):
    # assert no exception
    program.run_processor('unknown_processor', {})
