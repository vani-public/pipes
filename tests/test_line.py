from pypipes.context.define import define
from pypipes.processor import pipe_processor
from pypipes.processor.event import Event


@pipe_processor
def processor1():
    pass


@pipe_processor
def processor2():
    pass


@pipe_processor
def processor3():
    pass


def test_build_line_from_processors():
    pipeline = processor1 >> processor2 >> processor3
    assert len(pipeline) == 3
    assert list(p.processor_func for p in pipeline) == [processor1.processor_func,
                                                        processor2.processor_func,
                                                        processor3.processor_func]


def test_build_line_join_lines():
    pipeline1 = processor1 >> processor2
    pipeline2 = processor2 >> processor3
    pipeline = pipeline1 >> pipeline2

    assert len(pipeline) == 4
    assert list(p.processor_func for p in pipeline) == [processor1.processor_func,
                                                        processor2.processor_func,
                                                        processor2.processor_func,
                                                        processor3.processor_func]


def test_apply_contextmanager():
    pipeline1 = processor1 >> processor2
    pipeline2 = define(key='value')(pipeline1) >> processor3

    # pipeline1 is used to build pipeline2 but `define` context must be applied
    # to processors in pipeline2 only
    assert all(len(p.context_managers) == 0 for p in pipeline1)
    assert all(len(p.context_managers) for p in pipeline2[:2])


def test_apply_contextmanager_later():
    pipeline1 = processor1 >> processor2
    pipeline2 = pipeline1 >> processor3

    # apply context to pipeline1 but pipeline2 must be not changed
    pipeline1 = define(key='value')(pipeline1)

    # pipeline1 is used to build pipeline2 but `define` context must be applied
    # to processors in pipeline2 only
    assert all(len(p.context_managers) for p in pipeline1)
    assert all(len(p.context_managers) == 0 for p in pipeline2)


def test_join_attachment():
    # Event.on is a processor attachment
    pipeline = Event.on('START') >> processor1 >> processor2 >> processor3
    assert len(pipeline) == 3
    assert pipeline[0].monitor_events == ['START']

    # processor1 must be not changed
    assert processor1.monitor_events == []
