from time import time

from pypipes.context.manager import pipe_contextmanager
from pypipes.exceptions import RetryMessageException, DropMessageException


@pipe_contextmanager
def collect_metrics(processor_id, response, metrics):
    """
    Save processor execution metrics
    :param response: processor response
    :type response: pypipes.infrastructure.response.IResponseHandler
    :param processor_id: processor id
    :param metrics: metrics aggregator client
    :type metrics: pypipes.service.metric.IMetrics
    """
    tags = {'op': processor_id}
    start_time = time()

    def _filter(msg):
        metrics.increment('pipe.processor.emit_message', tags=tags)
        return msg

    response.add_message_filter(_filter)

    try:
        yield {}
        execution_time = int((time() - start_time) * 1000)
        metrics.timing('pipe.processor.execution_time', execution_time, tags=tags)
    except RetryMessageException:
        metrics.increment('pipe.processor.retry_message', tags=tags)
        raise
    except DropMessageException:
        metrics.increment('pipe.processor.drop_message', tags=tags)
        raise
    except Exception as e:
        error_name = e.__class__.__name__
        metrics.increment('pipe.processor.error', tags=dict(tags, error=error_name))
        raise
