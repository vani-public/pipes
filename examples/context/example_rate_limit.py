from pypipes.context import message
from pypipes.context.rate_limit import rate_limit_guard
from pypipes.infrastructure.on_gevent import GeventInf
from pypipes.processor import pipe_processor
from pypipes.processor.scheduler import Scheduler
from pypipes.service.counter import memory_counter_pool
from pypipes.service.lock import memory_lock_pool
from pypipes.service.logger import logger_context, init_logging
from pypipes.service.rate_counter import memory_rate_pool

from pypipes.program import Program


@pipe_processor
def emit_messages():
    # this processor emits 2 times more messages with id=1 than id=2
    # we expect that message with id=1 will fall into rate limit block
    yield {'id': 1}
    yield {'id': 1}
    yield {'id': 2}


@rate_limit_guard(5, 5,  # only 5 messages in 5 seconds are allowed
                  suspend=False,  # drop messages over rate limit
                  msg_id=message.id)  # limit rate per message id
@pipe_processor
def processor_with_rate_limit(message, logger, counter):
    # counter should be almost similar for different messaged
    # because of rate limit guard limits messages with id=1 to only 5 per 5 seconds
    counter.msg.increment(message.id)
    logger.info('Processed message %s', message.id)
    logger.info('Message balance: %s, %s',
                counter.msg.increment(1, value=0), counter.msg.increment(2, value=0))


pipeline = Scheduler.start_period(seconds=1) >> emit_messages >> processor_with_rate_limit


init_logging()

program = Program(name='test1',
                  pipelines={'rate_example': pipeline})

if __name__ == '__main__':
    services = {'logger': logger_context(),
                'rate_counter': memory_rate_pool,
                'counter': memory_counter_pool,  # count messages
                'lock': memory_lock_pool}

    infrastructure = GeventInf(services)
    infrastructure.load(program)
    infrastructure.start(program)

    # run gevent worker
    infrastructure.run_worker()
