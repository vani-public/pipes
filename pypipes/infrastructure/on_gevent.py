import logging
import signal
from datetime import datetime
from greenlet import GreenletExit

import gevent
import gevent.pool
import gevent.queue
from gevent.exceptions import LoopExit
from pypipes.infrastructure.base import ListenerInfrastructure

logger = logging.getLogger(__name__)

WORKER_COUNT = 100


class QueueItem(object):
    def __init__(self, program, target, message):
        self.program = program
        self.target = target
        self.message = message


class GeventInf(ListenerInfrastructure):

    def __init__(self, context=None):
        super(GeventInf, self).__init__(context)
        self.message_queue = gevent.queue.Queue()
        self.pool = gevent.pool.Pool(WORKER_COUNT)
        self.schedulers = {}

    def try_start_program(self, program):
        if program.id in self.schedulers:
            return False
        self.schedulers[program.id] = {}
        return super(GeventInf, self).try_start_program(program)

    def try_stop_program(self, program):
        if program.id not in self.schedulers:
            return False
        super(GeventInf, self).try_stop_program(program)
        self._stop_all_schedulers(program.id)
        return True

    def _main_worker(self):
        terminated = False
        while not terminated:
            item = self.message_queue.get()
            if item is None:
                # None is a worker termination signal
                terminated = True
            else:
                self.pool.spawn(self.process_message,
                                self.get_program(item.program), item.target, item.message)
        # wait till worker pool complete all active jobs
        # Note that main worker will not process any new message henceforth
        # so all new messages will be lost if you don't persist them somehow
        self.pool.join()

    def handle_shutdown(self, signalnum, flag):
        # signal callback is executed in main thread and blocks main event loop of gevent
        # so gevent can't wait anything inside this function
        for program_id in self.schedulers.keys():
            gevent.spawn(self._stop_all_schedulers, program_id)
        # send a termination signal into main worker via message queue
        self.message_queue.put(None)

    def run_worker(self):
        orig_term = signal.getsignal(signal.SIGTERM)
        orig_int = signal.getsignal(signal.SIGINT)

        signal.signal(signal.SIGTERM, self.handle_shutdown)
        signal.signal(signal.SIGINT, self.handle_shutdown)
        try:
            main_worker = gevent.spawn(self._main_worker)
            main_worker.join()
        except LoopExit:
            raise RuntimeError('Worker is terminated, because of no more job is expected.')
        finally:
            # revert to original signals
            signal.signal(signal.SIGTERM, orig_term)
            signal.signal(signal.SIGINT, orig_int)

    def send_message(self, program, processor_id, message, start_in=None, priority=None):
        # current version ignores priority
        # but it could be implemented with gevent.queue.PriorityQueue
        self.message_queue.put(QueueItem(program.id, processor_id, message))
        gevent.sleep(0)

    def _scheduler_worker(self, program, scheduler_id, processor_id, message, start_time=None,
                          repeat_period=None):
        repeat_period = repeat_period and repeat_period.total_seconds()
        if start_time and start_time > datetime.now():
            sleep_time = (start_time - datetime.now()).total_seconds()
        else:
            # start immediately
            sleep_time = 0

        try:
            while sleep_time is not None:
                gevent.sleep(sleep_time)
                self.send_message(program, processor_id, message)
                sleep_time = repeat_period
        except GreenletExit:
            logger.debug('scheduler %s of %s program was stopped', processor_id, program.id)
        finally:
            # remove scheduler from list if exists
            if scheduler_id in self.schedulers.get(program.id, {}):
                del self.schedulers[program.id][scheduler_id]

    def add_scheduler(self, program, scheduler_id, processor_id, message,
                      start_time=None, repeat_period=None):
        # remove previous scheduler with such name if exists
        self.remove_scheduler(program, scheduler_id)

        # create a separate greenlet for each scheduler
        scheduler = gevent.spawn(self._scheduler_worker, program, scheduler_id,
                                 processor_id, message,
                                 start_time=start_time, repeat_period=repeat_period)
        self.schedulers[program.id][scheduler_id] = scheduler

    def remove_scheduler(self, program, scheduler_id):
        scheduler = self.schedulers.get(program.id, {}).get(scheduler_id)
        if scheduler:
            # remove scheduler from list
            del self.schedulers[program.id][scheduler_id]
            if not scheduler.ready():
                # if this scheduler still active, kill a greenlet
                scheduler.kill()

    def _stop_all_schedulers(self, program_id):
        schedulers = self.schedulers[program_id].keys()
        for scheduler_id in schedulers:
            self.remove_scheduler(self.get_program(program_id), scheduler_id)
        del self.schedulers[program_id]
