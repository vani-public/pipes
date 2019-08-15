from pypipes.infrastructure.base import ListenerInfrastructure


class RunInline(ListenerInfrastructure):

    def send_message(self, program, processor_id, message, start_in=None, priority=None):
        # instantly process the message missing any message queue
        self.process_message(program, processor_id, message)

    def add_scheduler(self, program, scheduler_id, processor_id, message,
                      start_time=None, repeat_period=None):
        # instantly process the message only once
        self.process_message(program, processor_id, message)

    def remove_scheduler(self, program, scheduler_id):
        pass
