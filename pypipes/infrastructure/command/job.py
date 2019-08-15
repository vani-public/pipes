from pypipes.infrastructure.command import SubCommand, ContextCommand


class JobCommand(SubCommand):
    def __init__(self, *jobs):
        """
        :type jobs: [Job]
        """
        super(JobCommand, self).__init__(
            'Job names', dest='job_name',
            help='manage program jobs',
            commands={job.job_name: JobSubCommands(job)
                      for job in jobs})


class JobSubCommands(SubCommand):
    def __init__(self, job, commands=None):
        job_commands = {
            'start': JobStartCommand(job),
            'stop': JobStopCommand(job),
            'list': JobListCommand(job)
        }
        if commands:
            job_commands.update(commands)
        super(JobSubCommands, self).__init__('Job action', 'job_command',
                                             help='manage {} jobs'.format(job.job_name),
                                             commands=job_commands)


class JobActionCommand(ContextCommand):
    help_template = '{} action'

    def __init__(self, job):
        """
        :type job: Job
        """
        self.job = job
        self.help = self.help_template.format(job.job_name)


class JobStartCommand(JobActionCommand):
    help_template = 'Start new {} job'

    @staticmethod
    def argument_dest(parameter):
        return 'job_param_{}'.format(parameter)

    def add_arguments(self, parser, infrastructure):
        for parameter in self.job.kwargs:
            parser.add_argument('--{}'.format(parameter), dest=self.argument_dest(parameter),
                                required=True, help='job parameter {!r}'.format(parameter))

    def run(self, infrastructure, args):
        job_params = {}
        for parameter in self.job.kwargs:
            job_params[parameter] = getattr(args, self.argument_dest(parameter))
        infrastructure.send_event(args.program, self.job.event_name, message=job_params)
        args.print_message('Triggered {} job start with parameters: {}'.format(self.job.job_name,
                                                                               job_params))


class JobStopCommand(JobActionCommand):
    help_template = 'Stop active {} job'

    def add_arguments(self, parser, infrastructure):
        group = parser.add_mutually_exclusive_group(required=False)
        group.add_argument('job_ids', nargs='*', default=[],
                           help='list of job ids to stop')
        group.add_argument('--all', action='store_true', default=False,
                           help='stop all active {} jobs'.format(self.job.job_name))

    def run(self, infrastructure, args):
        super(JobStopCommand, self).run(infrastructure, args)
        active_jobs = dict(self.job.list_active_jobs(self.context))
        jobs_to_stop = (active_jobs if args.all else
                        [job_id for job_id in args.job_ids if job_id in active_jobs])
        stopped = 0
        for job_id in jobs_to_stop:
            if self.job.stop(job_id, self.context):
                stopped += 1
        args.print_message('Stopped {} jobs'.format(stopped))


class JobListCommand(JobActionCommand):
    help_template = 'List all active {} jobs'

    def run(self, infrastructure, args):
        super(JobListCommand, self).run(infrastructure, args)
        args.print_message(
            '\n Active {} jobs (job id, job parameters):\n'.format(self.job.job_name))
        for job_id, job_params in self.job.list_active_jobs(self.context):
            args.print_message(quiet_message='{} {}'.format(job_id, job_params))
