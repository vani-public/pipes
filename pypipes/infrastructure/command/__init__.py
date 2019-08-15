from __future__ import print_function

import argparse
import json
import sys
from collections import defaultdict
from contextlib import contextmanager

import six
from pypipes.infrastructure.base import ISchedulerCommands


def print_message(message, error=False):
    message = message or ''
    if error:
        print(message, file=sys.stderr)
    else:
        print(message)


def load_json_message(message=None, load_input=False):
    if load_input:
        # try to read the message body from an input stream
        message = sys.stdin.read()
    try:
        return dict(json.loads(message)) if message else {}
    except ValueError as e:
        raise ValueError('Message is not a valid json dump: {}'.format(e))
    except TypeError:
        raise ValueError('Message must be a dictionary')


def list_processors(program):
    processor_events = defaultdict(list)
    for event in program.events:
        for processor in program.events[event]:
            processor_events[processor].append(event)

    for processor in program.processors:
        event_names = (' [{}]'.format(', '.join(processor_events[processor]))
                       if processor in processor_events else '')
        print_message('{} {}'.format(processor, event_names))


def add_message_argument(parser):
    """
    Append message argument group into the parser
    """
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument('-m', '--message', nargs='?', help='message body in json format')
    group.add_argument('-', dest='load_input', action='store_true', default=False,
                       help='load message body from input stream')
    return group


def create_command_parser(infrastructure, commands=None):
    programs = infrastructure.programs.keys()

    parser = argparse.ArgumentParser(description='Pipeline program management.')
    parser.add_argument('-p', '--program', nargs='?',
                        choices=programs,
                        help='run command on specified program if many programs are loaded.')
    parser.add_argument('-q', '--quiet', action='store_true', default=False,
                        help='minimize program output.')
    parsers = parser.add_subparsers(title='Commands', dest='command')
    parsers.add_parser('start', help='start the program')
    parsers.add_parser('stop', help='stop the program')
    parsers.add_parser('list', help='list processors and event listeners of the program')
    event_parser = parsers.add_parser('event', help='send an event')
    add_message_argument(event_parser)
    event_parser.add_argument('event_name', help='event name')
    send_parser = parsers.add_parser('send',
                                     help='send message directly to specified processor')
    add_message_argument(send_parser)
    send_parser.add_argument('processor_id', help='processor name')
    if isinstance(infrastructure, ISchedulerCommands):
        scheduler_parser = parsers.add_parser('scheduler', help='manage active schedulers')
        scheduler_commands = scheduler_parser.add_subparsers(title='Scheduler commands',
                                                             dest='scheduler_command')
        scheduler_commands.add_parser('list', help='list active schedulers')
        trigger_parser = scheduler_commands.add_parser(
            'trigger', help='trigger specified scheduler asap')
        trigger_parser.add_argument('scheduler_id', help='scheduler name')
        terminate_parser = scheduler_commands.add_parser(
            'terminate', help='terminate specified scheduler')
        terminate_parser.add_argument('scheduler_id', help='scheduler name')

    if commands:
        add_subparsers(infrastructure, parsers, commands)
    return parser


def add_subparsers(infrastructure, subparsers, commands):
    for command_name, command in six.iteritems(commands):
        command_parser = command.get_parser(infrastructure)
        if command_parser:
            help = command_parser.description
            subparsers.add_parser(command_name, help=help, parents=[command_parser])


def run_command(infrastructure, parser=None, args=None, commands=None):
    """
    Run pipeline command
    :param infrastructure: pipeline infrastructure
    :type infrastructure: pypipes.infrastructure.base.Infrastructure | ISchedulerCommands
    :param args: optional list of command args
    :param parser: optional command parser
    :param commands: enhance parser with custom command parsers. {command_name: command}
    :type commands: dict[str, ICommand]
    """
    parser = parser or create_command_parser(infrastructure, commands=commands)
    args = parser.parse_args(args)

    def local_print_message(message=None, quiet_message=None, error=False):
        if not args.quiet:
            print_message(message or quiet_message, error)
        elif quiet_message:
            print_message(quiet_message, error)

    args.print_message = local_print_message
    if args.program:
        program = infrastructure.programs.get(args.program)
    else:
        program = infrastructure.programs and list(infrastructure.programs.values())[0]

    if program:
        # replace program name with program instance
        args.program = program
    else:
        raise ValueError('ERROR: Pipeline program not found or no program is loaded')

    local_print_message('Program: {}'.format(program.id))
    if args.command == 'start':
        infrastructure.start(program)
        local_print_message('\nProgram started')
    elif args.command == 'stop':
        infrastructure.stop(program)
        local_print_message('\nProgram stopped')
    elif args.command == 'list':
        local_print_message('\nprocessor id  [event names]\n')
        list_processors(program)
    elif args.command == 'event':
        infrastructure.send_event(program, args.event_name,
                                  message=load_json_message(args.message, args.load_input))
        local_print_message('\nEvent sent')
    elif args.command == 'send':
        if args.processor_id not in program.processors:
            raise ValueError('Invalid processor id. '
                             'Use "list" command to list available processors.')
        infrastructure.send_message(program, args.processor_id,
                                    message=load_json_message(args.message, args.load_input))
        local_print_message('\nMessage sent')
    elif args.command == 'scheduler':
        # ensure scheduler id is valid
        if (hasattr(args, 'scheduler_id') and
                args.scheduler_id not in infrastructure.list_schedulers(program)):
            raise ValueError('Invalid scheduler id. '
                             'Use "scheduler list" command to list active schedulers.')

        if args.scheduler_command == 'list':
            local_print_message('\nscheduler id\n')
            for scheduler in infrastructure.list_schedulers(program):
                print_message(scheduler)
        elif args.scheduler_command == 'trigger':
            infrastructure.trigger_scheduler(program, args.scheduler_id)
            local_print_message('\nScheduler activated')
        elif args.scheduler_command == 'terminate':
            infrastructure.remove_scheduler(program, args.scheduler_id)
            local_print_message('\nScheduler removed')
    elif args.command in commands:
        commands[args.command].run(infrastructure, args)
    return args


@contextmanager
def handle_command_error(print_traceback=False):
    try:
        yield
        exit(0)
    except Exception as e:  # noqa
        if print_traceback:
            import traceback
            print_message(traceback.format_exc())
        else:
            print_message('ERROR: {}'.format(e), error=True)
        exit(1)


class ICommand(object):
    def get_parser(self, infrastructure):
        raise NotImplementedError()

    def run(self, infrastructure, args):
        raise NotImplementedError()


class Command(ICommand):
    help = None

    def get_parser(self, infrastructure):
        parser = argparse.ArgumentParser(description=self.help, add_help=False)
        self.add_arguments(parser, infrastructure)
        return parser

    def add_arguments(self, parser, infrastructure):
        pass


class ContextCommand(Command):
    context = None

    def run(self, infrastructure, args):
        self.context = infrastructure.get_program_context(args.program)


class SubCommand(Command):
    def __init__(self, title, dest, help, commands):
        self.help = help
        self.title = title
        self.dest = dest
        self.commands = commands

    def get_parser(self, infrastructure):
        parser = super(SubCommand, self).get_parser(infrastructure)
        subparsers = parser.add_subparsers(title=self.title, dest=self.dest)
        add_subparsers(infrastructure, subparsers, self.commands)
        return parser

    def run(self, infrastructure, args):
        sub_command_name = getattr(args, self.dest)
        if sub_command_name in self.commands:
            self.commands[sub_command_name].run(infrastructure, args)
