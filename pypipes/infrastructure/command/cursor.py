import six

from pypipes.infrastructure.command import ContextCommand, SubCommand


class ClearCursorsCommand(ContextCommand):
    help = 'clear all cursors for selected version'

    def add_arguments(self, parser, infrastructure):
        parser.add_argument('--cursor-version', nargs='?', help='cursor version')

    def run(self, infrastructure, args):
        super(ClearCursorsCommand, self).run(infrastructure, args)
        if args.cursor_version:
            self.context['cursor_version'] = args.cursor_version
        func = input if six.PY3 else raw_input  # noqa:F821 undefined name 'raw_input'
        confirmed = args.quiet or (func(
            'Do you want to delete ALL cursors '
            'from cursor collection? [y/N]').lower() == 'y')
        if confirmed:
            cursor_storage = self.context['cursor_storage']
            cursor_storage.clear()
            args.print_message('Cursor collection cleared')


class GetCursorCommand(ContextCommand):
    help = 'get cursor value'

    def add_arguments(self, parser, infrastructure):
        parser.add_argument('cursor_name', help='scheduler name')

    def run(self, infrastructure, args):
        super(GetCursorCommand, self).run(infrastructure, args)
        cursor_storage = self.context['cursor_storage']
        cursor_value = cursor_storage.get(args.cursor_name)
        args.print_message('\n{}: {}'.format(args.cursor_name, cursor_value),
                           quiet_message=str(cursor_value))


class ListCursorsCommand(ContextCommand):
    help = 'list cursors'

    def run(self, infrastructure, args):
        super(ListCursorsCommand, self).run(infrastructure, args)
        # get all ids from cursor storage
        cursor_storage = self.context['cursor_storage']
        cursors = sorted(cursor_storage.list())
        args.print_message('\nCursor name\n')
        for cursor_name in cursors:
            args.print_message(quiet_message=cursor_name)


cursor_subcommand = SubCommand(
    title='Cursor commands',
    dest='cursor_command',
    help='manage registered connections',
    commands={
        'list': ListCursorsCommand(),
        'get': GetCursorCommand(),
        'clear': ClearCursorsCommand(),
    })
