from pypipes.context import apply_context_to_kwargs
from pypipes.context.lock import singleton_guard
from pypipes.context.manager import pipe_contextmanager
from pypipes.service import key
from pypipes.service.cursor_storage import cursor_storage_context

CURSOR_LOCK_EXPIRATION = 60


def cursor(cursor_name=None, guard_expire_in=CURSOR_LOCK_EXPIRATION,
           retry_if_locked=False, **context_kwargs):
    """
    A context manager for pipe processor to manage cursor value saved in IStorage
    This context manager ensure that only one processor works with a cursor in same time frame.
    Requires next services:
        storage.cursor (IStorage) - cursor storage
        lock.cursor (ILock) - cursor guard lock
    :param cursor_name: cursor name. Processor id is used as a name if cursor_name is missed.
        Note that processor id depends on program pipeline.
        Use fixed cursor name if cursor storage is persistent and you plan to change the pipeline
    :param guard_expire_in: singleton_guard lock expiration time.
        Processor lock will be automatically prolonged each time when new message is emitted.
    :param retry_if_locked: retry task if cursor is locked, otherwise just drop task
    :param context_kwargs: cursor parameters.
        Use ContextPath to extract values from processor context
    :return: pipe_contextmanager
    :rtype: PipeContextManager
    """
    @singleton_guard(cursor_name, expire_in=guard_expire_in, lock_category='cursor',
                     retry_if_locked=retry_if_locked, **context_kwargs)
    @pipe_contextmanager
    def cursor_contextmanager(processor_id, injections, response,
                              cursor_storage=cursor_storage_context):
        cursor_key = key(cursor_name or processor_id,
                         **apply_context_to_kwargs(context_kwargs, injections))
        context_name = '{}_cursor'.format(cursor_name) if cursor_name else 'cursor'
        cursor_value = cursor_storage.get(cursor_key)
        if cursor_value is not None:
            context = {context_name: cursor_value}
        else:
            # return empty context if no cursor
            context = {}

        def cursor_getter():
            # get actual cursor value from storage
            record = cursor_storage.get(cursor_key)
            return record and record.value

        def cursor_setter(value):
            # update a cursor value in storage
            cursor_storage.save(cursor_key, value)

        # add cursor property into response
        response.set_property(context_name, cursor_getter, cursor_setter)

        yield context

    return cursor_contextmanager
