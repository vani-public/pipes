import pytest

from pypipes.context import context, message
from pypipes.context.cursor import cursor
from pypipes.processor import pipe_processor


def test_cursor_update(response_mock, infrastructure_mock, program_mock, plain_cursor_storage, lock_pool_mock):

    @cursor()
    @pipe_processor
    def update_cursor_processor(response, cursor='NOT DEFINED'):
        lock_pool_mock.cursor.acquire.assert_called_once_with('processor_id', 60)
        lock_pool_mock.cursor.acquire.reset_mock()

        # this value must be saved into cursor storage
        response.cursor = 'NEW CURSOR VALUE'
        return {'cursor_value': cursor}

    update_cursor_processor.process({'processor_id': 'processor_id',
                                     'response': response_mock,
                                     'lock': lock_pool_mock,
                                     'cursor_storage': plain_cursor_storage})

    lock_pool_mock.cursor.release.assert_called_once_with('processor_id')
    infrastructure_mock.send_message.assert_called_once_with(
        program_mock, 'next_processor_id', {'cursor_value': 'NOT DEFINED'},
        priority=None, start_in=None)
    infrastructure_mock.send_message.reset_mock()

    update_cursor_processor.process({'processor_id': 'processor_id',
                                     'response': response_mock,
                                     'lock': lock_pool_mock,
                                     'cursor_storage': plain_cursor_storage})

    infrastructure_mock.send_message.assert_called_once_with(
        program_mock, 'next_processor_id', {'cursor_value': 'NEW CURSOR VALUE'},
        priority=None, start_in=None)


def test_cursor_update_custom_name(response_mock, infrastructure_mock, program_mock,
                                   plain_cursor_storage, lock_pool_mock):

    @cursor('custom1')
    @cursor('custom2')
    @pipe_processor
    def update_cursor_processor(response, custom1_cursor='NOT DEFINED',
                                custom2_cursor='NOT DEFINED'):
        lock_pool_mock.cursor.acquire.assert_any_call('custom1', 60)
        lock_pool_mock.cursor.acquire.assert_any_call('custom2', 60)
        lock_pool_mock.cursor.acquire.reset_mock()

        # these values must be saved into cursor storage
        response.custom1_cursor = 'NEW CURSOR VALUE 1'
        response.custom2_cursor = 'NEW CURSOR VALUE 2'
        return {'cursor_value': [custom1_cursor, custom2_cursor]}

    update_cursor_processor.process({'processor_id': 'processor_id',
                                     'response': response_mock,
                                     'lock': lock_pool_mock,
                                     'cursor_storage': plain_cursor_storage})

    lock_pool_mock.cursor.release.assert_any_call('custom1')
    lock_pool_mock.cursor.release.assert_any_call('custom2')

    infrastructure_mock.send_message.assert_called_once_with(
        program_mock, 'next_processor_id', {'cursor_value': ['NOT DEFINED', 'NOT DEFINED']},
        priority=None, start_in=None)
    infrastructure_mock.send_message.reset_mock()

    update_cursor_processor.process({'processor_id': 'processor_id',
                                     'response': response_mock,
                                     'lock': lock_pool_mock,
                                     'cursor_storage': plain_cursor_storage})

    infrastructure_mock.send_message.assert_called_once_with(
        program_mock, 'next_processor_id',
        {'cursor_value': ['NEW CURSOR VALUE 1', 'NEW CURSOR VALUE 2']},
        priority=None, start_in=None)


@pytest.mark.parametrize('cursor_name, cursor_kwargs, expected', [
    (None, dict(), 'processor_id'),
    (None, dict(conn_id=context, part=message.part), 'processor_id.conn_id:1.part:10'),
    ('cursor_name', dict(conn_id=context, part=message.part), 'cursor_name.conn_id:1.part:10'),
])
def test_cursor_parametrized(response_mock, cursor_storage_mock, lock_pool_mock,
                             cursor_name, cursor_kwargs, expected):
    @cursor(cursor_name=cursor_name, **cursor_kwargs)
    @pipe_processor
    def update_cursor_processor(response, injections):
        lock_pool_mock.cursor.acquire.assert_called_once_with(expected, 60)
        lock_pool_mock.cursor.acquire.reset_mock()
        cursor_storage_mock.get.assert_called_once_with(expected)
        cursor_storage_mock.get.reset_mock()

    update_cursor_processor.process({'conn_id': 1,
                                     'message': {'part': 10},
                                     'processor_id': 'processor_id',
                                     'response': response_mock,
                                     'lock': lock_pool_mock,
                                     'cursor_storage': cursor_storage_mock})
