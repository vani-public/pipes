from pypipes.context.manager import PipeContextManager

PAGE_KEY = '_page'


def pagination_contextmanager(message, response):
    """
    Manage processor restarting on next page exists
    :type message: pypipes.message.FrozenMessage
    :param response: pypipes.infrastructure.response.IResponseHandler
    """
    response.page = None

    yield {'page': message.pop(PAGE_KEY)} if PAGE_KEY in message else {}

    if response.page is not None:
        # restart original message with _page parameter
        response.retry_message[PAGE_KEY] = response.page


# just a trick to make a pyCharm type hinting happy
pagination = PipeContextManager(pagination_contextmanager)
