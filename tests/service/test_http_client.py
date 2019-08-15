import pytest
from mock import Mock
from requests import Response, Request

from pypipes.config import Config
from pypipes.service.http_client import create_http_client, HttpClient, HttpResponse, \
    ExtendedHTTPError


@pytest.fixture
def send_mock():
    response = Response()
    response.status_code = 200
    return Mock(return_value=response)


@pytest.fixture
def http_client(memory_cache, send_mock):
    metrics=Mock()
    cache = memory_cache
    client = HttpClient(api_name='test', metrics=metrics, cache=cache, max_retries=3)
    client.get_adapter = Mock(return_value=Mock(send=send_mock))
    return client


def test_create_http_client_default():
    metrics=Mock()
    config = Config()
    cache = Mock()

    client = create_http_client(http_client_name='test', metrics=metrics,
                                cache=cache, config=config)

    assert client
    assert isinstance(client, HttpClient)
    assert client.api_name == 'test'
    assert client._metrics == metrics
    assert client._cache is None
    assert client._cache_ttl == 60
    assert client._max_retries == 0


def test_create_http_client_configured():
    metrics=Mock()
    cache = Mock()
    config = Config({
        'requests': {
            'test': {
                'api_name': 'custom_name',
                'cache': {
                    'enabled': True,
                    'ttl': 100
                },
                'max_retries': 5
            }
        }
    })

    client = create_http_client(http_client_name='test', metrics=metrics,
                                cache=cache, config=config)

    assert client
    assert isinstance(client, HttpClient)
    assert client.api_name == 'custom_name'
    assert client._metrics == metrics
    assert client._cache == cache.http
    assert client._cache_ttl == 100
    assert client._max_retries == 5


def test_http_client(http_client, send_mock):
    response1 = http_client.get('http://url.com')
    assert send_mock.call_count == 1
    assert response1.status_code == 200

    # should take value from cache
    response2 = http_client.get('http://url.com')
    assert send_mock.call_count == 1
    assert response2.status_code == 200


def test_http_client_error(http_client, send_mock):
    def build_response(request, **kwargs):
        response = Response()
        response.headers = []
        response.status_code = 500
        response.request = request
        return response
    send_mock.side_effect = build_response
    response1 = http_client.get('http://url.com')
    assert send_mock.call_count == 4  # 1 + 3 retries
    assert response1.status_code == 500

    response2 = http_client.get('http://url.com')
    assert send_mock.call_count == 8
    assert response2.status_code == 500

    # ensure response class is custom HttpResponse
    assert isinstance(response2, HttpResponse)


def test_http_response():
    request = Request()
    request.headers = {
        'Authorization': 'token',
        'header1': 'value1',
        'header2': 'value2'
    }
    request.method = 'GET'
    request.url = 'http://url.com'
    request.body = None

    response = HttpResponse()
    response.status_code = 400
    response.request = request

    assert response.to_curl(request) == ("curl -X 'GET' -v -H 'Authorization: <...>' "
                                         "-H 'header1: value1' -H 'header2: value2' "
                                         "'http://url.com'")

    assert sorted(response.hide_and_serialize_sensitive_headers(request.headers).split('\n')) == [
        '', 'Authorization: <...>', 'header1: value1', 'header2: value2'
    ]

    with pytest.raises(ExtendedHTTPError) as exc_info:
        # ensure error is raised
        response.raise_for_status()
    assert exc_info.value.response == response
    assert exc_info.value.request == request
