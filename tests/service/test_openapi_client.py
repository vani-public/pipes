from bravado.client import SwaggerClient
from pypipes.config import Config
from mock import Mock, patch

from pypipes.service.openapi_client import configure_api_client_factory

API_SPEC = {
    'swagger': '2.0',
    'info': {
        'title': 'Example API',
        'version': '1.0.0'
    },
    'consumes': ['application/json'],
    'produces': ['application/json'],
    'paths': {
        '/health': {
            'get': {
                'operationId': 'api.health',
                'responses': {
                    '200': {
                        'description': 'Return ok status of health check'
                    }
                }
            }
        }
    }
}


@patch('bravado.client.Loader.load_spec')
def test_configure_api_client_factory(load_spec_mock):
    load_spec_mock.return_value = API_SPEC
    metrics=Mock()
    config = Config({
        'api': {
            'test': {
                'url': 'http://url.com'
            }
        }
    })
    cache = Mock()

    client = configure_api_client_factory('test', config, cache=cache, metrics=metrics)
    assert client
    assert isinstance(client, SwaggerClient)
    load_spec_mock.assert_called_once_with('http://url.com/swagger.json')


@patch('bravado.client.Loader.load_spec')
def test_configure_api_client_factory_custom_config(load_spec_mock):
    load_spec_mock.return_value = API_SPEC
    metrics = Mock()

    operation_cache_ttl_map = {'api.health': 60}
    config = Config({
        'api': {
            'test': {
                'url': 'http://url.com',
                'max_retries': 5,
                'operation_cache_ttl': 100,
                'operation_cache_ttl_map': operation_cache_ttl_map
            }
        }
    })
    cache = Mock()

    client = configure_api_client_factory('test', config, cache=cache, metrics=metrics)
    assert client
    assert isinstance(client, SwaggerClient)
    load_spec_mock.assert_called_once_with('http://url.com/swagger.json')

    http_client = client.swagger_spec.http_client.session
    assert http_client.api_name == 'test'
    assert http_client._metrics == metrics
    assert http_client._cache == cache.api
    assert http_client._operation_cache_ttl_map == operation_cache_ttl_map
    assert http_client._max_retries == 5
