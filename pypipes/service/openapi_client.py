try:
    from urlparse import urljoin
except ImportError:
    from urllib.parse import urljoin

from bravado.client import SwaggerClient
from bravado.requests_client import RequestsClient
from pypipes.exceptions import InvalidConfigException
from pypipes.service.http_client import HttpClient, extract_api_name

from pypipes.context.factory import LazyContextPoolFactory


def create_client(url,
                  api_name=None, use_models=False,
                  metrics=None, cache=None,
                  operation_cache_ttl=None,
                  operation_cache_ttl_map=None,
                  max_retries=3):
    """
    Creates a thin client for accessing OpenAPI-based services
    :param url: api root or url of open_api specification. Example 'http://localhost:80/'
    :param api_name: api name
    :type metrics: pypipes.service.metric.IMetrics
    :type cache: pypipes.service.cache.ICache
    :param max_retries: how many times the client should retry a request on error.
    :param operation_cache_ttl: default cache ttl for api operations in seconds
        Set None to prevent operation caching.
    :param operation_cache_ttl_map: defines a specific ttl for some api operations
        otherwise `operation_cache_ttl` value is used
    :type operation_cache_ttl_map: dict[str, int]
    :param use_models: use Python classes (models) instead of dicts
    """
    if api_name is None:
        api_name = extract_api_name(url)
    url = urljoin(url, 'swagger.json')
    http_client = OperationRequestsClient()
    http_client.session = OpenApiSession(api_name,
                                         metrics=metrics,
                                         cache=cache,
                                         operation_cache_ttl=operation_cache_ttl,
                                         operation_cache_ttl_map=operation_cache_ttl_map,
                                         max_retries=max_retries)
    return SwaggerClient.from_url(url, http_client=http_client,
                                  config={'use_models': use_models})


class OpenApiSession(HttpClient):
    def __init__(self, api_name, metrics=None, cache=None,
                 spec_cache_ttl=600,
                 operation_cache_ttl=None,
                 operation_cache_ttl_map=None, max_retries=3):
        """
        :param api_name: api name
        :type metrics: pypipes.service.metric.IMetrics
        :type cache: pypipes.service.cache.ICache
        :param max_retries: how many times the client should retry a request on error.
        :type spec_cache_ttl: cache ttl for swagger specification in seconds
        :param operation_cache_ttl: default cache ttl for api operations in seconds
            Set None to prevent operation caching.
        :param operation_cache_ttl_map: defines a specific ttl for some api operations
            otherwise `operation_cache_ttl` value is used
        :type operation_cache_ttl_map: dict[str, int]
        """
        super(OpenApiSession, self).__init__(api_name=api_name,
                                             metrics=metrics,
                                             cache=cache,
                                             cache_ttl=operation_cache_ttl,
                                             max_retries=max_retries)

        self._cache_all = operation_cache_ttl is not None
        self._operation_cache_ttl_map = dict(operation_cache_ttl_map or [])
        self._spec_cache_ttl = spec_cache_ttl

    def _may_cache(self, request, response=None):
        return super(OpenApiSession, self)._may_cache(request, response) and (
            self._cache_all or
            request.url.endswith('/swagger.json') or  # always cache specification
            request.operation_id in self._operation_cache_ttl_map)

    def _get_cache_ttl(self, request, response):
        if request.url.endswith('/swagger.json'):
            return self._spec_cache_ttl
        return self._operation_cache_ttl_map.get(request.operation_id)

    def _get_metric_tags(self, request):
        tags = super(OpenApiSession, self)._get_metric_tags(request)
        if request.operation_id:
            tags['op'] = request.operation_id
        return tags

    def prepare_request(self, request):
        # Propagate operation_id into prepared request
        prepared_request = super(OpenApiSession, self).prepare_request(request)
        prepared_request.operation_id = request.operation_id
        return prepared_request


class OperationRequestsClient(RequestsClient):
    """
    This RequestsClient attaches an operation ID to the request
    for later use in OpenApiSession
    """
    def request(self, request_params, operation=None, request_config=None):
        future = super(OperationRequestsClient, self).request(request_params,
                                                              operation=operation,
                                                              request_config=request_config)
        future.future.request.operation_id = operation.operation_id if operation else None
        return future


def configure_api_client_factory(api_name, config, cache=None, metrics=None):
    """
    A factory that creates api client by its config
    Config should contain a separate config block for each used API name

    Example:
    Config({"api": {
        "connector": {
            "url": "http://localhost:80/connector/",  # url is required
            "max_retries": 3,
            "use_models": False,
            "operation_cache_ttl_map": {
                "api.health": 60  # cache health request for 1 min
            }
        },
        "org_profile": {
            "url": "http://localhost:80/org_profile/"
        }
    })
    :param api_name: service name in pool
    :type config: pypipes.config import Config
    :type metrics: pypipes.service.metric.IMetrics
    :type cache: pypipes.context.pool.IContextPool[pypipes.service.cache.ICache]
    :return: API client
    :rtype: SwaggerClient
    """
    cache = cache and cache.api  # get api cache from cache context pool
    api_config = config.api.get_section(api_name)
    if 'url' not in api_config:
        raise InvalidConfigException(
            'Config value "config.api.{}.url" is required'.format(api_name))
    return api_config.apply_injections(create_client)(api_name=api_name,
                                                      cache=cache, metrics=metrics)


api_client_pool = LazyContextPoolFactory(configure_api_client_factory)
