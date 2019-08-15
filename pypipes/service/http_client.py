import re
import time

import requests
import six

try:
    import httplib as HTTPStatus
except ImportError:
    from http import HTTPStatus

from pypipes.context.factory import LazyContext, LazyContextPoolFactory
from pypipes.exceptions import ExtendedException

RE_HOST = re.compile('https?://([^/#?]+).*')


def extract_api_name(url):
    """
    Make a guess at the API name from the URL.
    """
    host = RE_HOST.sub('\\1', url)
    return host


class HttpClient(requests.Session):
    """Augmented version of requests.Session which emits metrics around outgoing API calls"""

    request_latency_metric_name = 'api.out.latency'

    def __init__(self, api_name=None, metrics=None, cache=None, cache_ttl=None, max_retries=3):
        """
        :type metrics: pypipes.service.metric.IMetrics
        :type cache: pypipes.service.cache.ICache
        :param cache_ttl: cache expiration time in seconds
        :param max_retries: how many times the client should retry a request on error.
        """
        super(HttpClient, self).__init__()
        self.api_name = api_name
        self._metrics = metrics
        self._cache = cache
        self._cache_ttl = cache_ttl or 60  # cache for 1 min by default
        self._max_retries = int(max_retries or 0)

    def _cached_response(self, request):
        if self._cache and self._may_cache(request):
            return self._cache.get(request.url)

    def _cache_response(self, request, response):
        if self._cache and self._may_cache(request, response):
            ttl = self._get_cache_ttl(request, response)
            ttl = self._cache_ttl if ttl is None else ttl
            self._cache.save(request.url, response, expires_in=ttl)

    def _may_cache(self, request, response=None):
        """
        Check if client may cache response of such request
        :type request: requests.PreparedRequest
        :type response: requests.Response
        :return: return True if response may be cached
        """
        # any successful request may be cached
        return ((HTTPStatus.OK <= response.status_code < HTTPStatus.BAD_REQUEST)
                if response else True)

    def _get_cache_ttl(self, request, response):
        """
        Returns cache ttl for response of request
        :type request: requests.PreparedRequest
        :type response: requests.Response
        :return: cache ttl for this request or None if default ttl have to be used
        """
        return None  # use default ttl

    def _get_metric_tags(self, request):
        """
        Return metric tags for the request
        :type request: requests.Request
        :return: dictionary of metric tags
        """
        return {'api_name': self.api_name or extract_api_name(request.url)}

    def prepare_request(self, request):
        request_tags = {}
        if isinstance(request.params, tuple):
            # extract tags from request params
            request.params, request_tags = request.params

        prepared_request = super(HttpClient, self).prepare_request(request)
        prepared_request.metric_tags = self._get_metric_tags(request)
        prepared_request.metric_tags.update(request_tags)
        return prepared_request

    def request(self, method, url,
                params=None, data=None, headers=None, cookies=None, files=None,
                auth=None, timeout=None, allow_redirects=True, proxies=None,
                hooks=None, stream=None, verify=None, cert=None, json=None, tags=None, **add_tags):
        if tags or add_tags:
            # `params` parameter is used as a container to forward tags into a prepared request
            # see prepare_request method
            params = (params, dict(tags or {}, **add_tags))
        return super(HttpClient, self).request(
            method, url, params=params,
            data=data, headers=headers, cookies=cookies, files=files,
            auth=auth, timeout=timeout, allow_redirects=allow_redirects, proxies=proxies,
            hooks=hooks, stream=stream, verify=verify, cert=cert, json=json)

    def send(self, request, **kwargs):
        if not hasattr(request, 'metric_tags'):
            # request is not properly prepared. Maybe it's a redirected request.
            return super(HttpClient, self).send(request, **kwargs)

        steam = kwargs.get('stream')

        # Check cache to see if we've looked this up already
        cached = not steam and self._cached_response(request)
        if cached:
            return cached

        tags = dict(request.metric_tags)
        retries = 0
        while True:
            retries += 1
            start = time.time()
            try:
                result = HttpResponse.wrap(super(HttpClient, self).send(request, **kwargs))
                if ((result.status_code >= HTTPStatus.INTERNAL_SERVER_ERROR or
                        result.status_code == HTTPStatus.REQUEST_TIMEOUT) and
                        retries <= self._max_retries):
                    # retry request
                    tags['retries'] = retries
                    continue
                if self._metrics:
                    tags['status'] = result.status_code
                    self._metrics.timing(self.request_latency_metric_name,
                                         (time.time() - start) * 1000, tags=tags)
                if not steam:
                    self._cache_response(request, result)
                return result
            except Exception as e:
                if retries <= self._max_retries:
                    tags['retries'] = retries
                    continue
                if self._metrics:
                    tags['error'] = e.__class__.__name__
                    self._metrics.timing(self.request_latency_metric_name,
                                         (time.time() - start) * 1000, tags=tags)
                raise


class HttpResponse(requests.models.Response):
    sensitive_headers = ['Authorization']

    @classmethod
    def wrap(cls, response):
        """
         Wraps response and provide extra functionality

        :type  response: requests.models.Response
        :return:
        """
        response.__class__ = cls
        return response

    def raise_for_status(self):
        try:
            super(HttpResponse, self).raise_for_status()
        except requests.HTTPError as e:
            message = '{}\n[Request Headers]:\n{}'.format(
                e, self.hide_and_serialize_sensitive_headers(self.request.headers))
            raise ExtendedHTTPError(
                message, response=self, extra={'cURL': self.to_curl(self.request)})

    def hide_and_serialize_sensitive_headers(self, headers):
        serialized_headers = ''
        for k, v in six.iteritems(headers):
            v = '<...>' if k in self.sensitive_headers else v
            serialized_headers += '{}: {}\n'.format(k, v)
        return serialized_headers

    def to_curl(self, request, compressed=False):
        """
        Returns string with curl command by provided request object

        Parameters
        ----------
        compressed : bool
            If `True` then `--compressed` argument will be added to result
        """
        parts = [
            ('curl', None),
            ('-X', request.method),
            ('-v', None)
        ]

        for k, v in sorted(request.headers.items()):
            v = '<...>' if k in self.sensitive_headers else v
            parts += [('-H', '{0}: {1}'.format(k, v))]

        if request.body:
            body = request.body
            if isinstance(body, bytes):
                body = body.decode('utf-8')
            parts += [('-d', body)]

        if compressed:
            parts += [('--compressed', None)]

        parts += [(None, request.url)]

        flat_parts = []
        for k, v in parts:
            if k:
                flat_parts.append(k)
            if v:
                flat_parts.append("'{0}'".format(v))

        return ' '.join(flat_parts)


class ExtendedHTTPError(ExtendedException, requests.HTTPError):
    pass


def create_http_client(http_client_name=None, metrics=None, cache=None, config=None):
    """
    Initialize an http client

    Client config example:
    Config(requests={
        'default': {
            'cache': {'enabled': True,
                      'ttl': 600},
            'max_retries': 5
            }
    })

    :param http_client_name: name of a client. You can config several clients with different names
    :type metrics: pypipes.service.metric.IMetrics
    :type cache: pypipes.context.pool.IContextPool[ICache]
    :type config: pypipes.config.Config
    :return: http client context for infrastructure
    :rtype: HttpClient
    """
    client_config = config and config.requests.get_section(http_client_name or 'default')
    cache_ttl = None
    max_retries = 0
    client_cache = None
    api_name = http_client_name

    if client_config:
        if cache and client_config.cache.get('enabled'):
            client_cache = cache.http
            cache_ttl = client_config.cache.get('ttl')
        max_retries = client_config.get('max_retries')
        api_name = client_config.get('api_name', api_name)

    return HttpClient(api_name=api_name,
                      metrics=metrics,
                      cache=client_cache,
                      cache_ttl=cache_ttl,
                      max_retries=max_retries)


http_client_context = LazyContext(create_http_client)
http_client_pool = LazyContextPoolFactory(create_http_client)
