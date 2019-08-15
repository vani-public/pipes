from __future__ import print_function

import logging

from pypipes.config import Config
from pypipes.service.cache import memory_cache_pool
from pypipes.service.http_client import http_client_context, http_client_pool
from pypipes.service.metric import log_metrics_context
from requests import ConnectionError

from pypipes.context import LazyContextCollection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# this is an example of http_client_context usage.
# http_client_context is a lazy context
# for using in infrastructure setup for pipeline processing

# it could use metrics and cache context if they are defined
# so lets create a context collection that will initialize the context for us
# like pipeline infrastructure does it.

enable_cache = Config(http_client={
    'cache': {'enabled': True}  # enable caching in http_client context
})

context = LazyContextCollection(
    http=http_client_context,
    cache=memory_cache_pool,  # we want to cache response
    metrics=log_metrics_context,  # we want to collect http_client metric
    config=Config(enable_cache),
    logger=logger)  # log is used by log_metrics_context to output metrics


# lets initialize our context
http_client = context['http']
print('HTTP client is : ', http_client)

# http_client inherits all features of requests.Session
# so lets make some test call. Metrics service should log a metric about the request
response = http_client.get('http://google.com')
print('First call status: ', response.status_code)

# lets call again. Any metric for this call should be not emitted
# because the call response is already in a cache
response = http_client.get('http://google.com')
print('Second call status: ', response.status_code)

# if you want to use several clients with different configurations you may use `http_client_pool`
# lets update our context a little
pool_config = Config(http_client={
    'cached': {'cache': {'enabled': True}},  # this one has caching
    'retry': {'max_retries': 5}  # this one has request retry
})

context['config'] = pool_config
context['http'] = http_client_pool

# initialize lazy context
http_client = context['http']

print('Client context pool:', http_client)
print('First call with cache:', http_client.cached.get('https://yahoo.com').status_code)
print('Second call with cache:', http_client.cached.get('https://yahoo.com').status_code)

# now lets use another client with request retry and no cache
print('Third call no cache:', http_client.retry.get('https://yahoo.com').status_code)

try:
    http_client.retry.get('http://invalid_domain.com').status_code
except ConnectionError:
    # you might see in log that this call was retried 5 times
    # and finally failed with ConnectionError
    print('Error was raised')

# if http_client for some name is not configured, cache will be disabled by default
print('Not configured call:', http_client.not_configured.get('https://yahoo.com').status_code)

# By default request metric will have 'api_name' tag only
# however you may pass additional metric tags in request
print('Request with custom tags:', http_client.not_configured.get(
      'https://yahoo.com', params={'q': 'test'}, tags={'app': 'example_http'}).status_code)

# call without any context creates a default client without any addons
# that act like a regular requests.Session()
print('No context at all:', http_client_context({}).get('https://yahoo.com').status_code)
