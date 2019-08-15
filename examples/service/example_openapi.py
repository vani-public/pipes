from __future__ import print_function

import logging

from pypipes.config import Config
from pypipes.context import LazyContextCollection
from pypipes.exceptions import InvalidConfigException
from pypipes.service.cache import memory_cache_pool
from pypipes.service.metric import log_metrics_context

from pypipes.service.openapi_client import api_client_pool

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# this is an example of api_client_context usage.
# api_client_context is a lazy context
# for using in infrastructure setup for pipeline processing

# it could use metrics and cache context if they are defined
# so lets create a context collection that will initialize the context for us
# like pipeline infrastructure does it.

API_SERVICE_URL = "http://localhost:8000/api/v1/doc/"
api_config = Config(api={
    'incidents': {
        'url': API_SERVICE_URL
    }
})

context = LazyContextCollection(
    api=api_client_pool,
    cache=memory_cache_pool,  # we plan to cache some responses
    metrics=log_metrics_context,  # we want to collect request metrics
    config=Config(api_config),
    logger=logger)  # logger is used by log_metrics_context to output metrics


# lets initialize our context
api = context['api']
print('API client is: ', api)

# lets call some API operation
directory = {
    'connection_uid': '1',
    'statuses': ['active'],
    'types': ['user', 'group']
}
result = api.incidents.directories.directory_add(directory=directory).result()
print('First api call:', result)
# By default api client caches only a swagger specification. You should see this in metrics log
print('Second api call (swagger spec is cached):',
      bool(api.incidents.directories.directory_info(
          directory_uid={'directory_uid': result['directory_uid']}).result()))

# if you want to cache some operation you should extent your api config a little
# you could also use `operation_cache_ttl` parameter to cache all operations
api_config = Config(api={
    'incidents': {
        'url': API_SERVICE_URL,
        'operation_cache_ttl_map': {
            'directory_info': 60  # cache incidents_list for 1 min
        }
    }
})

# reinitialize api context
context.update(config=api_config,
               api=api_client_pool)
api = context['api']

# this request should cache operation result
print('Third api call (swagger spec is cached):',
      bool(api.incidents.directories.directory_info(
          directory_uid={'directory_uid': result['directory_uid']}).result()))

# this one gets response from cache instead of making a call. See metrics log.
print('Fourth api call (operation is cached):',
      bool(api.incidents.directories.directory_info(
          directory_uid={'directory_uid': result['directory_uid']}).result()))

# Attention! api_client_context requires a configuration section for each api that you want to use
# because it needs to know an api address at least for a proper work.
# Therefore it will raise an error, if configuration for your api is not found,
# unlike regular context pool, that usually uses a default configuration to create a context.
try:
    api.unknown
except InvalidConfigException:
    print('Error raised')
