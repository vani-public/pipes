import json
import logging
import os
from collections import defaultdict

import requests
import six

from pypipes.exceptions import InvalidConfigException
from pypipes.context import apply_injections

if six.PY3:
    from configparser import ConfigParser
    from io import StringIO
else:
    from ConfigParser import ConfigParser
    from StringIO import StringIO


logger = logging.getLogger(__name__)


def decrypt(password, ciphertext):
    from pypipes.crypto import decrypt
    if not password:
        raise ValueError('password is required')
    return decrypt(password, ciphertext).decode('utf-8')


def from_url(url, password=None, method='get', content_type=None, **kwargs):
    """
    Download config from url. Function parameters are same as requests.request parameters
    :param url: request url
    :param method: request method
    :param content_type: override resource content type if needed.
    :param password: content decryption password
    :param kwargs: additional parameters for requests.request
    :return: configuration dict
    """
    try:
        with requests.request(method, url, allow_redirects=True, stream=True, **kwargs) as resp:
            if resp.status_code == 404:
                logger.warning('Config %r not exists', url)
                return {}

            resp.raise_for_status()
            response_stream = (StringIO(decrypt(password, resp.content))
                               if password else resp.raw)
            content_type = content_type or resp.headers.get('Content-Type')
            if content_type == 'application/json':
                return json.load(response_stream) or {}
            elif content_type in ('application/x-yaml', 'text/x-yaml', 'application/yaml'):
                import yaml
                return yaml.safe_load(response_stream) or {}
            else:
                raise NotImplementedError('no config parser for %r content type', content_type)
    except Exception:
        logger.exception('Cannot load config file from url: %r', url)
        raise


def from_file(location, password=None, **kwargs):
    if location and os.path.exists(location):
        try:
            result = None
            with open(location, 'r') as f:
                file_name, file_extension = os.path.splitext(location)
                if file_extension == '.enc':
                    # configuration file is encrypted
                    f = StringIO(decrypt(password, f.read()))
                    _, file_extension = os.path.splitext(file_name)

                if file_extension == '.yaml':
                    import yaml
                    result = yaml.safe_load(f, **kwargs) or {}
                elif file_extension == '.json':
                    result = json.load(f, **kwargs) or {}
                elif file_extension in ('.cfg', '.ini'):
                    section = kwargs.pop('section', None)
                    parser = ConfigParser(**kwargs)
                    parser.read(location)
                    if section:
                        result = dict(parser.items(section))
                    else:
                        # convert all sections into a dict
                        result = {name: dict(parser.items(name)) for name in parser.sections()}
                else:
                    raise NotImplementedError('no config parser for %r files', file_extension)
            return result
        except Exception:
            logger.exception('Cannot load config file %r', location)
            raise
    else:
        logger.warning('Config file %r not exists', location)
        return {}


class Config(dict):
    """
    Helper class to access config dict values like an object attributes
    """
    _normalized_keys = None

    def __repr__(self):
        return '{}{}'.format(self.__class__.__name__, super(Config, self).__repr__())

    def __getattr__(self, name):
        try:
            super(Config, self).__getattr__(name)
        except AttributeError:
            if self._normalized_keys is None:
                self._normalized_keys = {self._normalize_key(key): key for key in self}

            if name in self._normalized_keys:
                # ignore method overrides
                value = dict.__getitem__(self, self._normalized_keys[name])
                if isinstance(value, dict):
                    # create an object for nested config dict
                    result = Config(value)
                else:
                    result = value
            else:
                # return empty config
                result = Config()
            setattr(self, name, result)
            return result

    def get_section(self, name):
        result = getattr(self, name)
        if not isinstance(result, Config):
            raise InvalidConfigException('{} is not a config section'.format(name))
        return result

    def get_level(self):
        """
        Get items of this config level only. Filter out all dictionaries
        """
        return {key: value
                for key, value in six.iteritems(self)
                if not isinstance(value, dict)}  # filter out all dicts

    def apply_injections(self, func):
        # create a partial function with default parameters taken from config
        return apply_injections(func, self)

    @staticmethod
    def _normalize_key(key):
        return key.lower().replace('.', '_').replace('-', '_')


def merge(*configs):
    """
    Merge several configurations into one config dictionary
    :param configs: list of configs to merge.
        first config is considered to be a default one
        all others will override or extend its values
    :return: merged config dict
    """
    result = None
    has_result = True
    next_level = defaultdict(list)
    for config in configs:
        values = None
        if callable(config):
            # config will be updated with values generated by the callable
            # callable can return a callable for some key to update the next config level
            values = config(next_level.keys())
        elif isinstance(config, dict):
            values = six.iteritems(config)
        else:
            has_result = True
            result = config

        if values is not None:
            has_result = False
            for key, value in values:
                next_level[key].append(value)

    if has_result:
        return result
    else:
        # merge next config levels recursively
        return {key: merge(*next_level[key])
                for key in next_level}


def from_environ(prefix='', separator='_'):
    prefix = prefix.upper() + separator if prefix else ''

    def environ_items(keys):
        result = []
        if not keys:
            # this func updates only existing keys from default config
            # otherwise does nothing
            return

        for key in keys:
            env_name = prefix + key.upper().replace('.', '_').replace('-', '_')
            if env_name in os.environ:
                value = os.environ[env_name]
                if value.startswith('json:'):
                    try:
                        value = json.loads(value[5:])
                    except ValueError as e:
                        raise ValueError('Cannot load a json value from env variable "{}". '
                                         'Error: {}'.format(env_name, e))
                result.append((key, value))
            else:
                # next config levels should be updated by from_environ too
                result.append((key, from_environ(env_name, separator)))
        return result
    return environ_items


class InheritedConfig(Config):
    """
    Configuration where every next config level inherits all values from previous one.

    config = InheritedConfig({
        'storage': {
            'host': 'localhost',
            'port': 1234,
            'production': {
                'host': 'prod.com'
            }
        }
    })

    # config.storage.get_level()
    {
        'host': 'localhost',
        'port': 1234,
    }

    # config.storage.production.get_level()
    {
        'host': 'prod.com',
        'port': 1234,
    }

    port is inherited from upper config level
    Default configuration is returned if config doesn't contain the item

    # config.storage.stage.get_level()
    {
        'host': 'localhost',
        'port': 1234,
    }

    # config.cache  - top level is empty so there is nothing to inherit.
    {}
    """
    def __getattr__(self, name):
        result = super(InheritedConfig, self).__getattr__(name)
        if isinstance(result, Config):
            #  next level inherits current config as a base
            result = self.__class__(self.get_level(), **result)
            setattr(self, name, result)
        return result


class ClientConfig(InheritedConfig):
    """
    This extension of InheritedConfig just simplify a little the code for getting a plain config.
    client_config.storage.client_name.get_level() -> client_config.storage['client_name']
    """
    def __getitem__(self, service_name):
        return self.get_section(service_name).get_level()


_config = None

ENV_PREFIX = 'PIPES'
ENV_CONFIG = '{prefix}_CONFIG'
ENV_CONFIG_OVERRIDE = '{prefix}_CONFIG_OVERRIDE'


def get_config(env_prefix=ENV_PREFIX, config_path=None, override_path=None, override_env=True):
    global _config
    if not _config:
        config_sources = []
        config_path = config_path or os.environ.get(ENV_CONFIG.format(prefix=env_prefix))
        if config_path:
            config_sources.append(from_file(config_path))
        override_path = override_path or os.environ.get(
            ENV_CONFIG_OVERRIDE.format(prefix=env_prefix))
        if override_path:
            config_sources.append(from_file(override_path))
        if config_sources:
            if override_env:
                config_sources.append(from_environ(env_prefix))
            _config = Config(merge(*config_sources))
        else:
            # empty config if config path is not specified
            _config = Config()
    return _config
