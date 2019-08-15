from __future__ import print_function

import os
from pprint import pprint

from pypipes import (
    from_file, Config, from_environ, merge, from_url, InheritedConfig, ClientConfig)

# read config dict from yaml file
print('\nYAML config')
config = Config(from_file('config.yaml'))
# config is a dictionary
pprint(dict(config), width=1)

# read config dict from ini file by ConfigParser
print('\nINI config')
config = Config(from_file('config.ini'))
pprint(dict(config), width=1)

# read config dict from json file
print('\nJSON config')
config = Config(from_file('config.json'))
pprint(dict(config), width=1)

# all config values are accessible as config object attributes
print('key1 =', config.key1)
print('key2 =', config.key2)
print('key2.key2_1 =', config.key2.key2_1)

# `merge` build config chain. First file 'config.json' is a base default config,
# next files will override and extent it
print('\nMerge config.yaml and config.ini')
config = Config(merge(from_file('config.yaml'), from_file('config.ini', section='default')))
pprint(dict(config), width=1)

# next overrides default config values with system environment variables
# lets setup some environ values for from_environ example
os.environ['TEST_KEY1'] = 'env value1'
os.environ['TEST_KEY3_KEY3_3_KEY3_3_2'] = 'env value3.3.2'
# environ value could be a json dump.
os.environ['TEST_KEY2'] = 'json:{"key2.1": "env value2.1", "key2.3": "env added 2.3"}'

print('\nMerge config.json and environ')
config = Config(merge(from_file('config.json'), from_environ('TEST')))

pprint(dict(config), width=1)
# next config value is taken from TEST_KEY3_KEY3_3_KEY3_3_2 environment variable
print('config.key3.key3_3.key3_3_2 =', config.key3.key3_3.key3_3_2)

# Configuration file maybe encrypted with `openssl aes-256-cbc`.
# In this case encrypted config file must be marked with additional file extension '.enc'
# and you have provide a decryption password to load the config from file
print('\nLoad encrypted config')
config = Config(from_file('config.yaml.enc', password='example'))
pprint(dict(config), width=1)

# Config may be also downloaded from web server
# from_url supports all parameters of requests.request
print('\nDownload encrypted config')
try:
    config = Config(from_url('https://s3.amazonaws.com/config.yaml.enc',
                             content_type='application/yaml',
                             password='example'))
    pprint(dict(config), width=1)
except Exception:
    # make sure download url is correct
    pass


# In some cases it's useful to have a multi level config where each next level inherits
# and extends or updates the previous configuration level.

config = InheritedConfig({
    'storage': {
        'host': 'localhost',
        'port': 1234,
        'production': {
            'host': 'prod.com'
        },
        'stage': {
            'host': 'stage.com',
            'debug': True
        },
    }})

print('\nBase storage config:', config.storage.get_level())

# production and stage config inherits port from top level
print('Production storage config:', config.storage.production.get_level())
print('Stage storage config:', config.storage.stage.get_level())

# develop storage is not especially configured so default values are used
print('Develop storage config:', config.storage.get_section("develop").get_level())

# ClientConfig is an InheritedConfig adapted for getting client configuration for some service
# The getting of named config code is simpler with using of it.
config = ClientConfig(config)
print('\nStage storage client config:', config.storage["stage"])
print('Develop storage client config:', config.storage["develop"])
