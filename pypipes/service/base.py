import hashlib
from base64 import b64encode


def hash_key(key_value):
    return b64encode(hashlib.md5(key_value).digest())[:-2]  # base64 always ends with '=='


class ComplexKey(object):
    NAME_SEPARATOR = '.'
    MAX_KEY_LENGTH = None

    def __init__(self, prefix=None):
        self._prefix = (prefix + self.NAME_SEPARATOR) if prefix else ''

    def format_key(self, *key_parts):
        """
        Join all key parts with NAME_SEPARATOR
        :param key_parts: list of key parts to join
        :return: complex key string
        """
        result = self.NAME_SEPARATOR.join(map(str, key_parts))
        if self.MAX_KEY_LENGTH and len(result) + len(self._prefix) > self.MAX_KEY_LENGTH:
            # it might be important to have a fixed prefix so we hash only the variable part
            result = hash_key(result)
        return '{}{}'.format(self._prefix, result)


class MemcachedComplexKey(ComplexKey):
    MAX_KEY_LENGTH = 250
