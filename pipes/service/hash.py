class IHash(object):
    # base class for all key-value storage classes

    def save(self, key, value):
        """
        Save value into the hash storage
        :param key: key
        :param value: any python object
        """
        raise NotImplementedError()

    def get(self, key, default=None):
        """
        Get value from the hash storage if exists otherwise returns default value
        :param key: key to lookup
        :param default: default value
        :return: saved value or default
        """
        raise NotImplementedError()

    def delete(self, key):
        """
        Delete key from the key-value storage.
        :param key: key to delete
        :return: True if item was deleted, False if item doesn't exists
        """
        raise NotImplementedError()
