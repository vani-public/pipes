import re

from pypipes.service import key

from pypipes.context.factory import LazyContext

RE_VERSION = re.compile(r'[^.\d]')


class ICursorStorage(object):
    def get(self, cursor_name):
        """
        Get cursor value
        :param cursor_name: unique cursor name
        :return: cursor value or None
        """
        raise NotImplementedError()

    def save(self, cursor_name, value):
        """
        Save cursor value
        :param cursor_name: unique cursor name
        :param value: cursor value
        :type value: object
        """
        raise NotImplementedError()

    def clear(self):
        """
        Clear cursor collection.
        This method is used by cursor CLI only
        """
        raise NotImplementedError()

    def list(self):
        """
        List all cursor names
        This method is used by cursor CLI only
        :return: list of cursor ids
        :rtype: list[str]
        """
        raise NotImplementedError()


class CursorStorage(ICursorStorage):
    def __init__(self, storage):
        """
        :type storage: pypipes.service.storage.IStorage
        """
        self._storage = storage
        self._all_collection = 'all_cursors'

    def get(self, cursor_name):
        result = self._storage.get_item(cursor_name)
        return result and result.value

    def save(self, cursor_name, value):
        self._storage.save(cursor_name, value, collections=[self._all_collection])

    def clear(self):
        self._storage.delete_collection(self._all_collection, delete_items=True)

    def list(self):
        return list(self._storage.get_collection(self._all_collection, only_ids=True))


class VersionedCursorStorage(CursorStorage):
    version_collection_prefix = 'cursor_version:'
    _version_cache = {}

    def __init__(self, storage, version):
        """
        These storage returns saved cursor for current or any previous version
        but ignore any cursor saved for next version.
        :type storage: pypipes.service.storage.IStorage
        :param version: cursor version
        """
        super(VersionedCursorStorage, self).__init__(storage)
        self._version = version
        self._current_version_tuple = self._version_tuple(version)
        self._version_collection = self.version_collection_prefix + version

    @classmethod
    def _version_tuple(cls, version):
        if version not in cls._version_cache:
            # save all parsed versions in class cache
            only_numbers = RE_VERSION.sub('', version)
            cls._version_cache[version] = tuple((int(value) if value else 0)
                                                for value in only_numbers.split('.'))
        return cls._version_cache[version]

    def get(self, cursor_name):
        cursors = tuple(self._storage.get_collection(cursor_name))
        if not cursors:
            # try to get cursor saved by CursorStorage
            # this allows migrate data from CursorStorage to VersionedCursorStorage
            return super(VersionedCursorStorage, self).get(cursor_name)

        for cursor in cursors:
            # lookup for exact version match first
            if self._version_collection in cursor.collections:
                # exact version is matched
                return cursor.value

        # determinate best cursor version
        best_value = None
        best_version = None
        for cursor in cursors:
            # lookup for cursor version in cursor collections list
            version = None
            for col in cursor.collections:
                if col.startswith(self.version_collection_prefix):
                    version = self._version_tuple(col[len(self.version_collection_prefix):])
                    break

            if not version:
                # ignore invalid values
                continue
            if (best_version is None or best_version < version) and (
                    version <= self._current_version_tuple):
                # found more recent cursor version
                best_value = cursor.value
                best_version = version
        return best_value

    def save(self, cursor_name, value):
        versioned_name = key(cursor_name, self._version)
        self._storage.save(versioned_name, value,
                           collections=[cursor_name,
                                        self._all_collection,
                                        self._version_collection])

    def clear(self):
        # clear current version cursors only
        self._storage.delete_collection(self._version_collection, delete_items=True)


cursor_storage_context = LazyContext(lambda storage: CursorStorage(storage.cursor))
versioned_cursor_storage_context = LazyContext(
    lambda storage, cursor_version=None, program=None:
    VersionedCursorStorage(storage.cursor, cursor_version or program.version or ''))
