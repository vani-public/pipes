import logging
from collections import defaultdict, namedtuple
from copy import deepcopy
from threading import Lock

from pypipes.context.config import client_config
from pypipes.service.base import ComplexKey
from pypipes.service.base_client import RedisClient, get_redis_client
from pypipes.service.hash import IHash

from pypipes.context.factory import ContextPoolFactory, LazyContextPoolFactory

logger = logging.getLogger(__name__)

StorageItem = namedtuple('StorageItem', ['id', 'value', 'aliases', 'collections'])


class IStorage(IHash):
    # Base storage class
    # inherits get and delete methods from IHash
    # interface of the save method is extended but still compatible with IHash.save

    def save(self, primary_id, item, aliases=None, collections=None):
        """
        Save item
        :param item: item value
        :param primary_id: item id
        :param aliases: list of item id aliases
        :type aliases: list(string)
        :param collections: list of collections to include the item
        :type collections: list(string)
        :return:
        """
        raise NotImplementedError()

    def get_item(self, item_id):
        """
        Get StorageItem from the storage
        :param item_id: item primary id or alias
        :return: Item value or None if not found
        :rtype: StorageItem
        """
        raise NotImplementedError()

    def add_alias(self, primary_id, alias_id):
        """
        Assign a new alias to item with primary_id. If alias already exists it's moved
        to target item
        :param alias_id: alias id
        :param primary_id: primary id of target item
        :return: True if an alias was assigned, False if target item not found.
        """
        raise NotImplementedError()

    def delete_alias(self, alias_id):
        """
        Delete alias. Alias target item will be not deleted.
        To delete an item use delete method.
        :param alias_id: alias id
        :return: True if alias was deleted, False if alias not found
        """
        raise NotImplementedError()

    def get_collection(self, collection_id, only_ids=False):
        """
        Yield all collection items.
        :param collection_id: collection id
        :param only_ids: if True return only item ids
        :return: collection items if only_ids=False or item ids if only_ids=True
        :rtype: iterator
        """
        raise NotImplementedError()

    def delete_collection(self, collection_id, delete_items=False):
        """
        Remove all items from collection and delete collection.
        :param collection_id: collection id
        :param delete_items: if True - delete all items that was included into this collection
        :return: True if collection was deleted, False if collection not found
        """
        raise NotImplementedError()


class MemStorage(IStorage):
    def __init__(self):
        self._storage = {}
        self._aliases = {}
        self._collections = defaultdict(set)
        self._sync = Lock()

    def save(self, primary_id, item, aliases=None, collections=None):
        aliases = aliases or []
        collections = collections or []
        with self._sync:
            self._delete_alias(primary_id)
            self._delete_item(primary_id)
            for alias_id in aliases:
                self._delete_alias(alias_id)
            # save the item
            self._storage[primary_id] = (deepcopy(item), set(aliases), set(collections))
            # create item aliases
            self._aliases.update((alias_id, primary_id) for alias_id in aliases)
            # append the item into collections
            for collection_id in collections:
                self._collections[collection_id].add(primary_id)

    def get(self, key, default=None):
        result = self.get_item(key)
        return result.value if result else default

    def get_item(self, item_id):
        with self._sync:
            primary_id = self._aliases.get(item_id, item_id)
            return self._get_item(primary_id)

    def _get_item(self, primary_id):
        if primary_id and primary_id in self._storage:
            return StorageItem(primary_id, *deepcopy(self._storage[primary_id]))
        else:
            return None

    def delete(self, item_id):
        with self._sync:
            primary_id = self._aliases.get(item_id, item_id)
            return self._delete_item(primary_id)

    def _delete_item(self, primary_id):
        if primary_id and primary_id in self._storage:
            _, alias_ids, collection_ids = self._storage.pop(primary_id)
            for alias_id in alias_ids:
                self._aliases.pop(alias_id, None)
            for collection_id in collection_ids:
                self._collections[collection_id].discard(collection_id)
            return True
        else:
            return False

    def add_alias(self, primary_id, alias_id):
        with self._sync:
            self._delete_alias(alias_id)
            if primary_id in self._storage:
                self._aliases[alias_id] = primary_id
                self._storage[primary_id][1].add(alias_id)

    def delete_alias(self, alias_id):
        with self._sync:
            self._delete_alias(alias_id)

    def _delete_alias(self, alias_id):
        primary_id = self._aliases.pop(alias_id, None)
        # remove alias_id from item
        if primary_id and primary_id in self._storage:
            self._storage[primary_id][1].discard(alias_id)

    def get_collection(self, collection_id, only_ids=False):
        with self._sync:
            items = tuple(self._get_item(primary_id)
                          for primary_id in self._collections[collection_id])
        for item in items:
            if item:
                yield item[0] if only_ids else item

    def delete_collection(self, collection_id, delete_items=False):
        with self._sync:
            collection = self._collections.pop(collection_id, [])
            for primary_id in collection:
                if primary_id in self._storage:
                    if delete_items:
                        self._delete_item(primary_id)
                    else:
                        # just remove collection_id from item
                        self._storage[primary_id][2].discard(collection_id)


class RedisStorage(RedisClient, ComplexKey, IStorage):

    ITEM_PREFIX = 'i'
    ALIAS_PREFIX = 'a'
    COLLECTION_PREFIX = 'c'

    # KEYS[1] - item id
    # KEYS[2] - alias id
    # ARGV[1] - item value
    # ...
    # ARGV[n] - item alias or collection
    # ARGV[n+1] - 'c' if ARGV[n] is a collection id, 'a' if ARGV[n] is an alias id
    # return 1 if item created, 0 if existing item updated
    LUA_SAVE_SCRIPT = """
          -- delete alias if exists
          local primary = redis.call('get', KEYS[2])
          if primary then
            redis.call('del', KEYS[2])
            redis.call('hdel', primary, KEYS[2])
          end
          -- delete previous item if exists
          local result = 1
          if redis.call('exists', KEYS[1]) == 1 then
              result = 0
              local values = redis.call('hgetall', KEYS[1])
              for i = 1, table.getn(values), 2 do
                local key, value = values[i], values[i + 1]
                if key == 'i' then
                elseif value == 'a' then
                    redis.call('del', key)
                elseif value == 'c' then
                    redis.call('srem', key, KEYS[1])
                end
              end
              redis.call('del', KEYS[1])
          end
          -- save new item
          redis.call('hset', KEYS[1], 'i', ARGV[1])
          for i = 2, table.getn(ARGV), 2 do
            local key, type = ARGV[i], ARGV[i + 1]
            redis.call('hset', KEYS[1], key, type)
            if type == 'a' then
              -- save item alias
              local alias_primary = redis.call('get', key)
              if alias_primary then
                -- remove alias if exist
                redis.call('hdel', alias_primary, key)
              end
              redis.call('set', key, KEYS[1])
            elseif type == 'c' then
              -- add item to collection
              redis.call('sadd', key, KEYS[1])
            end
          end
          return result
      """
    # KEYS[1] - alias name
    # return 0 if alias not exists, 1 is alias was removed
    LUA_DEL_ALIAS_SCRIPT = """
          local primary = redis.call('get', KEYS[1])
          if not primary then
            return 0
          end
          redis.call('del', KEYS[1])
          redis.call('hdel', primary, KEYS[1])
          return 1
      """

    # KEYS[1] - alias key
    # KEYS[2] - item key
    # return 0 if item not exists, 1 is alias was added
    LUA_ADD_ALIAS_SCRIPT = """
          local primary = redis.call('get', KEYS[1])
          if primary then
            redis.call('del', KEYS[1])
            redis.call('hdel', primary, KEYS[1])
          end
          if redis.call('exists', KEYS[2]) == 0 then
            return 0
          end
          redis.call('set', KEYS[1], KEYS[2])
          redis.call('hset', KEYS[2], KEYS[1], 'a')
          return 1
    """

    # KEYS[1] - item key
    # KEYS[2] - alias key
    # return item if alias exists and item exists
    LUA_GET_SCRIPT = """
          local primary = KEYS[1]
          if KEYS[2] and redis.call('exists', KEYS[2]) == 1 then
            primary = redis.call('get', KEYS[2])
          end
          if redis.call('exists', primary) == 0 then
            return
          end
          return {primary, redis.call('hgetall', primary)}
    """

    # KEYS[1] - item key
    # KEYS[2] - alias key
    # return 0 if item not exists, 1 if item was deleted
    LUA_DEL_SCRIPT = """
          local primary = KEYS[1]
          if KEYS[2] and redis.call('exists', KEYS[2]) == 1 then
            primary = redis.call('get', KEYS[2])
          end
          if redis.call('exists', primary) == 0 then
            return 0
          end
          local values = redis.call('hgetall', primary)
          for i = 1, table.getn(values), 2 do
            local key, value = values[i], values[i + 1]
            if key == 'i' then
            elseif value == 'a' then
                redis.call('del', key)
            elseif value == 'c' then
                redis.call('srem', key, primary)
            end
          end
          redis.call('del', primary)
          return 1
    """

    # KEYS[1] - collection key
    # ARGV[1] - if 1 - delete collection items
    # return 0 if collection not exists, 1 - collection was removed
    LUA_DEL_COLLECTION_SCRIPT = """
          if redis.call('exists', KEYS[1]) == 0 then
            return 0
          end
          for _, primary in ipairs(redis.call('smembers', KEYS[1])) do
            if ARGV[1] and ARGV[1] == '1' then
              local values = redis.call('hgetall', primary)
              for i = 1, table.getn(values), 2 do
                local key, value = values[i], values[i + 1]
                if key == 'i' then
                elseif value == 'a' then
                    redis.call('del', key)
                elseif value == 'c' and key ~= KEYS[1] then
                    redis.call('srem', key, primary)
                end
              end
              redis.call('del', primary)
            else
                redis.call('hdel', primary, KEYS[1])
            end
          end
          redis.call('del', KEYS[1])
          return 1
    """

    # KEYS[1] - collection key
    # ARGV[1] - scan cursor position
    # ARGV[2] - if 1 return only ids
    # return collection scan result similar to result of SCAN command.
    LUA_SCAN_COLLECTION_SCRIPT = """
          if redis.call('exists', KEYS[1]) == 0 then
            return
          end
          local result = redis.call('sscan', KEYS[1], ARGV[1], 'count', 100)
          if ARGV[2] and ARGV[2] == '1' then
            return result
          end
          local items = {}
          for index, primary in ipairs(result[2]) do
            items[index] = {primary, redis.call('hgetall', primary)}
          end
          return {result[1], items}
    """

    lua_save = None
    lua_del_alias = None
    lua_get = None
    lua_add_alias = None
    lua_delete = None
    lua_del_collection = None
    lua_scan_collection = None

    def __init__(self, prefix=None, client=None, **kwargs):
        ComplexKey.__init__(self, prefix)
        RedisClient.__init__(self, client=client, **kwargs)
        RedisStorage.register_scripts(self.redis)
        self._prefix_len = {}

    @classmethod
    def register_scripts(cls, redis):
        if cls.lua_save is None:
            cls.lua_save = redis.register_script(cls.LUA_SAVE_SCRIPT)
        if cls.lua_get is None:
            cls.lua_get = redis.register_script(cls.LUA_GET_SCRIPT)
        if cls.lua_delete is None:
            cls.lua_delete = redis.register_script(cls.LUA_DEL_SCRIPT)
        if cls.lua_add_alias is None:
            cls.lua_add_alias = redis.register_script(cls.LUA_ADD_ALIAS_SCRIPT)
        if cls.lua_del_alias is None:
            cls.lua_del_alias = redis.register_script(cls.LUA_DEL_ALIAS_SCRIPT)
        if cls.lua_del_collection is None:
            cls.lua_del_collection = redis.register_script(cls.LUA_DEL_COLLECTION_SCRIPT)
        if cls.lua_scan_collection is None:
            cls.lua_scan_collection = redis.register_script(cls.LUA_SCAN_COLLECTION_SCRIPT)

    def _item_key(self, primary_id):
        return self.format_key(self.ITEM_PREFIX, primary_id)

    def _alias_key(self, alias):
        return self.format_key(self.ALIAS_PREFIX, alias)

    def _collection_key(self, collection):
        return self.format_key(self.COLLECTION_PREFIX, collection)

    def _extract_id(self, key, element_prefix):
        """
        Remove prefix from key
        """
        if element_prefix not in self._prefix_len:
            self._prefix_len[element_prefix] = len(self.format_key(element_prefix, ''))
        return key[self._prefix_len[element_prefix]:]

    def _normalise_item(self, item):
        primary_id, values = item
        item = None
        aliases = set()
        collections = set()
        for key, value in zip(values[::2], values[1::2]):
            if key == 'i' or key == b'i':
                item = self._deserialize(value)
            elif value == 'a' or value == b'a':
                aliases.add(self._extract_id(key.decode(), self.ALIAS_PREFIX))
            elif value == 'c' or value == b'c':
                collections.add(self._extract_id(key.decode(), self.COLLECTION_PREFIX))
        return StorageItem(self._extract_id(primary_id.decode(), self.ITEM_PREFIX),
                           item, aliases, collections)

    def save(self, primary_id, item, aliases=None, collections=None):
        item_key = self._item_key(primary_id)
        alias_key = self._alias_key(primary_id)

        logger.debug('Save item: %s', primary_id)
        args = []
        for alias in aliases or []:
            args.extend((self._alias_key(alias), 'a'))
        for collection in collections or []:
            args.extend((self._collection_key(collection), 'c'))
        return bool(self.lua_save(keys=[item_key, alias_key],
                                  args=[self._serialize(item)] + args,
                                  client=self.redis))

    def get(self, key, default=None):
        result = self.get_item(key)
        return result.value if result else default

    def get_item(self, name, client=None):
        item_key = self._item_key(name)
        alias_key = self._alias_key(name)
        logger.debug('Get item: %s', name)
        result = self.lua_get(
            keys=[item_key, alias_key],
            client=client or self.redis)
        return self._normalise_item(result) if result else None

    def delete(self, name, client=None):
        item_key = self._item_key(name)
        alias_key = self._alias_key(name)
        logger.debug('Delete item: %s', name)
        return bool(self.lua_delete(
            keys=[item_key, alias_key],
            client=client or self.redis))

    def add_alias(self, primary_id, alias_id):
        item_key = self._item_key(primary_id)
        alias_key = self._alias_key(alias_id)

        logger.debug('Add alias: %s => %s', alias_id, primary_id)
        return bool(self.lua_add_alias(keys=[alias_key, item_key],
                                       client=self.redis))

    def delete_alias(self, alias_id, client=None):
        alias_key = self._alias_key(alias_id)
        logger.debug('Delete alias: %s', alias_id)
        return bool(self.lua_del_alias(keys=[alias_key],
                                       client=client or self.redis))

    def get_collection(self, collection_id, only_ids=False):
        collection_key = self._collection_key(collection_id)
        logger.debug('Get collection: %s', collection_id)
        cursor = None
        while cursor != '0' and cursor != b'0':
            result = self.lua_scan_collection(
                keys=[collection_key],
                args=[cursor or b'0', 1 if only_ids else 0],
                client=self.redis)
            if not result:
                return
            cursor, items = result
            for item in items:
                yield (self._extract_id(item.decode(), self.ITEM_PREFIX) if only_ids
                       else self._normalise_item(item))

    def delete_collection(self, collection_id, delete_items=False):
        collection_key = self._collection_key(collection_id)
        logger.debug('Delete collection: %s', collection_id)
        return bool(self.lua_del_collection(keys=[collection_key],
                                            args=[1 if delete_items else 0],
                                            client=self.redis))


memory_storage_pool = ContextPoolFactory(lambda name: MemStorage())
local_redis_storage_pool = ContextPoolFactory(RedisStorage)  # service name => redis prefix
redis_storage_pool = LazyContextPoolFactory(
    lambda name, redis_config=client_config.redis:
    RedisStorage('s:{}'.format(name), client=get_redis_client(redis_config.storage[name])))
