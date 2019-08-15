from pypipes.processor import pipe_processor


class Items(object):
    def __init__(self, iterable, name='item'):
        self.name = name
        self.iterable = iterable

    @property
    def for_each(self):

        @pipe_processor
        def for_each(message):
            for value in self.iterable:
                msg = dict(message)
                msg[self.name] = value
                yield msg
        return for_each


class Collection(object):
    def __init__(self, collection, storage='default'):
        self.storage = storage
        self.collection = collection

    def _format_name(self, element):
        return '{}_{}_{}'.format(self.storage, self.collection, element)

    def transform_each(self, transform, unique=False, only_ids=False):

        @pipe_processor
        def for_each(message, storage):
            unique_cache = set() if unique else None

            for item in storage[self.storage].get_collection(self.collection, only_ids=only_ids):
                for msg in transform(item):
                    if unique:
                        msg_hash = tuple(msg.items())
                        if msg_hash in unique_cache:
                            continue
                        unique_cache.add(msg_hash)
                    # extend input message dict with new values
                    yield dict(message, **msg)
        return for_each

    def for_each(self, name=None, unique=False):
        name = name or self._format_name('item')

        def transform(item):
            yield {name: {
                'id': item.id,
                'value': item.value,
                'aliases': item.aliases,
                'collections': item.collections}}
        return self.transform_each(transform, unique=unique)

    def for_each_id(self, name=None, unique=False):
        name = name or self._format_name('id')

        def transform(item):
            yield {name: item}  # item is an ID because only_ids=True
        return self.transform_each(transform, unique=unique, only_ids=True)

    def for_each_value(self, name=None, unique=False):
        name = name or self._format_name('value')

        def transform(item):
            yield {name: item.value}
        return self.transform_each(transform, unique=unique)

    def for_each_alias(self, name=None, unique=True):
        # emit a message for each unique alias
        name = name or self._format_name('alias')

        def transform(item):
            return [{name: alias} for alias in item.aliases]
        return self.transform_each(transform, unique=unique)

    def for_each_collection(self, name=None, unique=True):
        # emit a message for each unique collection
        name = name or self._format_name('collection')

        def transform(item):
            return [{name: alias} for alias in item.collection]
        return self.transform_each(transform, unique=unique)
