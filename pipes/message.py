
class Message(dict):

    def __repr__(self):
        return '{}{}'.format(self.__class__.__name__, super(Message, self).__repr__())

    def __getattr__(self, name):
        if name in self:
            return self[name]
        else:
            raise AttributeError(name)

    def __setattr__(self, name, value):
        if hasattr(self, name):
            super(Message, self).__setattr__(name, value)
        else:
            self[name] = value

    def __delattr__(self, name):
        del self[name]


class FrozenMessage(Message):
    def __setattr__(self, name, value):
        # message should be used as frozen dict
        # this error is just a reminder
        raise AttributeError()

    def __setitem__(self, key, value):
        raise AttributeError()

    def update(self, *args, **kwargs):
        raise AttributeError()

    def __delitem__(self, item):
        raise AttributeError()

    def __delattr__(self, name):
        raise AttributeError()


class MessageUpdate(Message):
    __deleted_items = None

    def __init__(self, *args, **kwargs):
        self.__deleted_items = set()
        super(MessageUpdate, self).__init__(*args, **kwargs)

    def __nonzero__(self):
        return self.__bool__()

    def __bool__(self):
        return bool(self.__deleted_items) or bool(self.keys())

    def __delitem__(self, item):
        # save item to delete the key from original message later if exist
        self.__deleted_items.add(item)
        try:
            super(MessageUpdate, self).__delitem__(item)
        except KeyError:
            pass

    def merge_with_message(self, original_message):
        result = dict(original_message)
        for item in self.__deleted_items:
            result.pop(item, None)
        result.update(self)
        return result
