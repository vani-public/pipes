import uuid
from copy import deepcopy

from pypipes.context.model import IModelSerializer
from pypipes.model import IDestroyable, Model


class IModelPatch(object):
    def apply(self, model, context):
        raise NotImplementedError()

    def destroy(self, context):
        raise NotImplementedError()


class PatchCollection(IModelPatch):
    def __init__(self, patches=None):
        self._patches = patches or []  # type: list[IModelPatch]

    def __repr__(self):
        return '{}'.format(self._patches)

    def __nonzero__(self):
        return self.__bool__()

    def __bool__(self):
        return any(self._patches)

    def add_patch(self, patch):
        self._patches.append(patch)

    def apply(self, model, context):
        for patch in self._patches:
            patch.apply(model, context)

    def destroy(self, context):
        for patch in self._patches:
            patch.destroy(context)
        self._patches = []


class ModelPatch(PatchCollection):

    def __init__(self, item=None, value=None):
        super(ModelPatch, self).__init__()
        self._item = item
        self._value = value

    def __repr__(self):
        return '{}({!r}, {!r}, {})'.format(self.__class__.__name__,
                                           self._item, self._value,
                                           super(ModelPatch, self).__repr__())

    def __nonzero__(self):
        return self.__bool__()

    def __bool__(self):
        return bool(self._item or self._value or super(ModelPatch, self).__bool__())

    @classmethod
    def create(cls, context, item=None, value=None):
        return cls(item, value)

    @staticmethod
    def _apply_value(model, item, value):
        if value:
            if item:
                # assign value to path
                setattr(model, item, value[0])
            else:
                # call with value parameters
                model = model(*value[0], **value[1])
        elif item:
            model = getattr(model, item)
        return model

    def apply(self, model, context):
        model = self._apply_value(model, self._item, self._value)
        # apply nested patches
        super(ModelPatch, self).apply(model, context)


class Patcher(object):
    def __init__(self, context, patch=None, patch_class=ModelPatch):
        """
        :type context: dict, LazyContextCollection
        :param patch: patch
        :type patch: PatchCollection
        """
        self._patch_class = patch_class
        self._context = context
        self._patch = PatchCollection() if patch is None else patch

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, self._patch)

    def __nonzero__(self):
        return self.__bool__()

    def __bool__(self):
        return bool(self.patch)

    @property
    def patch(self):
        return self._patch

    def __getattr__(self, key):
        return self._add_patch(key)

    def __setattr__(self, key, value):
        if key.startswith('_'):
            super(Patcher, self).__setattr__(key, value)
        else:
            # save tuple with one element to correctly handle None value
            self._add_patch(key, (value,))

    def __call__(self, *args, **kwargs):
        return self._add_patch(None, (args, kwargs))

    def _add_patch(self, item=None, value=None):
        patch = self._patch_class.create(self._context, item, value)
        self.patch.add_patch(patch)
        return self.__class__(self._context, patch, patch_class=self._patch_class)

    def apply(self, model):
        self.patch.apply(model, self._context)

    def destroy(self):
        self.patch.destroy(self._context)


class ModelStorage(ModelPatch):
    """
    This patch uses storage.patch (IHash) to save patch data
    """
    def __init__(self, item=None, value_id=None):
        super(ModelStorage, self).__init__(item, value_id)

    @classmethod
    def _get_storage(cls, context):
        storage = context.get('storage')
        storage = storage and storage.patch
        if not storage:
            raise ValueError('Patch requires "storage.patch" context')
        return storage

    @classmethod
    def create(cls, context, item=None, value=None):
        value_id = None
        if value:
            storage = cls._get_storage(context)
            value_id = uuid.uuid4().hex
            storage.save(value_id, value)
        return cls(item, value_id)

    def apply(self, model, context):
        storage = self._get_storage(context)
        value = None
        if self._value:
            value = storage.get(self._value)
            if not value:
                raise ValueError(
                    'Patch value with id {} is corrupted'.format(self._value))
        model = self._apply_value(model, self._item, value)
        # apply nested patches
        super(ModelPatch, self).apply(model, context)

    def destroy(self, context):
        # destroy nested patches
        super(ModelStorage, self).destroy(context)
        # delete saved value from storage
        if self._value:
            storage = self._get_storage(context)
            storage.delete(self._value)


class PatchSerializer(IModelSerializer):
    def deserialize(self, value):
        if value is not None:
            # ensure model type is correct
            if not isinstance(value, PatchCollection):
                raise ValueError(
                    'Type of model {!r} is invalid. '
                    'PatchCollection expected, but {} received'.format(
                        self.name, value.__class__.__name__))
        return Patcher(self.context, value, patch_class=ModelStorage)

    def serialize(self, model):
        """
        :type model: Patcher
        :rtype: PatchCollection
        """
        return model.patch


class PostponeUpdate(IModelSerializer, IDestroyable, Model):
    def __init__(self, name, context):
        super(PostponeUpdate, self).__init__(name, context)
        self.postpone = Patcher(context, patch_class=ModelStorage)
        self._patched_clone = None

    @property
    def patched_clone(self):
        if not self._patched_clone:
            self._patched_clone = model_copy = self.__class__('copy', {})
            model_copy._model = deepcopy(self._model)
            self.postpone.apply(model_copy)
        return self._patched_clone

    def validate(self, name=None):
        # validate patched model
        return super(PostponeUpdate, self.patched_clone).validate()

    def to_dict(self):
        # patched model dict
        return super(PostponeUpdate, self.patched_clone).to_dict()

    def deserialize(self, value):
        if value is not None:
            self._model, patch = value
            self.postpone = Patcher(self.context, patch, patch_class=ModelStorage)
        return self

    def serialize(self, model):
        # send  model dictionary with _content_collection id as part of a message
        return self._model, self.postpone.patch

    def destroy(self):
        self.postpone.destroy()
        self._model = {}
