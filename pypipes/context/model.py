import six
from pypipes.model import Model

from pypipes.context import IContextFactory, apply_context_to_kwargs
from pypipes.context.manager import pipe_contextmanager


class IModelSerializer(object):
    def __init__(self, name, context):
        self.name = name
        self.context = context
        super(IModelSerializer, self).__init__()

    def deserialize(self, value):
        """
        Deserialize value
        :param value: serialized model value
        :return: model
        :type: object
        """
        raise NotImplementedError()

    def serialize(self, model):
        """
        Serialize model object
        :param model: model object
        :return: serialized value
        """
        raise NotImplementedError()


class ModelSerializer(IModelSerializer):
    model_class = None

    def deserialize(self, value):
        if value is None:
            return self.model_class()
        # ensure model type is correct
        if not isinstance(value, dict):
            raise ValueError('Dictionary value is expected for model {!r}'.format(self.name))
        return self.model_class(value)

    def serialize(self, model):
        assert isinstance(model, Model)
        return model.to_dict()

    @classmethod
    def create(cls, model_cls):
        """
        Build model serializer for custom model class
        :param model_cls: model class
        :type model_cls: Type[Model]
        :rtype: ModelSerializer
        """
        class CustomModelSerializer(cls):
            model_class = model_cls
        return CustomModelSerializer


class TransparentSerializer(IModelSerializer):
    model_class = None

    def deserialize(self, value):
        if value is None:
            return self.model_class()
        # ensure model type is correct
        if not isinstance(value, self.model_class):
            raise ValueError(
                'Type of model {!r} is invalid. {} expected, but {} received'.format(
                    self.name, self.model_class.__name__, value.__class__.__name__))
        return value

    def serialize(self, model):
        # return model as is.
        return model

    @classmethod
    def create(cls, model_cls):
        """
        Build transparent serializer for custom model class
        :param model_cls: model class
        :type model_cls: type
        :rtype: TransparentSerializer
        """
        class CustomTransparentSerializer(cls):
            model_class = model_cls
        return CustomTransparentSerializer


def get_model_serializer(model_type):
    if isinstance(model_type, IContextFactory):
        # model type should be taken from context later.
        return model_type
    else:
        assert isinstance(model_type, type)

    if issubclass(model_type, (IModelSerializer, )):
        # this model knows how to serialize itself
        return model_type
    elif issubclass(model_type, Model):
        # replace Model class with model serializer
        return ModelSerializer.create(model_type)
    else:
        # replace with transparent serializer
        return TransparentSerializer.create(model_type)


def _serialization_filter(name, model, serializer):
    """
    Build model serialization filter
    """
    def _message_filter(msg):
        # forward models to next processor
        value = serializer.serialize(model)
        if value:
            # if model is empty it's not included into the response message
            msg[name] = value
        return msg
    return _message_filter


def model(**models):
    """
    Build specific type of pipe processor that enriches some data model in an input model
    and forwards it to a next processor

    Usage example:
        Library = list  # list is here just for example. Could be any object class

        @model(library=Library)
        @pipe_processor
        def fill_library(library):
            library.append('book1')
            library.append('book2')
            return {'ready': True}

    Next processor will receive a message like this
    {
        'library': Library(['book1', 'book2']),
        'ready': True
    }

    :param models: a dictionary {message key: Model type}
        Typically pipeline message is a dictionary.
        Here you can specify which key of a message is a data model
        and how it have to be represented in processor context
    :return: pipe_contextmanager
    """
    assert models
    models = {name: get_model_serializer(model_type) for name, model_type in six.iteritems(models)}

    @pipe_contextmanager
    def model_contextmanager(message, response, injections):
        model_context = {}
        # get model serializer classes from context if any
        model_serializers = apply_context_to_kwargs(models, injections)
        for model_key, serializer_class in six.iteritems(model_serializers):
            serializer = serializer_class(model_key, injections)  # type: IModelSerializer
            model_context[model_key] = serializer.deserialize(message.get(model_key))
            response.add_message_filter(_serialization_filter(model_key,
                                                              model_context[model_key],
                                                              serializer))
        yield model_context
    return model_contextmanager
