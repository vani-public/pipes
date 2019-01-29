import logging
from copy import deepcopy

logger = logging.getLogger(__name__)


class ICloneable(object):
    def clone(self):
        raise NotImplementedError()


class Cloneable(ICloneable):
    def clone(self):
        return deepcopy(self)


class PipelineJoin(object):
    def to_pipeline(self):
        """
        Converts pipeline join into a pipeline
        :rtype: Pipeline
        """
        raise NotImplementedError()

    def join(self, other):
        """
        Join two pipeline elements together
        :param other: next join in pipeline
        :type other: PipelineJoin
        :return pipeline that includes both pipeline elements
        :rtype: Pipeline
        """
        return self.to_pipeline().join(other)

    def __rshift__(self, other):
        if isinstance(other, PipelineJoin):
            return self.join(other)
        else:
            raise TypeError('cannot build a pipeline with {!r}'.format(other))


class Pipeline(list, Cloneable, PipelineJoin):

    def __repr__(self):
        return 'Pipeline{}'.format(super(Pipeline, self))

    def to_pipeline(self):
        return self

    def join(self, other):
        pipeline = self.clone()
        pipeline.extend(other.clone().to_pipeline())
        return pipeline
