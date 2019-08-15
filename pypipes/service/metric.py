import logging

import six

from pypipes.context.factory import LazyContext


class IMetrics(object):

    def annotation(self, message, tags=None):
        """
        Send an annotation message
        :param message: message text
        :param tags: tags
        :type tags: dict
        """
        raise NotImplementedError()

    def timing(self, metric, value, tags=None):
        """
        Record a timing
        :param metric: metric name
        :param value: time in milliseconds
        :param tags: tags
        :type tags: dict
        """
        raise NotImplementedError()

    def gauge(self, metric, value, tags=None):
        """
        Record the value of a gauge
        :param metric: metric name
        :param value: value of a gauge
        :param tags: tags
        :type tags: dict
        """
        raise NotImplementedError()

    def increment(self, metric, value=1, tags=None):
        """
        Increment a counter
        :param metric: metric name
        :param value: value to increment on. May be negative
        :param tags: tags
        :type tags: dict
        """
        raise NotImplementedError()


class MetricDecorator(IMetrics):
    def __init__(self, nested, tags=None, annotation_format=None):
        """
        This is a decorator for IMetrics instance
        :type nested: IMetrics
        :param tags: default tags
        :param annotation_format: formatting template for annotation message. May include tags
            Example: "Annotation: [{conn_id}]{mgs}"
        """
        self.tags = tags
        self.nested = nested
        self.annotation_format = annotation_format

    def merge_tags(self, tags):
        if tags:
            return dict(self.tags, **tags) if self.tags else tags
        else:
            return self.tags

    def annotation(self, message, tags=None):
        tags = self.merge_tags(tags)
        if self.annotation_format:
            message = self.annotation_format.format(msg=message, **tags)
        self.nested.annotation(message, tags)

    def timing(self, metric, value, tags=None):
        self.nested.timing(metric, value, self.merge_tags(tags))

    def gauge(self, metric, value, tags=None):
        self.nested.gauge(metric, value, self.merge_tags(tags))

    def increment(self, metric, value=1, tags=None):
        self.nested.increment(metric, value, self.merge_tags(tags))


class LogMetrics(IMetrics):
    """
    A simple metric service that logs metrics instead of sending them to some aggregator
    """
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger('metrics')

    def _format_tags(self, tags=None):
        return (''.join('[{}]'.format('{}:{}'.format(*item)) for item in six.iteritems(tags)) + ' '
                if tags else '')

    def annotation(self, message, tags=None):
        self.logger.info('%sAnnotation: %s', self._format_tags(tags), message)

    def timing(self, metric, value, tags=None):
        self.logger.info('%s%s is %s ms', self._format_tags(tags), metric, value)

    def gauge(self, metric, value, tags=None):
        self.logger.info('%s%s = %s', self._format_tags(tags), metric, value)

    def increment(self, metric, value=1, tags=None):
        # it's probably not good idea to log something on metric increment
        # because they could spam a lot of messages
        pass


datadog_statsd = None


def get_datadog_statsd(config=None):
    """
    Create DataDog statsd client
    :type config: pypipes.config.Config
    :return: statsd client
    :rtype: datadog.DogStatsd
    """
    global datadog_statsd
    if datadog_statsd is None:
        from datadog import DogStatsd
        if config:
            host = config.datadog.get('host', 'localhost')
            port = config.datadog.get('port', 8125)
            global_tags = config.datadog.get('tags', [])
            namespace = config.datadog.get('namespace')
            # DogStatsd client sends metrics to DataDog agent via UDP, like statsd client.
            datadog_statsd = DogStatsd(host, port,
                                       namespace=namespace,
                                       constant_tags=global_tags)
        else:
            # use default
            datadog_statsd = DogStatsd()
    return datadog_statsd


class DataDogMetrics(IMetrics):
    def __init__(self, config=None, tags=None):
        """
        :type config: pypipes.config.Config
        :type tags: dict
        """
        self.default_tags = tags
        self.client = get_datadog_statsd(config)

    def _format_tags(self, tags=None):
        if tags:
            if self.default_tags:
                tags = dict(self.default_tags, **tags)
        else:
            tags = self.default_tags or {}
        return ['{}:{}'.format(*item) for item in six.iteritems(tags)]

    def annotation(self, message, tags=None):
        tags = self._format_tags(tags)
        self.client.event('Annotation', message, alert_type='info', tags=tags)

    def timing(self, metric, value, tags=None):
        tags = self._format_tags(tags)
        self.client.timing(metric, value, tags=tags)

    def gauge(self, metric, value, tags=None):
        tags = self._format_tags(tags)
        self.client.gauge(metric, value, tags=tags)

    def increment(self, metric, value=1, tags=None):
        tags = self._format_tags(tags)
        self.client.increment(metric, value, tags=tags)


def datadog_metrics_factory(program, config=None):
    """
    Build DataDogMetric object
    :type program: pypipes.program.Program
    :type config: pypipes.config.Config
    :rtype: DataDogMetrics
    """
    return DataDogMetrics(config, tags={'program': program.name, 'version': program.version})


datadog_metrics_context = LazyContext(datadog_metrics_factory)
log_metrics_context = LazyContext(lambda logger=None: LogMetrics(logger))
