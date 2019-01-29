
def test_annotation(metrics):
    metrics.annotation('annotation message')
    metrics.annotation('annotation message', tags={'tag2': 'value2'})


def test_timing(metrics):
    metrics.timing('metric.name', value=10)
    metrics.timing('metric.name', value=10, tags={'tag2': 'value2'})


def test_gauge(metrics):
    metrics.gauge('metric.name', value=10)
    metrics.gauge('metric.name', value=10, tags={'tag2': 'value2'})


def test_increment(metrics):
    metrics.increment('metric.name')
    metrics.increment('metric.name', tags={'tag2': 'value2'})
    metrics.increment('metric.name', value=2, tags={'tag2': 'value2'})
