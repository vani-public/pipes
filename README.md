# pypipes

[![Build Status](https://cloud.drone.io/api/badges/vani-public/pipes/status.svg)](https://cloud.drone.io/vani-public/pipes)
[![Version](https://img.shields.io/pypi/v/pypipes.svg)](https://pypi.python.org/pypi/pypipes)
[![Supported Versions](https://img.shields.io/pypi/pyversions/pypipes.svg)](https://pypi.python.org/pypi/pypipes)

Infrastructure independent task execution framework based on logical task pipelines.


## Examples
You can find many examples of common framework use-cases inside the `./examples` folder.
It's recommended to start a review from consolidated examples:
- `./examples/example_inline.py` - pipeline program sample on inline executing infrastructure.
- `./examples/example_gevent.py` - pipeline program sample on gevent infrastructure.
- `./examples/example_celery.py` - pipeline program sample on celery infrastructure.
