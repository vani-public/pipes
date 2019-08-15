========
Overview
========

.. start-badges

.. list-table::
    :stub-columns: 1

    * - tests
      - | |drone|
    * - package
      - | |version|

.. |build| image:: https://cloud.drone.io/api/badges/vani-public/pipes/status.svg
    :alt: Drone-CI Build Status
    :target: https://cloud.drone.io/vani-public/pipes

.. |version| image:: https://img.shields.io/pypi/v/pypipes.svg
    :alt: PyPI Package latest release
    :target: https://pypi.python.org/pypi/pypipes

.. end-badges

What?
~~~~~

**pypipes** is an infrastructure independent task execution framework based on logical task pipelines.

Examples
~~~~~~~~

You can find many examples of common framework use-cases inside the ``./examples`` folder.
It's recommended to start a review from consolidated examples:
- ``./examples/example_inline.py`` - pipeline program sample on inline executing infrastructure.
- ``./examples/example_gevent.py`` - pipeline program sample on gevent infrastructure.
- ``./examples/example_celery.py`` - pipeline program sample on celery infrastructure.
