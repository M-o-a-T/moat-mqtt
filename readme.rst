DistMQTT
======

``DistMQTT`` is an open source `MQTT`_ client and broker implementation.

Built on top of `asyncio`_, Python's standard asynchronous I/O framework, DistMQTT provides a straightforward API
based on coroutines, making it easy to write highly concurrent applications.

.. _asyncio: https://docs.python.org/3/library/asyncio.html

Features
--------

DistMQTT implements the full set of `MQTT 3.1.1`_ protocol specifications and provides the following features:

- Support QoS 0, QoS 1 and QoS 2 messages flow
- Client auto-reconnection on network lost
- Authentication through password file (more methods can be added through a plugin system)
- Basic ``$SYS`` topics
- TCP and websocket support
- SSL support over TCP and websocket
- Plugin system

Build status
------------

.. image:: https://travis-ci.org/beerfactory/distmqtt.svg?branch=master
    :target: https://travis-ci.org/beerfactory/distmqtt

.. image:: https://coveralls.io/repos/beerfactory/distmqtt/badge.svg?branch=master&service=github
    :target: https://coveralls.io/github/beerfactory/distmqtt?branch=master

Project status
--------------

.. image:: https://readthedocs.org/projects/distmqtt/badge/?version=latest
    :target: http://distmqtt.readthedocs.org/en/latest/?badge=latest
    :alt: Documentation Status

.. image:: https://badge.fury.io/py/distmqtt.svg
    :target: https://badge.fury.io/py/distmqtt

Getting started
---------------

distmqtt is available on `Pypi <https://pypi.python.org/pypi/distmqtt>`_ and can installed simply using ``pip`` :
::

    $ pip install distmqtt

Documentation is available on `Read the Docs`_.

Bug reports, patches and suggestions welcome! Just `open an issue`_ or join the `gitter channel`_.

.. image:: https://badges.gitter.im/Join%20Chat.svg
    :target: https://gitter.im/beerfactory/distmqtt?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge
    :alt: 'Join the chat at https://gitter.im/beerfactory/distmqtt'

.. _MQTT: http://www.mqtt.org
.. _MQTT 3.1.1: http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
.. _Read the Docs: http://distmqtt.readthedocs.org/
.. _open an issue: https://github.com/beerfactory/distmqtt/issues/new
.. _gitter channel: https://gitter.im/beerfactory/distmqtt
