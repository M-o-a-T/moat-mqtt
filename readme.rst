MoaT-MQTT
======

``MoaT-MQTT`` is an open source `MQTT`_ client and broker implementation. It
is a fork of `hbmqtt`_ with support for `anyio`_ and `MoaT-KV`_.

MoaT-MQTT provides a straightforward API based on coroutines, making it easy
to write highly concurrent applications.

.. _anyio: https://github.com/agronholm/anyio
.. _MoaT-KV: https://github.com/M-o-a-T/moat-kv
.. _hbmqtt: https://github.com/beerfactory/hbmqtt

Features
--------

MoaT-MQTT implements the full set of `MQTT 3.1.1`_ protocol specifications and provides the following features:

- Support for QoS 0, QoS 1 and QoS 2 messages flows
- Client auto-reconnection
- Authentication via password file (more methods can be added through a plugin system)
- Basic ``$SYS`` topics
- TCP and websocket support
- SSL support over TCP and websocket
- Plugin system
- Optional: Storage of retained messages in MoaT-KV

Build status
------------

TODO.

Project status
--------------

TODO.

Getting started
---------------

MoaT-MQTT is available on `Pypi <https://pypi.python.org/pypi/moat-mqtt>`_ and can installed simply using ``pip`` :
::

    $ pip install moat-mqtt

Documentation is available on `Read the Docs`_.

Bug reports, patches and suggestions welcome! Just `open an issue`_.

.. image:: https://badges.gitter.im/Join%20Chat.svg
    :target: https://gitter.im/beerfactory/moat-mqtt?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge
    :alt: 'Join the chat at https://gitter.im/beerfactory/moat-mqtt'

.. _MQTT: http://www.mqtt.org
.. _MQTT 3.1.1: http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
.. _Read the Docs: http://moat-mqtt.readthedocs.org/
.. _open an issue: https://github.com/M-o-a-T/moat-mqtt/issues/new

Moat-MQTT? DistMQTT? MoaT-KV? Whatever?
--------------------------------------

MoaT-MQTT is a Python package that includes a stand-alone MQTT server, as
well as basic client scripts. It is based on hbmqtt and was previously
named "DistMQTT".

`MoaT-KV <https://github.com/M-o-a-T/moat-kv>`_ is a distributed key-value
storage system. It uses the MoaT-MQTT client library as its connector to
an MQTT server, preferably Mosquitto or some other low-latency broker.
It was previously named "DistKV".

A MoaT-MQTT server can hook into MoaT-KV so that some messages are persisted,
translated (i.e. store msgpack messages encoding `True` and `False`, instead of
the strings "on" and "off" (or "ON" and "OFF" or "1" and "0" or …)), filtered
(e.g. the client can only modify existing messages but not add any), et al..

