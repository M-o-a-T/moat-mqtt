MQTTClient API
==============

The :class:`~distmqtt.client.MQTTClient` class implements the client part of MQTT protocol. It can be used to publish and/or subscribe MQTT message on a broker accessible on the network through TCP or websocket protocol, both secured or unsecured.


Usage examples
--------------

Subscriber
..........

The example below shows how to write a simple MQTT client which subscribes a topic and prints every messages received from the broker :

.. code-block:: python

    import logging
    import anyio

    from distmqtt.client import open_mqttclient, ClientException
    from distmqtt.mqtt.constants import QOS_1, QOS_2
    
    logger = logging.getLogger(__name__)

    async def uptime_coro():
        async with open_mqttclient(uri='mqtt://test.mosquitto.org/') as C:
            # Subscribe to '$SYS/broker/uptime' with QOS=1
            # Subscribe to '$SYS/broker/load/#' with QOS=2
            await C.subscribe([
                    ('$SYS/broker/uptime', QOS_1),
                    ('$SYS/broker/load/#', QOS_2),
                 ])
            for i in range(1, 100):
                message = await C.deliver_message()
                packet = message.publish_packet
                print("%d:  %s => %s" % (i, packet.variable_header.topic_name, str(packet.payload.data)))
            await C.unsubscribe(['$SYS/broker/uptime', '$SYS/broker/load/#'])

    if __name__ == '__main__':
        formatter = "[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
        logging.basicConfig(level=logging.DEBUG, format=formatter)
        anyio.run(uptime_coro)

This code has a problem: there's one central dispatcher which needs to know
all message types. Fortunately `distmqtt` has a built-in dispatcher.

.. code-block:: python

    async def show(C, topic, qos):
        async with C.subscription(topic, qos) as sub:
            count = 0
            async for message in sub:
                packet = message.publish_packet
                print("%d:  %s => %s" % (i, packet.variable_header.topic_name, str(packet.payload.data)))
                count += 1
                if count >= 100:
                break

    async def uptime_coro():
        async with open_mqttclient(uri='mqtt://test.mosquitto.org/') as C:
            # Subscribe to '$SYS/broker/uptime' with QOS=1
            # Subscribe to '$SYS/broker/load/#' with QOS=2
            async with anyio.create_task_group() as tg:
               await tg.spawn(show, C, '$SYS/broker/uptime', QOS_1);
               await tg.spawn(show, C, '$SYS/broker/load/#', QOS_2);

    if __name__ == '__main__':
        formatter = "[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
        logging.basicConfig(level=logging.DEBUG, format=formatter)
        anyio.run(uptime_coro)


Publisher
.........

The example below uses the :class:`~distmqtt.client.MQTTClient` class to implement a publisher.
This test publish 3 messages asynchronously to the broker on a test topic.
For the purposes of the test, each message is published with a different Quality Of Service.

.. code-block:: python

    import logging
    import anyio

    from distmqtt.client import MQTTClient
    from distmqtt.mqtt.constants import QOS_0, QOS_1, QOS_2

    logger = logging.getLogger(__name__)
    
    async def test_coro():
        """Publish in parallel"""
        async with open_mqttclient(uri='mqtt://test.mosquitto.org/') as C:
            async with anyio.create_task_group() as tg:
                await tg.spawn(C.publish,'a/b', b'TEST MESSAGE WITH QOS_0')
                await tg.spawn(C.publish,'a/b', b'TEST MESSAGE WITH QOS_1', qos=QOS_1)),
                await tg.spawn(C.publish,'a/b', b'TEST MESSAGE WITH QOS_2', qos=QOS_2)),
            logger.info("messages published")


    async def test_coro2():
        """Publish sequentially"""
        try:
            async with open_mqttclient(uri='mqtt://test.mosquitto.org/') as C:
               await C.publish('a/b', b'TEST MESSAGE WITH QOS_0', qos=QOS_0)
               await C.publish('a/b', b'TEST MESSAGE WITH QOS_1', qos=QOS_1)
               await C.publish('a/b', b'TEST MESSAGE WITH QOS_2', qos=QOS_2)
               logger.info("messages published")
        except ConnectException as ce:
            logger.error("Connection failed: %s", ce)


    if __name__ == '__main__':
        formatter = "[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
        logging.basicConfig(level=logging.DEBUG, format=formatter)
        anyio.run(test_coro)
        anyio.run(test_coro2)

Both coroutines have the same results except that ``test_coro()`` sends its
messages in parallel, and thus is probably a bit faster.


Reference
---------

MQTTClient API
..............

.. autofunction:: distmqtt.client.open_mqttclient

.. automodule:: distmqtt.client

    .. autoclass:: MQTTClient

        .. automethod:: connect
        .. automethod:: disconnect
        .. automethod:: reconnect
        .. automethod:: ping
        .. automethod:: publish
        .. automethod:: subscribe
        .. automethod:: unsubscribe
        .. automethod:: deliver_message
        .. automethod:: subscription


MQTTClient configuration
........................

Typically, you create a :class:`~distmqtt.client.MQTTClient` instance with an async context manager, i.e. by way of ``async with`` :func:`~distmqtt.client.open_mqttclient`\(). This context manager creates a taskgroup for the client's housekeeping tasks to run in.

:func:`~distmqtt.client.open_mqttclient` accepts a ``config`` parameter which allows to setup some behaviour and defaults settings. This argument must be a Python dictionary which may contain the following entries:

* ``keep_alive``: keep alive interval (in seconds) to send when connecting to the broker (defaults to ``10`` seconds). :class:`~distmqtt.client.MQTTClient` will *auto-ping* the broker if no message is sent within the keep-alive interval. This avoids disconnection from the broker.
* ``ping_delay``: *auto-ping* delay before keep-alive times out (defaults to ``1`` seconds). This should be larger than twice the worst-case roundtrip between your client and the broker.
* ``default_qos``: Default QoS (``0``) used by :meth:`~distmqtt.client.MQTTClient.publish` if ``qos`` argument is not given.
* ``default_retain``: Default retain (``False``) used by :meth:`~distmqtt.client.MQTTClient.publish` if ``retain`` argument is not given.
* ``auto_reconnect``: enable or disable auto-reconnect feature (defaults to ``True``).
* ``reconnect_max_interval``: maximum interval (in seconds) to wait before two connection retries (defaults to ``10``).
* ``reconnect_retries``: maximum number of connect retries (defaults to ``2``). Negative value will cause client to reconnect infinietly.
* ``codec``: the codec to use by default. May be overridden.
* ``codec_params``: Config values to use with a particular codec. Indexed by codec name.

Default QoS and default retain can also be overriden by adding a ``topics`` entry with may contain QoS and retain values for specific topics. See the following example:

.. code-block:: python

    config = {
        'keep_alive': 10,
        'ping_delay': 1,
        'default_qos': 0,
        'default_retain': False,
        'auto_reconnect': True,
        'reconnect_max_interval': 5,
        'reconnect_retries': 10,
        'codec': 'utf8',
        'codec_params': {
            'bool': {on='on',off='off'}, ## default, actually
            'BOOL': {on='ON',off='OFF',name='bool'}
            'yesno': {on='yes',off='no', name='bool'}
        },
        'topics': {
            '/test': { 'qos': 1 },
            '/some_topic': { 'qos': 2, 'retain': True }
        }
    }

With this setting any message published will set with QOS_0 and retain flag unset except for

* messages sent to ``/test`` topic will be sent with QOS_1
* messages sent to ``/some_topic`` topic will be sent with QOS_2 and retained

Also, 'codec="yesno"' will only accept a ``bool`` as message, and translate
that to "yes" and "no" messages.

In any case, any ``qos`` and ``retain`` arguments passed to method :meth:`~distmqtt.client.MQTTClient.publish` will override these settings.
