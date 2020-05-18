Quickstart
==========

A quick way for getting started with ``DistMQTT`` is to use console scripts provided for :

* publishing a message on some topic on an external MQTT broker.
* subscribing some topics and getting published messages.
* running an autonomous MQTT broker


Installation
++++++++++++

That's easy::

  (venv) $ pip install distmqtt


Sample code
+++++++++++

As ``DistMQTT`` is async Python, you need to wrap all examples with::

   async def main():
      [ actual sample code here ]
   anyio.run(main)

The easiest way to do this is to use the ``asyncclick`` package::

   import asyncclick as click
   @click.command()
   @click.option('-t','--test',is_flag=True, help="Set Me")
   async def main(test):
      if not test:
         raise click.UsageError("I told you to set me")  # :-)
      [ actual sample code here ]

   main()  # click.command() wraps that in a call to anyio.run()


Connecting to a broker
----------------------

An MQTT connection is typically used as a context manager::

   async with open_mqttclient(uri='mqtt://localhost:1883', codec='utf8') as C:
      await some_mqtt_commands()

Sending messages
----------------

That's easy::

   async with open_mqttclient(…) as C:
      async C.publish("one/two/three/four", [1,2,3,4], codec="msgpack")

Receiving messages
------------------

Receiving uses another context manager::

   async with open_mqttclient(…) as C:
      async with C.subscription("one/two/#", codec="msgpack") as S:
         async for msg in S:
            print("I got",msg)

The subscription affords a ``publish`` method which inherits its codec
and QoS settings.

If you want to process multiple subscriptions in parallel, the easiest way
is to use multiple tasks.

Console scripts
+++++++++++++++

A quick way for getting started with ``DistMQTT`` is to examine the code in
``DistMQTT``'s console scripts.

These scripts are installed automatically when installing ``DistMQTT``.

Publishing messages
-------------------

``distmqtt_pub`` is a command-line tool which can be used for publishing some messages on a topic. It requires a few arguments like broker URL, topic name, QoS and data to send. Additional options allow more complex use case.

Publishing ```some_data`` to a ``/test`` topic on is as simple as :
::

    $ distmqtt_pub --url mqtt://test.mosquitto.org -t /test -m some_data
    [2015-11-06 22:21:55,108] :: INFO - distmqtt_pub/5135-MacBook-Pro.local Connecting to broker
    [2015-11-06 22:21:55,333] :: INFO - distmqtt_pub/5135-MacBook-Pro.local Publishing to '/test'
    [2015-11-06 22:21:55,336] :: INFO - distmqtt_pub/5135-MacBook-Pro.local Disconnected from broker

This will use insecure TCP connection to connect to test.mosquitto.org. ``distmqtt_pub`` also allows websockets and secure connection:
::

    $ distmqtt_pub --url ws://test.mosquitto.org:8080 -t /test -m some_data
    [2015-11-06 22:22:42,542] :: INFO - distmqtt_pub/5157-MacBook-Pro.local Connecting to broker
    [2015-11-06 22:22:42,924] :: INFO - distmqtt_pub/5157-MacBook-Pro.local Publishing to '/test'
    [2015-11-06 22:22:52,926] :: INFO - distmqtt_pub/5157-MacBook-Pro.local Disconnected from broker

``distmqtt_pub`` can read from file or stdin and use data read as message payload:
::

    $ some_command | distmqtt_pub --url mqtt://localhost -t /test -l

See :doc:`references/distmqtt_pub` reference documentation for details about available options and settings.

Subscribing a topic
-------------------

``distmqtt_sub`` is a command-line tool which can be used to subscribe for some pattern(s) on a broker and get date from messages published on topics matching these patterns by other MQTT clients.

Subscribing a ``/test/#`` topic pattern is done with :
::

  $ distmqtt_sub --url mqtt://localhost -t /test/#

This command will run forever and print on the standard output every messages received from the broker. The ``-n`` option allows to set a maximum number of messages to receive before stopping.

See :doc:`references/distmqtt_sub` reference documentation for details about available options and settings.


URL Scheme
----------

DistMQTT command line tools use the ``--url`` to establish a network connection with the broker. The ``--url`` parameter value must conform to the `MQTT URL scheme`_. The general accepted form is :
::

    {mqtt,ws}[s]://[username][:password]@host.domain[:port]

Here are some examples of valid URLs:
::

    mqtt://localhost
    mqtt://localhost:1884
    mqtt://user:password@localhost
    ws://test.mosquitto.org
    wss://user:password@localhost

.. _MQTT URL scheme: https://github.com/mqtt/mqtt.github.io/wiki/URI-Scheme


Running a broker
----------------

``distmqtt`` is a command-line tool for running a MQTT broker:
::

    $ distmqtt
    [2015-11-06 22:45:16,470] :: INFO - Listener 'default' bind to 0.0.0.0:1883 (max_connections=-1)

See :doc:`references/distmqtt` reference documentation for details about available options and settings.
