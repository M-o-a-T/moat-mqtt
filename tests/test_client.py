# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import unittest
import asyncio,anyio
import os
import logging
from hbmqtt.client import MQTTClient, ConnectException
from hbmqtt.broker import Broker
from hbmqtt.mqtt.constants import QOS_0, QOS_1, QOS_2

formatter = "[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
logging.basicConfig(level=logging.ERROR, format=formatter)
log = logging.getLogger(__name__)

broker_config = {
    'listeners': {
        'default': {
            'type': 'tcp',
            'bind': '127.0.0.1:1883',
            'max_connections': 10
        },
        'ws': {
            'type': 'ws',
            'bind': '127.0.0.1:8080',
            'max_connections': 10
        },
        'wss': {
            'type': 'ws',
            'bind': '127.0.0.1:8081',
            'max_connections': 10
        },
    },
    'sys_interval': 0,
    'auth': {
        'allow-anonymous': True,
    }
}


class MQTTClientTest(unittest.TestCase):
    def test_connect_tcp(self):
        async def test_coro():
            client = MQTTClient()
            await client.connect('mqtt://test.mosquitto.org/')
            self.assertIsNotNone(client.session)
            await client.disconnect()

        anyio.run(test_coro)

    def test_connect_tcp_secure(self):
        async def test_coro():
            client = MQTTClient(config={'check_hostname': False})
            ca = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mosquitto.org.crt')
            await client.connect('mqtts://test.mosquitto.org/', cafile=ca)
            self.assertIsNotNone(client.session)
            await client.disconnect()

        anyio.run(test_coro)

    def test_connect_tcp_failure(self):
        async def test_coro():
            try:
                config = {'auto_reconnect': False}
                client = MQTTClient(config=config)
                await client.connect('mqtt://127.0.0.1/')
            except ConnectException as e:
                pass
            else:
                raise RuntimeError("should not be able to connect")

        anyio.run(test_coro)

    def test_connect_ws(self):
        async def test_coro():
            broker = Broker(broker_config, plugin_namespace="hbmqtt.test.plugins")
            await broker.start()
            client = MQTTClient()
            await client.connect('ws://127.0.0.1:8080/')
            self.assertIsNotNone(client.session)
            await client.disconnect()
            await broker.shutdown()

        anyio.run(test_coro)

    def test_reconnect_ws_retain_username_password(self):
        async def test_coro():
            broker = Broker(broker_config, plugin_namespace="hbmqtt.test.plugins")
            await broker.start()
            client = MQTTClient()
            await client.connect('ws://fred:password@127.0.0.1:8080/')
            self.assertIsNotNone(client.session)
            await client.disconnect()
            await client.reconnect()

            self.assertIsNotNone(client.session.username)
            self.assertIsNotNone(client.session.password)
            await broker.shutdown()

        anyio.run(test_coro)

    def test_connect_ws_secure(self):
        async def test_coro():
            broker = Broker(broker_config, plugin_namespace="hbmqtt.test.plugins")
            await broker.start()
            client = MQTTClient()
            ca = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mosquitto.org.crt')
            await client.connect('ws://127.0.0.1:8081/', cafile=ca)
            self.assertIsNotNone(client.session)
            await client.disconnect()
            await broker.shutdown()

        anyio.run(test_coro)

    def test_ping(self):
        async def test_coro():
            broker = Broker(broker_config, plugin_namespace="hbmqtt.test.plugins")
            await broker.start()
            client = MQTTClient()
            await client.connect('mqtt://127.0.0.1/')
            self.assertIsNotNone(client.session)
            await client.ping()
            await client.disconnect()
            await broker.shutdown()

        anyio.run(test_coro)

    def test_subscribe(self):
        async def test_coro():
            broker = Broker(broker_config, plugin_namespace="hbmqtt.test.plugins")
            await broker.start()
            client = MQTTClient()
            await client.connect('mqtt://127.0.0.1/')
            self.assertIsNotNone(client.session)
            ret = await client.subscribe([
                ('$SYS/broker/uptime', QOS_0),
                ('$SYS/broker/uptime', QOS_1),
                ('$SYS/broker/uptime', QOS_2),
            ])
            self.assertEqual(ret[0], QOS_0)
            self.assertEqual(ret[1], QOS_1)
            self.assertEqual(ret[2], QOS_2)
            await client.disconnect()
            await broker.shutdown()

        anyio.run(test_coro)

    def test_unsubscribe(self):
        async def test_coro():
            broker = Broker(broker_config, plugin_namespace="hbmqtt.test.plugins")
            await broker.start()
            client = MQTTClient()
            await client.connect('mqtt://127.0.0.1/')
            self.assertIsNotNone(client.session)
            ret = await client.subscribe([
                ('$SYS/broker/uptime', QOS_0),
            ])
            self.assertEqual(ret[0], QOS_0)
            await client.unsubscribe(['$SYS/broker/uptime'])
            await client.disconnect()
            await broker.shutdown()

        anyio.run(test_coro)

    def test_deliver(self):
        data = b'data'

        async def test_coro():
            broker = Broker(broker_config, plugin_namespace="hbmqtt.test.plugins")
            await broker.start()
            client = MQTTClient()
            await client.connect('mqtt://127.0.0.1/')
            self.assertIsNotNone(client.session)
            ret = await client.subscribe([
                ('test_topic', QOS_0),
            ])
            self.assertEqual(ret[0], QOS_0)
            client_pub = MQTTClient()
            await client_pub.connect('mqtt://127.0.0.1/')
            await client_pub.publish('test_topic', data, QOS_0)
            await client_pub.disconnect()
            message = await client.deliver_message()
            self.assertIsNotNone(message)
            self.assertIsNotNone(message.publish_packet)
            self.assertEqual(message.data, data)
            await client.unsubscribe(['$SYS/broker/uptime'])
            await client.disconnect()
            await broker.shutdown()

        anyio.run(test_coro)

    def test_deliver_timeout(self):
        async def test_coro():
            broker = Broker(broker_config, plugin_namespace="hbmqtt.test.plugins")
            await broker.start()
            client = MQTTClient()
            await client.connect('mqtt://127.0.0.1/')
            self.assertIsNotNone(client.session)
            ret = await client.subscribe([
                ('test_topic', QOS_0),
            ])
            self.assertEqual(ret[0], QOS_0)
            with self.assertRaises(asyncio.TimeoutError):
                await client.deliver_message(timeout=2)
            await client.unsubscribe(['$SYS/broker/uptime'])
            await client.disconnect()
            await broker.shutdown()

        anyio.run(test_coro)
