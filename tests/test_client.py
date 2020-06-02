# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import unittest
import anyio
import os
import logging
from distmqtt.client import open_mqttclient, ConnectException
from distmqtt.broker import create_broker
from distmqtt.mqtt.constants import QOS_0, QOS_1, QOS_2

formatter = (
    "[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
)
logging.basicConfig(level=logging.ERROR, format=formatter)
log = logging.getLogger(__name__)

PORT = 40000 + (os.getpid() + 4) % 10000
URI = "mqtt://127.0.0.1:%d/" % PORT

broker_config = {
    "listeners": {
        "default": {
            "type": "tcp",
            "bind": "127.0.0.1:%d" % PORT,
            "max_connections": 10,
        },
        "ws": {"type": "ws", "bind": "127.0.0.1:8080", "max_connections": 10},
        "wss": {"type": "ws", "bind": "127.0.0.1:8081", "max_connections": 10},
    },
    "sys_interval": 0,
    "auth": {"allow-anonymous": True},
}


class MQTTClientTest(unittest.TestCase):
    def test_connect_tcp(self):
        async def test_coro():
            async with open_mqttclient() as client:
                await client.connect("mqtt://test.mosquitto.org/")
                self.assertIsNotNone(client.session)

        try:
            anyio.run(test_coro)
        except ConnectException:
            log.error("Broken by server")

    def test_connect_tcp_secure(self):
        async def test_coro():
            async with open_mqttclient(config={"check_hostname": False}) as client:
                ca = os.path.join(
                    os.path.dirname(os.path.realpath(__file__)), "mosquitto.org.crt"
                )
                await client.connect("mqtts://test.mosquitto.org/", cafile=ca)
                self.assertIsNotNone(client.session)

        try:
            anyio.run(test_coro)
        except ConnectException:
            log.error("Broken by server")

    def test_connect_tcp_failure(self):
        async def test_coro():
            try:
                config = {"auto_reconnect": False}
                async with open_mqttclient(config=config) as client:
                    await client.connect(URI)
            except ConnectException:
                pass
            else:
                raise RuntimeError("should not be able to connect")

        anyio.run(test_coro)

    def test_connect_ws(self):
        async def test_coro():
            async with create_broker(
                broker_config, plugin_namespace="distmqtt.test.plugins"
            ):
                async with open_mqttclient() as client:
                    await client.connect("ws://127.0.0.1:8080/")
                    self.assertIsNotNone(client.session)

        anyio.run(test_coro, backend="trio")

    def test_reconnect_ws_retain_username_password(self):
        async def test_coro():
            async with create_broker(
                broker_config, plugin_namespace="distmqtt.test.plugins"
            ):
                async with open_mqttclient() as client:
                    await client.connect("ws://fred:password@127.0.0.1:8080/")
                    self.assertIsNotNone(client.session)
                    await client.reconnect()

                    self.assertIsNotNone(client.session.username)
                    self.assertIsNotNone(client.session.password)

        anyio.run(test_coro, backend="trio")

    def test_connect_ws_secure(self):
        async def test_coro():
            async with create_broker(
                broker_config, plugin_namespace="distmqtt.test.plugins"
            ):
                async with open_mqttclient() as client:
                    ca = os.path.join(
                        os.path.dirname(os.path.realpath(__file__)), "mosquitto.org.crt"
                    )
                    await client.connect("ws://127.0.0.1:8081/", cafile=ca)
                    self.assertIsNotNone(client.session)

        anyio.run(test_coro, backend="trio")

    def test_ping(self):
        async def test_coro():
            async with create_broker(
                broker_config, plugin_namespace="distmqtt.test.plugins"
            ):
                async with open_mqttclient() as client:
                    await client.connect(URI)
                    self.assertIsNotNone(client.session)
                    await client.ping()

        anyio.run(test_coro, backend="trio")

    def test_subscribe(self):
        async def test_coro():
            async with create_broker(
                broker_config, plugin_namespace="distmqtt.test.plugins"
            ):
                async with open_mqttclient() as client:
                    await client.connect(URI)
                    self.assertIsNotNone(client.session)
                    ret = await client.subscribe(
                        [
                            ("$SYS/broker/uptime", QOS_0),
                            ("$SYS/broker/uptime", QOS_1),
                            ("$SYS/broker/uptime", QOS_2),
                        ]
                    )
                    self.assertEqual(ret[0], QOS_0)
                    self.assertEqual(ret[1], QOS_1)
                    self.assertEqual(ret[2], QOS_2)

        anyio.run(test_coro, backend="trio")

    def test_unsubscribe(self):
        async def test_coro():
            async with create_broker(
                broker_config, plugin_namespace="distmqtt.test.plugins"
            ):
                async with open_mqttclient() as client:
                    await client.connect(URI)
                    self.assertIsNotNone(client.session)
                    ret = await client.subscribe([("$SYS/broker/uptime", QOS_0)])
                    self.assertEqual(ret[0], QOS_0)
                    await client.unsubscribe(["$SYS/broker/uptime"])

        anyio.run(test_coro, backend="trio")

    def test_deliver(self):
        data = b"data"

        async def test_coro():
            async with create_broker(
                broker_config, plugin_namespace="distmqtt.test.plugins"
            ):
                async with open_mqttclient() as client:
                    await client.connect(URI)
                    self.assertIsNotNone(client.session)
                    ret = await client.subscribe([("test_topic", QOS_0)])
                    self.assertEqual(ret[0], QOS_0)
                    async with open_mqttclient() as client_pub:
                        await client_pub.connect(URI)
                        await client_pub.publish("test_topic", data, QOS_0)
                    message = await client.deliver_message()
                    self.assertIsNotNone(message)
                    self.assertIsNotNone(message.publish_packet)
                    self.assertEqual(message.data, data)
                    await client.unsubscribe(["$SYS/broker/uptime"])

        anyio.run(test_coro, backend="trio")

    def test_deliver_timeout(self):
        async def test_coro():
            async with create_broker(
                broker_config, plugin_namespace="distmqtt.test.plugins"
            ):
                async with open_mqttclient() as client:
                    await client.connect(URI)
                    self.assertIsNotNone(client.session)
                    ret = await client.subscribe([("test_topic", QOS_0)])
                    self.assertEqual(ret[0], QOS_0)
                    with self.assertRaises(TimeoutError):
                        async with anyio.fail_after(2):
                            await client.deliver_message()
                    await client.unsubscribe(["$SYS/broker/uptime"])

        anyio.run(test_coro, backend="trio")
