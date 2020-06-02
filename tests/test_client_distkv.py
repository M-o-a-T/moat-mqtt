# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import unittest
import pytest
import anyio
import trio
import os
import logging
import random

try:
    from contextlib import asynccontextmanager
except ImportError:
    from async_generator import asynccontextmanager

from distmqtt.client import open_mqttclient
from distmqtt.broker import create_broker
from distmqtt.mqtt.constants import QOS_0

try:
    from distkv.server import Server
    from distkv.client import open_client
except ImportError:
    pytestmark = pytest.mark.skip

from functools import partial

formatter = (
    "[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
)
logging.basicConfig(level=logging.DEBUG, format=formatter)
log = logging.getLogger(__name__)

# Port for distkv-based broker
PORT = 40000 + (os.getpid() + 5) % 10000
# Port for base broker
PORT_B = 40000 + (os.getpid() + 6) % 10000
# Port for distkv
PORT_D = 40000 + (os.getpid() + 7) % 10000

URI = "mqtt://127.0.0.1:%d/" % PORT
URI_B = "mqtt://127.0.0.1:%d/" % PORT_B

log.debug("Ports: distkv=%d up=%d low=%d", PORT_D, PORT, PORT_B)

broker_config = {
    "broker": {"uri": "mqtt://127.0.0.1:%d" % PORT_B},
    "distkv": {
        "topic": "test_" + "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=7)),
        "base": ("test", "retain"),
        "transparent": (("test", "vis"),),
        "connect": {"port": PORT_D},
        "server": {
            "backend": "mqtt",
            "mqtt": {"uri": URI_B},
            "bind": [{"host": "localhost", "port": PORT_D, "ssl": False}],
        },
    },
    "listeners": {
        "default": {
            "type": "tcp",
            "bind": "127.0.0.1:%d" % (PORT),
            "max_connections": 10,
        }
    },
    "sys_interval": 0,
    "auth": {"allow-anonymous": True},
}


test_config = {
    "listeners": {
        "default": {
            "type": "tcp",
            "bind": "127.0.0.1:%d" % PORT_B,
            "max_connections": 10,
        }
    },
    "sys_interval": 0,
    "retain": False,
    "auth": {"allow-anonymous": True},
}


@asynccontextmanager
async def distkv_server(n):
    msgs = []
    async with anyio.create_task_group() as tg:
        async with create_broker(test_config, plugin_namespace="distmqtt.test.plugins"):
            s = Server("test", cfg=broker_config["distkv"], init="test")
            evt = anyio.create_event()
            await tg.spawn(partial(s.serve, ready_evt=evt))
            await evt.wait()

            async with open_client(**broker_config["distkv"]) as cl:

                async def msglog(task_status=trio.TASK_STATUS_IGNORED):
                    async with cl._stream(
                        "msg_monitor", topic="*"
                    ) as mon:  # , topic=broker_config['distkv']['topic']) as mon:
                        log.info("Monitor Start")
                        task_status.started()
                        async for m in mon:
                            log.info("Monitor Msg %r", m.data)
                            msgs.append(m.data)

                await cl.tg.spawn(msglog)
                yield s
                await cl.tg.cancel_scope.cancel()
            await tg.cancel_scope.cancel()
    if len(msgs) != n:
        log.error("MsgCount %d %d", len(msgs), n)
    # assert len(msgs) == n, msgs


class MQTTClientTest(unittest.TestCase):
    def test_deliver(self):
        data = b"data 123 a"

        async def test_coro():
            async with distkv_server(1):
                async with create_broker(
                    broker_config, plugin_namespace="distmqtt.test.plugins"
                ):
                    async with open_mqttclient(
                        config=broker_config["broker"]
                    ) as client:
                        self.assertIsNotNone(client.session)
                        ret = await client.subscribe([("test_topic", QOS_0)])
                        self.assertEqual(ret[0], QOS_0)
                        async with open_mqttclient(
                            config=broker_config["broker"]
                        ) as client_pub:
                            await client_pub.publish(
                                "test_topic", data, QOS_0, retain=False
                            )
                        async with anyio.fail_after(0.5):
                            message = await client.deliver_message()
                        self.assertIsNotNone(message)
                        self.assertIsNotNone(message.publish_packet)
                        self.assertEqual(message.data, data)
                        pass  # exit client
                    pass  # exit broker
                pass  # exit server
            pass  # exit test

        anyio.run(test_coro, backend="trio")

    def test_deliver_transparent(self):
        data = b"data 123 t"

        async def test_coro():
            async with distkv_server(1):
                async with create_broker(
                    broker_config, plugin_namespace="distmqtt.test.plugins"
                ):
                    async with open_mqttclient(
                        config=broker_config["broker"]
                    ) as client:
                        self.assertIsNotNone(client.session)
                        ret = await client.subscribe([("test/vis/foo", QOS_0)])
                        self.assertEqual(ret[0], QOS_0)
                        async with open_mqttclient(
                            config=broker_config["broker"]
                        ) as client_pub:
                            await client_pub.publish(
                                "test/vis/foo", data, QOS_0, retain=False
                            )
                        async with anyio.fail_after(0.5):
                            message = await client.deliver_message()
                        self.assertIsNotNone(message)
                        self.assertIsNotNone(message.publish_packet)
                        self.assertEqual(message.data, data)
                        pass  # exit client
                    pass  # exit broker
                pass  # exit server
            pass  # exit test

        anyio.run(test_coro, backend="trio")

    def test_deliver_direct(self):
        data = b"data 123 b"

        async def test_coro():
            async with distkv_server(0):
                async with create_broker(
                    broker_config, plugin_namespace="distmqtt.test.plugins"
                ):
                    async with open_mqttclient(
                        config=broker_config["broker"]
                    ) as client:
                        self.assertIsNotNone(client.session)
                        ret = await client.subscribe([("test_topic", QOS_0)])
                        self.assertEqual(ret[0], QOS_0)
                        async with open_mqttclient(
                            config=broker_config["broker"]
                        ) as client_pub:
                            await client_pub.publish(
                                "test_topic", data, QOS_0, retain=False
                            )
                        async with anyio.fail_after(0.5):
                            message = await client.deliver_message()
                        self.assertIsNotNone(message)
                        self.assertIsNotNone(message.publish_packet)
                        self.assertEqual(message.data, data)

        anyio.run(test_coro, backend="trio")

    def test_deliver_timeout(self):
        async def test_coro():
            async with distkv_server(0):
                async with create_broker(
                    broker_config, plugin_namespace="distmqtt.test.plugins"
                ):
                    async with open_mqttclient(
                        config=broker_config["broker"]
                    ) as client:
                        self.assertIsNotNone(client.session)
                        ret = await client.subscribe([("test_topic", QOS_0)])
                        self.assertEqual(ret[0], QOS_0)
                        with self.assertRaises(TimeoutError):
                            async with anyio.fail_after(2):
                                await client.deliver_message()

        anyio.run(test_coro, backend="trio")
