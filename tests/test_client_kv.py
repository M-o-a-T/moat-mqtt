# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import logging
import os
import random
import unittest

import anyio
import pytest
import trio

try:
    from contextlib import asynccontextmanager
except ImportError:
    from async_generator import asynccontextmanager

from moat.mqtt.broker import create_broker
from moat.mqtt.client import open_mqttclient
from moat.mqtt.mqtt.constants import QOS_0

try:
    from moat.kv.client import open_client
    from moat.kv.server import Server
except ImportError:
    pytestmark = pytest.mark.skip

from functools import partial

from . import anyio_run

formatter = "[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
logging.basicConfig(level=logging.DEBUG, format=formatter)
log = logging.getLogger(__name__)

# Port for moat-kv-based broker
PORT = 40000 + (os.getpid() + 5) % 10000
# Port for base broker
PORT_B = 40000 + (os.getpid() + 6) % 10000
# Port for moat-kv
PORT_D = 40000 + (os.getpid() + 7) % 10000

URI = f"mqtt://127.0.0.1:{PORT}/"
URI_B = f"mqtt://127.0.0.1:{PORT_B}/"

log.debug("Ports: moat_kv=%d up=%d low=%d", PORT_D, PORT, PORT_B)

broker_config = {
    "broker": {"uri": f"mqtt://127.0.0.1:{PORT_B}"},
    "kv": {
        "topic": "test_" + "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=7)),
        "base": ("test", "retain"),
        "transparent": (("test", "vis"),),
        "conn": {"port": PORT_D},
        "server": {
            "backend": "mqtt",
            "mqtt": {"uri": URI_B},
            "bind": [{"host": "localhost", "port": PORT_D, "ssl": False}],
        },
    },
    "listeners": {
        "default": {"type": "tcp", "bind": f"127.0.0.1:{PORT}", "max_connections": 10}
    },
    "sys_interval": 0,
    "auth": {"allow-anonymous": True},
}


test_config = {
    "listeners": {
        "default": {"type": "tcp", "bind": f"127.0.0.1:{PORT_B}", "max_connections": 10}
    },
    "sys_interval": 0,
    "retain": False,
    "auth": {"allow-anonymous": True},
}


@asynccontextmanager
async def moat_kv_server(n):
    msgs = []
    async with anyio.create_task_group() as tg:
        async with create_broker(test_config, plugin_namespace="moat.mqtt.test.plugins"):
            s = Server("test", cfg=broker_config["kv"], init="test")
            evt = anyio.Event()
            tg.start_soon(partial(s.serve, ready_evt=evt))
            await evt.wait()

            async with open_client(**broker_config["kv"]) as cl:

                async def msglog(evt):
                    try:
                        async with cl._stream(
                            "msg_monitor", topic="*"
                        ) as mon:  # , topic=broker_config['kv']['topic']) as mon:
                            log.info("Monitor Start")
                            evt.set()
                            async for m in mon:
                                log.info("Monitor Msg %r", m.data)
                                msgs.append(m.data)
                    except Exception:
                        log.exception("DEAD")

                evt = anyio.Event()
                await cl.scope.spawn(msglog, evt)
                await evt.wait()
                yield s
                cl.scope.cancel()
            tg.cancel_scope.cancel()
    if len(msgs) != n:
        log.error("MsgCount %d %d", len(msgs), n)
    # assert len(msgs) == n, msgs


class MQTTClientTest(unittest.TestCase):
    def test_deliver(self):
        data = b"data 123 a"

        async def test_coro():
            async with moat_kv_server(1):
                async with create_broker(broker_config, plugin_namespace="moat.mqtt.test.plugins"):
                    async with open_mqttclient(config=broker_config["broker"]) as client:
                        self.assertIsNotNone(client.session)
                        ret = await client.subscribe([("test_topic", QOS_0)])
                        self.assertEqual(ret[0], QOS_0)
                        async with open_mqttclient(config=broker_config["broker"]) as client_pub:
                            await client_pub.publish("test_topic", data, QOS_0, retain=False)
                        with anyio.fail_after(0.5):
                            message = await client.deliver_message()
                        self.assertIsNotNone(message)
                        self.assertIsNotNone(message.publish_packet)
                        self.assertEqual(message.data, data)
                        pass  # exit client
                    pass  # exit broker
                pass  # exit server
            pass  # exit test

        anyio_run(test_coro, backend="trio")

    def test_deliver_transparent(self):
        data = b"data 123 t"

        async def test_coro():
            async with moat_kv_server(1):
                async with create_broker(broker_config, plugin_namespace="moat.mqtt.test.plugins"):
                    async with open_mqttclient(config=broker_config["broker"]) as client:
                        self.assertIsNotNone(client.session)
                        ret = await client.subscribe([("test/vis/foo", QOS_0)])
                        self.assertEqual(ret[0], QOS_0)
                        async with open_mqttclient(config=broker_config["broker"]) as client_pub:
                            await client_pub.publish("test/vis/foo", data, QOS_0, retain=False)
                        with anyio.fail_after(0.5):
                            message = await client.deliver_message()
                        self.assertIsNotNone(message)
                        self.assertIsNotNone(message.publish_packet)
                        self.assertEqual(message.data, data)
                        pass  # exit client
                    pass  # exit broker
                pass  # exit server
            pass  # exit test

        anyio_run(test_coro, backend="trio")

    def test_deliver_direct(self):
        data = b"data 123 b"

        async def test_coro():
            async with moat_kv_server(0):
                async with create_broker(broker_config, plugin_namespace="moat.mqtt.test.plugins"):
                    async with open_mqttclient(config=broker_config["broker"]) as client:
                        self.assertIsNotNone(client.session)
                        ret = await client.subscribe([("test_topic", QOS_0)])
                        self.assertEqual(ret[0], QOS_0)
                        async with open_mqttclient(config=broker_config["broker"]) as client_pub:
                            await client_pub.publish("test_topic", data, QOS_0, retain=False)
                        with anyio.fail_after(0.5):
                            message = await client.deliver_message()
                        self.assertIsNotNone(message)
                        self.assertIsNotNone(message.publish_packet)
                        self.assertEqual(message.data, data)

        anyio_run(test_coro, backend="trio")

    def test_deliver_timeout(self):
        async def test_coro():
            async with moat_kv_server(0):
                async with create_broker(broker_config, plugin_namespace="moat.mqtt.test.plugins"):
                    async with open_mqttclient(config=broker_config["broker"]) as client:
                        self.assertIsNotNone(client.session)
                        ret = await client.subscribe([("test_topic", QOS_0)])
                        self.assertEqual(ret[0], QOS_0)
                        with self.assertRaises(TimeoutError):
                            with anyio.fail_after(2):
                                await client.deliver_message()

        anyio_run(test_coro, backend="trio")
