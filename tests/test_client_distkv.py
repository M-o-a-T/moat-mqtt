# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import unittest
import anyio
import trio
import os
import logging
import random
try:
    from contextlib import asynccontextmanager
except ImportError:
    from async_generator import asynccontextmanager

from hbmqtt.client import open_mqttclient, ConnectException
from hbmqtt.broker import create_broker
from hbmqtt.mqtt.constants import QOS_0, QOS_1, QOS_2
from distkv.server import Server
from distkv.client import open_client

formatter = "[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=formatter)
log = logging.getLogger(__name__)

broker_config = {
    'distkv': {
        'server': { "host": '127.0.0.1', "port": None },
        'serf': {'host':"localhost", 'port':7373},
        'topic': 'test_'+''.join(random.choices("abcdefghijklmnopqrstuvwxyz",k=7))
    },
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


@asynccontextmanager
async def distkv_server(n):
    msgs = []
    async with trio.open_nursery() as tg:
        s = Server("test", cfg={"serf":broker_config['distkv']['serf'], "server":{"host":"127.0.0.1", "port":None}}, init="test")
        await tg.start(s.serve)

        broker_config['distkv']['server']['host'] = s.ports[0][0]
        broker_config['distkv']['server']['port'] = s.ports[0][1]
        async with open_client(*s.ports[0]) as cl:
            async def serflog(task_status=trio.TASK_STATUS_IGNORED):
                async with cl.stream('serfmon', type=broker_config['distkv']['topic']) as mon:
                    log.info("Serf Start")
                    task_status.started()
                    async for m in mon:
                        log.info("Serf Msg %r",m.data)
                        msgs.append(m.data)
            await cl.tg.start(serflog)
            yield s
            cl.tg.cancel_scope.cancel()
        tg.cancel_scope.cancel()
    assert len(msgs) == n, msgs


class MQTTClientTest(unittest.TestCase):
    def test_subscribe(self):
        async def test_coro():
            async with distkv_server(0) as s:
                async with create_broker(broker_config, plugin_namespace="hbmqtt.test.plugins") as broker:
                    async with open_mqttclient() as client:
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

        anyio.run(test_coro, backend='trio')

    def test_unsubscribe(self):
        async def test_coro():
            async with distkv_server(0):
                async with create_broker(broker_config, plugin_namespace="hbmqtt.test.plugins") as broker:
                    async with open_mqttclient() as client:
                        await client.connect('mqtt://127.0.0.1/')
                        self.assertIsNotNone(client.session)
                        ret = await client.subscribe([
                            ('$SYS/broker/uptime', QOS_0),
                        ])
                        self.assertEqual(ret[0], QOS_0)
                        await client.unsubscribe(['$SYS/broker/uptime'])

        anyio.run(test_coro, backend='trio')

    def test_deliver(self):
        data = b'data'

        async def test_coro():
            async with distkv_server(1):
                async with create_broker(broker_config, plugin_namespace="hbmqtt.test.plugins") as broker:
                    async with open_mqttclient() as client:
                        await client.connect('mqtt://127.0.0.1/')
                        self.assertIsNotNone(client.session)
                        ret = await client.subscribe([
                            ('test_topic', QOS_0),
                        ])
                        self.assertEqual(ret[0], QOS_0)
                        async with open_mqttclient() as client_pub:
                            await client_pub.connect('mqtt://127.0.0.1/')
                            await client_pub.publish('test_topic', data, QOS_0)
                        message = await client.deliver_message()
                        self.assertIsNotNone(message)
                        self.assertIsNotNone(message.publish_packet)
                        self.assertEqual(message.data, data)
                        await client.unsubscribe(['$SYS/broker/uptime'])

        anyio.run(test_coro, backend='trio')

    def test_deliver_timeout(self):
        async def test_coro():
            async with distkv_server(0):
                async with create_broker(broker_config, plugin_namespace="hbmqtt.test.plugins") as broker:
                    async with open_mqttclient() as client:
                        await client.connect('mqtt://127.0.0.1/')
                        self.assertIsNotNone(client.session)
                        ret = await client.subscribe([
                            ('test_topic', QOS_0),
                        ])
                        self.assertEqual(ret[0], QOS_0)
                        with self.assertRaises(TimeoutError):
                            await client.deliver_message(timeout=2)
                        await client.unsubscribe(['$SYS/broker/uptime'])

        anyio.run(test_coro, backend='trio')
