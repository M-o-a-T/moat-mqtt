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

from distmqtt.client import open_mqttclient, ConnectException
from distmqtt.broker import create_broker
from distmqtt.mqtt.constants import QOS_0, QOS_1, QOS_2
from distkv.server import Server
from distkv.client import open_client
from functools import partial

formatter = "[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
logging.basicConfig(level=logging.DEBUG, format=formatter)
log = logging.getLogger(__name__)

PORT=40000+os.getpid()%10000

broker_config = {
    'broker': { 'uri':'mqtt://127.0.0.1:%d'%(PORT+1), },
    'distkv': {
        'server': { "host": '127.0.0.1', "port": None },
        'serf': {'host':"localhost", 'port':7373},
        'topic': 'test_'+''.join(random.choices("abcdefghijklmnopqrstuvwxyz",k=7)),
        'retain': ('test','retain'),
        'connect': {'port':PORT},
        'server': {'bind':[{'host':"localhost", 'port':PORT, 'ssl':False}]},
    },
    'listeners': {
        'default': {
            'type': 'tcp',
            'bind': '127.0.0.1:%d'%(PORT+1),
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
    async with anyio.create_task_group() as tg:
        s = Server("test", cfg=broker_config['distkv'], init="test")
        evt = anyio.create_event()
        await tg.spawn(partial(s.serve, ready_evt=evt))
        await evt.wait()

        for h, p, *_ in s.ports:
            if h[0] != ":":
                break
        broker_config['distkv']['server']['host'] = h
        broker_config['distkv']['server']['port'] = p
        async with open_client(**broker_config['distkv']) as cl:
            async def msglog(task_status=trio.TASK_STATUS_IGNORED):
                async with cl._stream('msg_monitor',topic='*') as mon:#, topic=broker_config['distkv']['topic']) as mon:
                    log.info("Monitor Start")
                    task_status.started()
                    async for m in mon:
                        log.info("Monitor Msg %r",m.data)
                        msgs.append(m.data)
            await cl.tg.spawn(msglog)
            yield s
            await cl.tg.cancel_scope.cancel()
        await tg.cancel_scope.cancel()
    if len(msgs) != n:
        log.error("MsgCount %d %d",len(msgs),n)
    #assert len(msgs) == n, msgs


class MQTTClientTest(unittest.TestCase):
    def test_deliver(self):
        data = b'data 123 a'

        async def test_coro():
            async with distkv_server(1):
                async with create_broker(broker_config, plugin_namespace="distmqtt.test.plugins") as broker:
                    async with open_mqttclient(config=broker_config['broker']) as client:
                        self.assertIsNotNone(client.session)
                        ret = await client.subscribe([
                            ('test_topic', QOS_0),
                        ])
                        self.assertEqual(ret[0], QOS_0)
                        async with open_mqttclient(config=broker_config['broker']) as client_pub:
                            await client_pub.publish('test_topic', data, QOS_0, retain=False)
                        #async with anyio.fail_after(0.5):
                        message = await client.deliver_message()
                        self.assertIsNotNone(message)
                        self.assertIsNotNone(message.publish_packet)
                        self.assertEqual(message.data, data)
                        pass # exit client
                    pass # exit broker
                pass # exit server
            pass # exit test

        anyio.run(test_coro, backend='trio')

    def test_deliver_retain_direct(self):
        data = b'data 123 b'

        async def test_coro():
            async with distkv_server(0):
                async with create_broker(broker_config, plugin_namespace="distmqtt.test.plugins") as broker:
                    async with open_mqttclient(config=broker_config['broker']) as client:
                        self.assertIsNotNone(client.session)
                        ret = await client.subscribe([
                            ('test_topic', QOS_0),
                        ])
                        self.assertEqual(ret[0], QOS_0)
                        async with open_mqttclient(config=broker_config['broker']) as client_pub:
                            await client_pub.publish('test_topic', data, QOS_0, retain=True)
                        async with anyio.fail_after(0.5):
                            message = await client.deliver_message()
                        self.assertIsNotNone(message)
                        self.assertIsNotNone(message.publish_packet)
                        self.assertEqual(message.data, data)

        anyio.run(test_coro, backend='trio')

    def test_deliver_retain_later(self):
        data = b'data 123 c'

        async def test_coro():
            async with distkv_server(0) as s:
                async with create_broker(broker_config, plugin_namespace="distmqtt.test.plugins") as broker:
                    async with open_mqttclient(config=broker_config['broker']) as client:
                        self.assertIsNotNone(client.session)
                        async with open_mqttclient(config=broker_config['broker']) as client_pub:
                            await client_pub.publish('test_topic', data, QOS_0, retain=True)
                    await anyio.sleep(1)

                    async with open_mqttclient(config=broker_config['broker']) as client:
                        self.assertIsNotNone(client.session)
                        ret = await client.subscribe([
                            ('test_topic', QOS_0),
                        ])
                        self.assertEqual(ret[0], QOS_0)
                        async with anyio.fail_after(0.5):
                            message = await client.deliver_message()
                        self.assertIsNotNone(message)
                        self.assertIsNotNone(message.publish_packet)
                        self.assertEqual(message.data, data)

                async with create_broker(broker_config, plugin_namespace="distmqtt.test.plugins") as broker:
                    async with open_mqttclient(config=broker_config['broker']) as client:
                        self.assertIsNotNone(client.session)
                        ret = await client.subscribe([
                            ('test_topic', QOS_0),
                        ])
                        self.assertEqual(ret[0], QOS_0)
                        async with anyio.fail_after(0.5):
                            message = await client.deliver_message()
                        self.assertIsNotNone(message)
                        self.assertIsNotNone(message.publish_packet)
                        self.assertEqual(message.data, data)

                seen = 0
                for h, p, *_ in s.ports:
                    if h[0] != ":":
                        break
                async with open_client(**broker_config['distkv']) as cl:
                    async for m in cl.get_tree(min_depth=1):
                        del m['tock']
                        del m['seq']
                        assert m == {
                            'path': ('test', 'retain', 'test_topic'),
                            'value': data}
                        seen += 1
                assert seen == 1

        anyio.run(test_coro, backend='trio')

    def test_deliver_timeout(self):
        async def test_coro():
            async with distkv_server(0):
                async with create_broker(broker_config, plugin_namespace="distmqtt.test.plugins") as broker:
                    async with open_mqttclient(config=broker_config['broker']) as client:
                        self.assertIsNotNone(client.session)
                        ret = await client.subscribe([
                            ('test_topic', QOS_0),
                        ])
                        self.assertEqual(ret[0], QOS_0)
                        with self.assertRaises(TimeoutError):
                            async with anyio.fail_after(2):
                                await client.deliver_message()

        anyio.run(test_coro, backend='trio')
