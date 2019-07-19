# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import anyio
import logging

from typing import Optional

from transitions import Machine
from collections import OrderedDict

from distmqtt.mqtt.publish import PublishPacket
from distmqtt.errors import DistMQTTException, MQTTException
from distmqtt.mqtt.constants import QOS_0

from distmqtt.broker import Broker
from distmqtt.session import ApplicationMessage

from distkv.client import open_client as open_distkv_client
from distkv.util import PathLongener


class DistKVbroker(Broker):
    """
    A Broker that routes messages through DistKV / MQTT.
    """

    __slots__ = (
        '_distkv_broker__client',
        '_distkv_broker__topic',
    )
    __retain = False

    def __init__(self, tg: anyio.abc.TaskGroup, config=None, plugin_namespace=None):
        self.__client = None
        super().__init__(tg, config=config, plugin_namespace=plugin_namespace)

    async def __session(self, cfg: dict, evt: Optional[anyio.abc.Event] = None):
        async with open_distkv_client(**cfg['server']) as client:
            self.__client = client
            self.__topic = cfg['topic']

            try:
                async with self.__client.serf_mon(tag=self.__topic) as q:
                    await evt.set()
                    async for m in q:
                        d = m.data
                        sess = d.pop('session', None)
                        if sess is not None:
                            sess = self._sessions.get(sess, None)
                            if sess is not None:
                                sess = sess[0]
                        await super().broadcast_message(session=sess, **d)
            finally:
                self.__client = None

    async def __retain(self, cfg: dict, evt: Optional[anyio.abc.Event] = None):
        path = cfg['retain']
        if isinstance(path,str):
            path = path.split('/')
        self.__retain = list(path)
        pl = PathLongener(())  # intentionally not including the path
        async with self.__client.watch(*path, fetch=True, long_path=False) as w:
            await evt.set()
            async for msg in w:
                if 'path' not in msg or msg.get('value', None) is None:
                    continue
                pl(msg)
                topic = '/'.join(msg.path)
                data = msg.get('value', None)
                super().retain_message(None, topic, data, qos=None)
                if not data:
                    continue
                await super().broadcast_message(session=None, topic=topic, data=data)
            
    async def start(self):
        cfg = self.config['distkv']

        evt = anyio.create_event()
        await self._tg.spawn(self.__session, cfg, evt)
        await evt.wait()
        if 'retain' in cfg:
            evt = anyio.create_event()
            await self._tg.spawn(self.__retain, cfg, evt)
            await evt.wait()
        await super().start()

    async def broadcast_message(self, session, topic, data, force_qos=None, qos=None, retain=False):
        if retain and self.__retain:
            await self.__client.set(*(self.__retain + topic.split('/')), value=data)
            return
        msg = dict(session=session.client_id, topic=topic, data=data, qos=qos, force_qos=force_qos, retain=retain)
        await self.__client.serf_send(tag=self.__topic, data=msg)

