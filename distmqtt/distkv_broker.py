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
from distkv.util import PathLongener, NotGiven


class DistKVbroker(Broker):
    """
    A Broker that routes messages through DistKV / MQTT.
    """

    __slots__ = (
        '_distkv_broker__client',
        '_distkv_broker__topic',
        '_distkv_broker__tranparent',
        '_distkv_broker__retain',
    )
    __retain = False

    def __init__(self, tg: anyio.abc.TaskGroup, config=None, plugin_namespace=None):
        self.__client = None
        super().__init__(tg, config=config, plugin_namespace=plugin_namespace)
        cfg = self.config['distkv']
        self.__topic = cfg['topic']
        self.__transparent = cfg['transparent']

    async def __read_encap(self, client, cfg: dict, evt: Optional[anyio.abc.Event] = None):
        """
        Read encapsulated messages from the real server and forward them
        """

        async with self.__client.msg_monitor(self.__topic) as q:
            if evt is not None:
                await evt.set()
            async for m in q:
                d = m.data
                sess = d.pop('session', None)
                if sess is not None:
                    sess = self._sessions.get(sess, None)
                    if sess is not None:
                        sess = sess[0]
                await super().broadcast_message(session=sess, **d)

    async def __read_topic(self, topic, client, cfg: dict, evt: Optional[anyio.abc.Event] = None):
        """
        Read topical messages from the real server and forward them
        """
        async with self.__client.msg_monitor(topic) as q:
            if evt is not None:
                await evt.set()
            async for m in q:
                d = m.data
                t = m.topic
                await super().broadcast_message(topic=t,data=d,session=None)


    async def __session(self, cfg: dict, evt: Optional[anyio.abc.Event] = None):
        """
        Read any messages from the real server and forward them
        """
        try:
            async with open_distkv_client(connect=cfg['server']) as client:
                self.__client = client
                async with anyio.create_task_group() as tg:
                    async def start(p,*a):
                        evt = anyio.create_event()
                        await tg.spawn(p,*a, client,cfg, evt)
                        await evt.wait()

                    if self.__topic:
                        await start(self.__read_encap)
                    for t in cfg.get('transparent',()):
                        await start(self.__read_topic,t)
                        await start(self.__read_topic,t+['#'])

                    if evt is not None:
                        await evt.set()
                # The taskgroup waits for it all to finish, i.e. indefinitely
        finally:
            self.__client = None

    async def __retain_task(self, cfg: dict, evt: Optional[anyio.abc.Event] = None):
        """
        Read changes from DistKV and broadcast them
        """
        path = cfg['retain']
        if isinstance(path,str):
            path = path.split('/')
        self.__retain = list(path)

        pl = PathLongener(())  # intentionally not including the path
        async with self.__client.watch(*path, fetch=True, long_path=False) as w:
            await evt.set()
            async for msg in w:
                if 'path' not in msg:
                    continue
                pl(msg)
                topic = '/'.join(msg.path)
                data = msg.get('value', NotGiven)
                if data is NotGiven:
                    data = b''
                await super().broadcast_message(session=None, topic=topic, data=data, retain=True)
            
    async def start(self):
        cfg = self.config['distkv']

        evt = anyio.create_event()
        await self._tg.spawn(self.__session, cfg, evt)
        await evt.wait()
        if 'retain' in cfg:
            evt = anyio.create_event()
            await self._tg.spawn(self.__retain_task, cfg, evt)
            await evt.wait()
        await super().start()

    async def broadcast_message(self, session, topic, data, force_qos=None, qos=None, retain=False):
        ts = topic.split('/')

        if topic[0] == '$':
            # $SYS and whatever-else-dollar messages are not DistKV's problem.
            await super().broadcast_message(session, topic, data, retain=retain)
            return

        if retain and self.__retain:
            # All retained messages get stored in DistKV.
            await self.__client.set(*(self.__retain + ts), value=data)
            return

        for t in self.__transparent:
            # Messages to be forwarded transparently. The "retain" flag is ignored.
            if len(ts) >= len(t) and t == ts[:len(t)]:
                await self.__client.msg_send(topic=ts, data=data)
                return

        if self.__topic:
            # Anything else is encapsulated
            msg = dict(session=session.client_id, topic=topic, data=data, retain=retain)
            if qos is not None:
                msg['qos'] = qos
            if force_qos is not None:
                msg['force_qos'] = qos
            await self.__client.msg_send(topic=self.__topic, data=msg)
            return

        self.logger.info("Message ignored: %s %r",topic,data)

