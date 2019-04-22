# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import anyio
import logging

from typing import Optional

from transitions import Machine
from collections import OrderedDict

from hbmqtt.mqtt.publish import PublishPacket
from hbmqtt.errors import HBMQTTException, MQTTException
from hbmqtt.mqtt.constants import QOS_0

from hbmqtt.broker import Broker
from hbmqtt.session import ApplicationMessage

from distkv.client import open_client as open_distkv_client


class DistKVbroker(Broker):

    """
        A Broker that routes messages through DistKV / MQTT.
    """

    __slots__ = (
        '_distkv_broker__client', '_distkv_broker__topic',
    )

    def __init__(self, tg: anyio.abc.TaskGroup, config=None, plugin_namespace=None):
        self.__client = None
        super().__init__(tg, config=config, plugin_namespace=plugin_namespace)

    async def __session(self, evt: Optional[anyio.abc.Event] = None):
        cfg = self.config['distkv']
        async with open_distkv_client(**cfg['server']) as client:
            self.__client = client
            self.__topic = cfg['topic']
            try:
                async with self.__client.stream('serfmon', type=self.__topic) as q:
                    await evt.set()
                    async for m in q:
                        d = m.data
                        sess = d.pop('session', None)
                        if sess is not None:
                            sess = self._sessions.get(sess, None)
                        await super().broadcast_message(session=sess, **d)
            finally:
                self.__client = None
            
    async def start(self):
        evt = anyio.create_event()
        await self._tg.spawn(self.__session, evt)
        await evt.wait()
        await super().start()

    async def broadcast_message(self, session, topic, data, force_qos=None, qos=None, retain=False):
        msg = dict(session=session.client_id, topic=topic, data=data, qos=qos, force_qos=force_qos, retain=retain)
        await self.__client.request('serfsend', type=self.__topic, data=msg)

