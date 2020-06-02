# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import anyio

from typing import Optional

from distmqtt.broker import Broker

from distkv.client import open_client as open_distkv_client
from distkv.util import PathLongener, NotGiven
from distkv.errors import ErrorRoot


class DistKVbroker(Broker):
    """
    A Broker that routes messages through DistKV / MQTT.
    """

    __slots__ = (
        "_distkv_broker__client",
        "_distkv_broker__topic",
        "_distkv_broker__tranparent",
        "_distkv_broker__base",
    )

    def __init__(self, tg: anyio.abc.TaskGroup, config=None, plugin_namespace=None):
        self.__client = None
        super().__init__(tg, config=config, plugin_namespace=plugin_namespace)
        cfg = self.config["distkv"]

        def spl(x, from_cfg=True):
            if from_cfg:
                x = cfg[x]
            if isinstance(x, str):
                x = x.split("/")
            return tuple(x)

        self.__topic = spl("topic")
        self.__base = spl("base")
        self.__transparent = [spl(x, False) for x in cfg.get("transparent", ())]

        if self.__topic[: len(self.__base)] == self.__base:
            raise ValueError("'topic' must not start with 'base'")

    async def __read_encap(
        self, client, cfg: dict, evt: Optional[anyio.abc.Event] = None
    ):  # pylint: disable=unused-argument
        """
        Read encapsulated messages from the real server and forward them
        """

        async with self.__client.msg_monitor(self.__topic) as q:
            if evt is not None:
                await evt.set()
            async for m in q:
                d = m.data
                sess = d.pop("session", None)
                if sess is not None:
                    sess = self._sessions.get(sess, None)
                    if sess is not None:
                        sess = sess[0]
                await super().broadcast_message(session=sess, **d)

    async def __read_topic(
        self, topic, client, cfg: dict, evt: Optional[anyio.abc.Event] = None
    ):  # pylint: disable=unused-argument
        """
        Read topical messages from the real server and forward them
        """
        async with self.__client.msg_monitor(topic, raw=True) as q:
            if evt is not None:
                await evt.set()
            async for m in q:
                d = m.raw
                t = m.topic
                await super().broadcast_message(topic=t, data=d, session=None)

    async def __session(self, cfg: dict, evt: Optional[anyio.abc.Event] = None):
        """
        Connect to the real server, read messages, forward them
        """
        try:
            async with open_distkv_client(connect=cfg["connect"]) as client:
                self.__client = client
                async with anyio.create_task_group() as tg:

                    async def start(p, *a):
                        evt = anyio.create_event()
                        await tg.spawn(p, *a, client, cfg, evt)
                        await evt.wait()

                    if self.__topic:
                        await start(self.__read_encap)
                    for t in self.__transparent:
                        await start(self.__read_topic, t)
                        await start(self.__read_topic, t + ("#",))

                    if evt is not None:
                        await evt.set()
                # The taskgroup waits for it all to finish, i.e. indefinitely
        finally:
            self.__client = None

    async def __retain_reader(
        self, cfg: dict, evt: Optional[anyio.abc.Event] = None
    ):  # pylint: disable=unused-argument
        """
        Read changes from DistKV and broadcast them
        """

        pl = PathLongener(self.__base)
        err = await ErrorRoot.as_handler(self.__client)
        async with self.__client.watch(*self.__base, fetch=True, long_path=False) as w:
            await evt.set()
            async for msg in w:
                if "path" not in msg:
                    continue
                pl(msg)
                data = msg.get("value", NotGiven)
                if data is NotGiven:
                    data = b""
                elif not isinstance(data, (bytes, bytearray)):
                    await err.record_error(
                        "distmqtt",
                        *msg.path,
                        data={"data": data},
                        message="non-binary data"
                    )
                    return
                else:
                    await super().broadcast_message(
                        session=None,
                        topic="/".join(msg["path"]),
                        data=data,
                        retain=True,
                    )
                await err.record_working("distmqtt", *msg.path)

    async def start(self):
        cfg = self.config["distkv"]

        await super().start()

        evt = anyio.create_event()
        await self._tg.spawn(self.__session, cfg, evt)
        await evt.wait()

        evt = anyio.create_event()
        await self._tg.spawn(self.__retain_reader, cfg, evt)
        await evt.wait()

    async def broadcast_message(
        self, session, topic, data, force_qos=None, qos=None, retain=False
    ):
        if isinstance(topic, str):
            ts = tuple(topic.split("/"))
        else:
            ts = tuple(topic)
            topic = "/".join(ts)

        if self.__client is None:
            self.logger.error("No client, dropping %s", topic)
            return  # can't do anything

        if topic[0] == "$":
            # $SYS and whatever-else-dollar messages are not DistKV's problem.
            await super().broadcast_message(session, topic, data, retain=retain)
            return

        if ts[: len(self.__base)] == self.__base:
            # All messages on "base" get stored in DistKV, retained or not.
            await self.__client.set(*ts, value=data)
            return

        for t in self.__transparent:
            # Messages to be forwarded transparently. The "retain" flag is ignored.
            if len(ts) >= len(t) and t == ts[: len(t)]:
                await self.__client.msg_send(topic=ts, raw=data)
                return

        if self.__topic:
            # Anything else is encapsulated
            msg = dict(session=session.client_id, topic=topic, data=data, retain=retain)
            if qos is not None:
                msg["qos"] = qos
            if force_qos is not None:
                msg["force_qos"] = qos
            await self.__client.msg_send(topic=self.__topic, data=msg)
            return

        self.logger.info("Message ignored: %s %r", topic, data)
