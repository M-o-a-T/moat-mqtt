# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import anyio
import os
import logging
import unittest
from unittest.mock import patch, call, MagicMock

from distmqtt.adapters import StreamAdapter
from distmqtt.broker import (
    EVENT_BROKER_PRE_START,
    EVENT_BROKER_POST_START,
    EVENT_BROKER_PRE_SHUTDOWN,
    EVENT_BROKER_POST_SHUTDOWN,
    EVENT_BROKER_CLIENT_CONNECTED,
    EVENT_BROKER_CLIENT_DISCONNECTED,
    EVENT_BROKER_CLIENT_SUBSCRIBED,
    EVENT_BROKER_CLIENT_UNSUBSCRIBED,
    EVENT_BROKER_MESSAGE_RECEIVED,
    create_broker,
)
from distmqtt.client import open_mqttclient, ConnectException
from distmqtt.mqtt import (
    ConnectPacket,
    ConnackPacket,
    PublishPacket,
    PubrecPacket,
    PubrelPacket,
    DisconnectPacket,
)
from distmqtt.mqtt.connect import ConnectVariableHeader, ConnectPayload
from distmqtt.mqtt.constants import QOS_0, QOS_1, QOS_2


formatter = "%(asctime)s %(name)s:%(lineno)d %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=formatter)
log = logging.getLogger(__name__)

PORT = 40000 + (os.getpid() + 3) % 10000
URL = "mqtt://127.0.0.1:%d/" % PORT

test_config = {
    "listeners": {
        "default": {"type": "tcp", "bind": "127.0.0.1:%d" % PORT, "max_connections": 10}
    },
    "sys_interval": 0,
    "auth": {"allow-anonymous": True},
}


class AsyncMock(MagicMock):
    def __await__(self):
        async def foo():
            return self

        return foo().__await__()


class BrokerTest(unittest.TestCase):
    @patch("distmqtt.broker.PluginManager", new_callable=AsyncMock)
    def test_start_stop(self, MockPluginManager):  # pylint: disable=unused-argument
        async def test_coro():
            async with create_broker(
                test_config, plugin_namespace="distmqtt.test.plugins"
            ) as broker:
                broker.plugins_manager._tg = broker._tg
                self.assertTrue(broker.transitions.is_started())
                self.assertDictEqual(broker._sessions, {})
                self.assertIn("default", broker._servers)
                MockPluginManager.assert_has_calls(
                    [
                        call().fire_event(EVENT_BROKER_PRE_START),
                        call().fire_event(EVENT_BROKER_POST_START),
                    ],
                    any_order=True,
                )
                MockPluginManager.reset_mock()
            MockPluginManager.assert_has_calls(
                [
                    call().fire_event(EVENT_BROKER_PRE_SHUTDOWN),
                    call().fire_event(EVENT_BROKER_POST_SHUTDOWN),
                ],
                any_order=True,
            )
            self.assertTrue(broker.transitions.is_stopped())

        anyio.run(test_coro, backend="trio")

    @patch("distmqtt.broker.PluginManager", new_callable=AsyncMock)
    def test_client_connect(self, MockPluginManager):
        async def test_coro():
            async with create_broker(
                test_config, plugin_namespace="distmqtt.test.plugins"
            ) as broker:
                broker.plugins_manager._tg = broker._tg
                self.assertTrue(broker.transitions.is_started())
                async with open_mqttclient() as client:
                    ret = await client.connect(URL)
                    self.assertEqual(ret, 0)
                    self.assertEqual(len(broker._sessions), 1)
                    self.assertIn(client.session.client_id, broker._sessions)
                await anyio.sleep(0.1)  # let the broker task process the packet
            self.assertTrue(broker.transitions.is_stopped())
            self.assertDictEqual(broker._sessions, {})
            MockPluginManager.assert_has_calls(
                [
                    call().fire_event(
                        EVENT_BROKER_CLIENT_CONNECTED,
                        client_id=client.session.client_id,
                    ),
                    call().fire_event(
                        EVENT_BROKER_CLIENT_DISCONNECTED,
                        client_id=client.session.client_id,
                    ),
                ],
                any_order=True,
            )

        anyio.run(test_coro)

    @patch("distmqtt.broker.PluginManager", new_callable=AsyncMock)
    def test_client_connect_will_flag(
        self, MockPluginManager
    ):  # pylint: disable=unused-argument
        async def test_coro():
            async with create_broker(
                test_config, plugin_namespace="distmqtt.test.plugins"
            ) as broker:
                broker.plugins_manager._tg = broker._tg
                self.assertTrue(broker.transitions.is_started())

                async with await anyio.connect_tcp("127.0.0.1", PORT) as conn:
                    stream = StreamAdapter(conn)

                    vh = ConnectVariableHeader()
                    payload = ConnectPayload()

                    vh.keep_alive = 10
                    vh.clean_session_flag = False
                    vh.will_retain_flag = False
                    vh.will_flag = True
                    vh.will_qos = QOS_0
                    payload.client_id = "test_id"
                    payload.will_message = b"test"
                    payload.will_topic = "/topic"
                    connect = ConnectPacket(vh=vh, payload=payload)
                    await connect.to_stream(stream)
                    await ConnackPacket.from_stream(stream)

                    disconnect = DisconnectPacket()
                    await disconnect.to_stream(stream)

            self.assertTrue(broker.transitions.is_stopped())
            self.assertDictEqual(broker._sessions, {})

        anyio.run(test_coro, backend="trio")

    @patch("distmqtt.broker.PluginManager", new_callable=AsyncMock)
    def test_client_connect_clean_session_false(
        self, MockPluginManager
    ):  # pylint: disable=unused-argument
        async def test_coro():
            async with create_broker(
                test_config, plugin_namespace="distmqtt.test.plugins"
            ) as broker:
                broker.plugins_manager._tg = broker._tg
                self.assertTrue(broker.transitions.is_started())
                async with open_mqttclient(
                    client_id="", config={"auto_reconnect": False}
                ) as client:
                    return_code = None
                    try:
                        await client.connect(URL, cleansession=False)
                    except ConnectException as ce:
                        return_code = ce.return_code
                    self.assertEqual(return_code, 0x02)
                    self.assertNotIn(client.session.client_id, broker._sessions)

        anyio.run(test_coro)

    @patch("distmqtt.broker.PluginManager", new_callable=AsyncMock)
    def test_client_subscribe(self, MockPluginManager):
        async def test_coro():
            async with create_broker(
                test_config, plugin_namespace="distmqtt.test.plugins"
            ) as broker:
                broker.plugins_manager._tg = broker._tg
                self.assertTrue(broker.transitions.is_started())
                async with open_mqttclient() as client:
                    ret = await client.connect(URL)
                    self.assertEqual(ret, 0)
                    await client.subscribe([("/topic", QOS_0)])

                    # Test if the client test client subscription is registered
                    subs = broker._subscriptions[("", "topic")]
                    self.assertEqual(len(subs), 1)
                    (s, qos) = subs[0]
                    self.assertEqual(s, client.session)
                    self.assertEqual(qos, QOS_0)

            self.assertTrue(broker.transitions.is_stopped())
            MockPluginManager.assert_has_calls(
                [
                    call().fire_event(
                        EVENT_BROKER_CLIENT_SUBSCRIBED,
                        client_id=client.session.client_id,
                        topic="/topic",
                        qos=QOS_0,
                    )
                ],
                any_order=True,
            )

        anyio.run(test_coro, backend="trio")

    @patch("distmqtt.broker.PluginManager", new_callable=AsyncMock)
    def test_client_subscribe_twice(self, MockPluginManager):
        async def test_coro():
            async with create_broker(
                test_config, plugin_namespace="distmqtt.test.plugins"
            ) as broker:
                broker.plugins_manager._tg = broker._tg
                self.assertTrue(broker.transitions.is_started())
                async with open_mqttclient() as client:
                    ret = await client.connect(URL)
                    self.assertEqual(ret, 0)
                    await client.subscribe([("/topic", QOS_0)])

                    # Test if the client test client subscription is registered
                    subs = broker._subscriptions[("", "topic")]
                    self.assertEqual(len(subs), 1)
                    (s, qos) = subs[0]
                    self.assertEqual(s, client.session)
                    self.assertEqual(qos, QOS_0)

                    await client.subscribe([("/topic", QOS_0)])
                    self.assertEqual(len(subs), 1)
                    (s, qos) = subs[0]
                    self.assertEqual(s, client.session)
                    self.assertEqual(qos, QOS_0)

            self.assertTrue(broker.transitions.is_stopped())
            MockPluginManager.assert_has_calls(
                [
                    call().fire_event(
                        EVENT_BROKER_CLIENT_SUBSCRIBED,
                        client_id=client.session.client_id,
                        topic="/topic",
                        qos=QOS_0,
                    )
                ],
                any_order=True,
            )

        anyio.run(test_coro, backend="trio")

    @patch("distmqtt.broker.PluginManager", new_callable=AsyncMock)
    def test_client_unsubscribe(self, MockPluginManager):
        async def test_coro():
            async with create_broker(
                test_config, plugin_namespace="distmqtt.test.plugins"
            ) as broker:
                broker.plugins_manager._tg = broker._tg
                self.assertTrue(broker.transitions.is_started())
                async with open_mqttclient() as client:
                    ret = await client.connect(URL)
                    self.assertEqual(ret, 0)
                    await client.subscribe([("/topic", QOS_0)])

                    # Test if the client test client subscription is registered
                    subs = broker._subscriptions[("", "topic")]
                    self.assertEqual(len(subs), 1)
                    (s, qos) = subs[0]
                    self.assertEqual(s, client.session)
                    self.assertEqual(qos, QOS_0)

                    await client.unsubscribe(["/topic"])
                    self.assertEqual(broker._subscriptions[("", "topic")], [])
            self.assertTrue(broker.transitions.is_stopped())
            MockPluginManager.assert_has_calls(
                [
                    call().fire_event(
                        EVENT_BROKER_CLIENT_SUBSCRIBED,
                        client_id=client.session.client_id,
                        topic="/topic",
                        qos=QOS_0,
                    ),
                    call().fire_event(
                        EVENT_BROKER_CLIENT_UNSUBSCRIBED,
                        client_id=client.session.client_id,
                        topic="/topic",
                    ),
                ],
                any_order=True,
            )

        anyio.run(test_coro, backend="trio")

    @patch("distmqtt.broker.PluginManager", new_callable=AsyncMock)
    def test_client_publish(self, MockPluginManager):
        async def test_coro():
            async with create_broker(
                test_config, plugin_namespace="distmqtt.test.plugins"
            ) as broker:
                broker.plugins_manager._tg = broker._tg
                self.assertTrue(broker.transitions.is_started())
                async with open_mqttclient() as pub_client:
                    ret = await pub_client.connect(URL)
                    self.assertEqual(ret, 0)

                    ret_message = await pub_client.publish("/topic", b"data", QOS_0)
                await anyio.sleep(0.1)  # let the broker task process the packet
                self.assertEqual(broker._retained_messages, {})

            self.assertTrue(broker.transitions.is_stopped())
            MockPluginManager.assert_has_calls(
                [
                    call().fire_event(
                        EVENT_BROKER_MESSAGE_RECEIVED,
                        client_id=pub_client.session.client_id,
                        message=ret_message,
                    )
                ],
                any_order=True,
            )

        anyio.run(test_coro)

    # @patch('distmqtt.broker.PluginManager', new_callable=AsyncMock)
    def test_client_publish_dup(self):
        async def test_coro():
            async with create_broker(
                test_config, plugin_namespace="distmqtt.test.plugins"
            ) as broker:
                broker.plugins_manager._tg = broker._tg
                self.assertTrue(broker.transitions.is_started())

                async with await anyio.connect_tcp("127.0.0.1", PORT) as conn:
                    stream = StreamAdapter(conn)

                    vh = ConnectVariableHeader()
                    payload = ConnectPayload()

                    vh.keep_alive = 10
                    vh.clean_session_flag = False
                    vh.will_retain_flag = False
                    payload.client_id = "test_id"
                    connect = ConnectPacket(vh=vh, payload=payload)
                    await connect.to_stream(stream)
                    await ConnackPacket.from_stream(stream)

                    publish_1 = PublishPacket.build(
                        "/test", b"data", 1, False, QOS_2, False
                    )
                    await publish_1.to_stream(stream)
                    await PubrecPacket.from_stream(stream)

                    publish_dup = PublishPacket.build(
                        "/test", b"data", 1, True, QOS_2, False
                    )
                    await publish_dup.to_stream(stream)
                    await PubrecPacket.from_stream(stream)
                    pubrel = PubrelPacket.build(1)
                    await pubrel.to_stream(stream)
                    # await PubcompPacket.from_stream(stream)

                    disconnect = DisconnectPacket()
                    await disconnect.to_stream(stream)

        anyio.run(test_coro, backend="trio")

    @patch("distmqtt.broker.PluginManager", new_callable=AsyncMock)
    def test_client_publish_invalid_topic(
        self, MockPluginManager
    ):  # pylint: disable=unused-argument
        async def test_coro():
            async with create_broker(
                test_config, plugin_namespace="distmqtt.test.plugins"
            ) as broker:
                broker.plugins_manager._tg = broker._tg
                self.assertTrue(broker.transitions.is_started())
                async with open_mqttclient() as pub_client:
                    ret = await pub_client.connect(URL)
                    self.assertEqual(ret, 0)

                    await pub_client.publish("/+", b"data", QOS_0)

            self.assertTrue(broker.transitions.is_stopped())

        anyio.run(test_coro)

    @patch("distmqtt.broker.PluginManager", new_callable=AsyncMock)
    def test_client_publish_big(self, MockPluginManager):
        async def test_coro():
            async with create_broker(
                test_config, plugin_namespace="distmqtt.test.plugins"
            ) as broker:
                broker.plugins_manager._tg = broker._tg
                self.assertTrue(broker.transitions.is_started())
                async with open_mqttclient() as pub_client:
                    ret = await pub_client.connect(URL)
                    self.assertEqual(ret, 0)

                    ret_message = await pub_client.publish(
                        "/topic", bytearray(b"\x99" * 256 * 1024), QOS_2
                    )
                self.assertEqual(broker._retained_messages, {})

            self.assertTrue(broker.transitions.is_stopped())
            MockPluginManager.assert_has_calls(
                [
                    call().fire_event(
                        EVENT_BROKER_MESSAGE_RECEIVED,
                        client_id=pub_client.session.client_id,
                        message=ret_message,
                    )
                ],
                any_order=True,
            )

        anyio.run(test_coro, backend="trio")

    @patch("distmqtt.broker.PluginManager", new_callable=AsyncMock)
    def test_client_publish_retain(
        self, MockPluginManager
    ):  # pylint: disable=unused-argument
        async def test_coro():
            async with create_broker(
                test_config, plugin_namespace="distmqtt.test.plugins"
            ) as broker:
                broker.plugins_manager._tg = broker._tg
                self.assertTrue(broker.transitions.is_started())

                async with open_mqttclient() as pub_client:
                    ret = await pub_client.connect(URL)
                    self.assertEqual(ret, 0)
                    await pub_client.publish("/topic", b"data", QOS_0, retain=True)
                await anyio.sleep(0.1)  # let the broker task process the packet
                self.assertIn("/topic", broker._retained_messages)
                retained_message = broker._retained_messages["/topic"]
                self.assertEqual(retained_message.source_session, pub_client.session)
                self.assertEqual(retained_message.topic, "/topic")
                self.assertEqual(retained_message.data, b"data")
                self.assertEqual(retained_message.qos, QOS_0)
            self.assertTrue(broker.transitions.is_stopped())

        anyio.run(test_coro)

    @patch("distmqtt.broker.PluginManager", new_callable=AsyncMock)
    def test_client_publish_retain_delete(
        self, MockPluginManager
    ):  # pylint: disable=unused-argument
        async def test_coro():
            async with create_broker(
                test_config, plugin_namespace="distmqtt.test.plugins"
            ) as broker:
                broker.plugins_manager._tg = broker._tg
                self.assertTrue(broker.transitions.is_started())

                async with open_mqttclient() as pub_client:
                    ret = await pub_client.connect(URL)
                    self.assertEqual(ret, 0)
                    await pub_client.publish("/topic", b"", QOS_0, retain=True)
                await anyio.sleep(0.1)  # let the broker task process the packet
                self.assertNotIn("/topic", broker._retained_messages)
            self.assertTrue(broker.transitions.is_stopped())

        anyio.run(test_coro)

    @patch("distmqtt.broker.PluginManager", new_callable=AsyncMock)
    def test_client_subscribe_publish(
        self, MockPluginManager
    ):  # pylint: disable=unused-argument
        async def test_coro():
            async with create_broker(
                test_config, plugin_namespace="distmqtt.test.plugins"
            ) as broker:
                broker.plugins_manager._tg = broker._tg
                self.assertTrue(broker.transitions.is_started())
                async with open_mqttclient() as sub_client:
                    await sub_client.connect(URL)
                    ret = await sub_client.subscribe(
                        [("/qos0", QOS_0), ("/qos1", QOS_1), ("/qos2", QOS_2)]
                    )
                    self.assertEqual(ret, [QOS_0, QOS_1, QOS_2])

                    await self._client_publish("/qos0", b"data", QOS_0)
                    await self._client_publish("/qos1", b"data", QOS_1)
                    await self._client_publish("/qos2", b"data", QOS_2)
                    for qos in [QOS_0, QOS_1, QOS_2]:
                        message = await sub_client.deliver_message()
                        self.assertIsNotNone(message)
                        self.assertEqual(message.topic, "/qos%s" % qos)
                        self.assertEqual(message.data, b"data")
                        self.assertEqual(message.qos, qos)
            self.assertTrue(broker.transitions.is_stopped())

        anyio.run(test_coro)

    @patch("distmqtt.broker.PluginManager", new_callable=AsyncMock)
    def test_client_subscribe_invalid(
        self, MockPluginManager
    ):  # pylint: disable=unused-argument
        async def test_coro():
            async with create_broker(
                test_config, plugin_namespace="distmqtt.test.plugins"
            ) as broker:
                broker.plugins_manager._tg = broker._tg
                self.assertTrue(broker.transitions.is_started())
                async with open_mqttclient() as sub_client:
                    await sub_client.connect(URL)
                    ret = await sub_client.subscribe(
                        [
                            ("+", QOS_0),
                            ("+/tennis/#", QOS_0),
                            ("sport+", QOS_0),
                            ("sport/+/player1", QOS_0),
                        ]
                    )
                    self.assertEqual(ret, [QOS_0, QOS_0, 0x80, QOS_0])

            self.assertTrue(broker.transitions.is_stopped())

        anyio.run(test_coro, backend="trio")

    @patch("distmqtt.broker.PluginManager", new_callable=AsyncMock)
    def test_client_subscribe_publish_dollar_topic_1(
        self, MockPluginManager
    ):  # pylint: disable=unused-argument
        async def test_coro():
            async with create_broker(
                test_config, plugin_namespace="distmqtt.test.plugins"
            ) as broker:
                broker.plugins_manager._tg = broker._tg
                self.assertTrue(broker.transitions.is_started())
                async with open_mqttclient() as sub_client:
                    await sub_client.connect(URL)
                    ret = await sub_client.subscribe([("#", QOS_0)])
                    self.assertEqual(ret, [QOS_0])

                    await self._client_publish("/topic", b"data", QOS_0)
                    message = await sub_client.deliver_message()
                    self.assertIsNotNone(message)

                    await self._client_publish("$topic", b"data", QOS_0)
                    message = None
                    with self.assertRaises(TimeoutError):
                        async with anyio.fail_after(1):
                            message = await sub_client.deliver_message()
                    self.assertIsNone(message)
            self.assertTrue(broker.transitions.is_stopped())

        anyio.run(test_coro)

    @patch("distmqtt.broker.PluginManager", new_callable=AsyncMock)
    def test_client_subscribe_publish_dollar_topic_2(
        self, MockPluginManager
    ):  # pylint: disable=unused-argument
        async def test_coro():
            async with create_broker(
                test_config, plugin_namespace="distmqtt.test.plugins"
            ) as broker:
                broker.plugins_manager._tg = broker._tg
                self.assertTrue(broker.transitions.is_started())
                async with open_mqttclient() as sub_client:
                    await sub_client.connect(URL)
                    ret = await sub_client.subscribe([("+/monitor/Clients", QOS_0)])
                    self.assertEqual(ret, [QOS_0])

                    await self._client_publish("test/monitor/Clients", b"data", QOS_0)
                    message = await sub_client.deliver_message()
                    self.assertIsNotNone(message)

                    await self._client_publish("$SYS/monitor/Clients", b"data", QOS_0)
                    message = None
                    with self.assertRaises(TimeoutError):
                        async with anyio.fail_after(1):
                            message = await sub_client.deliver_message()
                    self.assertIsNone(message)
            self.assertTrue(broker.transitions.is_stopped())

        anyio.run(test_coro)

    @patch("distmqtt.broker.PluginManager", new_callable=AsyncMock)
    def test_client_publish_retain_subscribe(
        self, MockPluginManager
    ):  # pylint: disable=unused-argument
        async def test_coro():
            async with create_broker(
                test_config, plugin_namespace="distmqtt.test.plugins"
            ) as broker:
                broker.plugins_manager._tg = broker._tg
                self.assertTrue(broker.transitions.is_started())
                async with open_mqttclient() as sub_client:
                    await sub_client.connect(URL, cleansession=False)
                    ret = await sub_client.subscribe(
                        [("/qos0", QOS_0), ("/qos1", QOS_1), ("/qos2", QOS_2)]
                    )
                    self.assertEqual(ret, [QOS_0, QOS_1, QOS_2])
                    await sub_client.disconnect()

                    await self._client_publish("/qos0", b"data", QOS_0, retain=True)
                    await self._client_publish("/qos1", b"data", QOS_1, retain=True)
                    await self._client_publish("/qos2", b"data", QOS_2, retain=True)
                    await sub_client.reconnect()
                    for qos in [QOS_0, QOS_1, QOS_2]:
                        log.debug("TEST QOS: %d", qos)
                        message = await sub_client.deliver_message()
                        log.debug("Message: %r", message.publish_packet)
                        self.assertIsNotNone(message)
                        self.assertEqual(message.topic, "/qos%s" % qos)
                        self.assertEqual(message.data, b"data")
                        self.assertEqual(message.qos, qos)
            self.assertTrue(broker.transitions.is_stopped())

        anyio.run(test_coro)

    async def _client_publish(self, topic, data, qos, retain=False):
        async with open_mqttclient() as pub_client:
            ret = await pub_client.connect(URL)
            self.assertEqual(ret, 0)
            ret = await pub_client.publish(topic, data, qos, retain)
        return ret
