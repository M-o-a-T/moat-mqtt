# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import unittest
import anyio
import logging
import random
from distmqtt.plugins.manager import PluginManager
from distmqtt.session import (
    Session,
    OutgoingApplicationMessage,
    IncomingApplicationMessage,
)
from distmqtt.mqtt.protocol.handler import ProtocolHandler
from distmqtt.adapters import StreamAdapter
from distmqtt.mqtt.constants import QOS_0, QOS_1, QOS_2
from distmqtt.mqtt.publish import PublishPacket
from distmqtt.mqtt.puback import PubackPacket
from distmqtt.mqtt.pubrec import PubrecPacket
from distmqtt.mqtt.pubrel import PubrelPacket
from distmqtt.mqtt.pubcomp import PubcompPacket

formatter = (
    "[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
)
logging.basicConfig(level=logging.DEBUG, format=formatter)
log = logging.getLogger(__name__)


def rand_packet_id():
    return random.randint(0, 65535)


def adapt(conn):
    return StreamAdapter(conn)


class ProtocolHandlerTest(unittest.TestCase):
    handler = session = plugin_manager = None  # appease pylint
    listen_ctx = None

    async def listen_(self, server_mock, server):
        async with anyio.open_cancel_scope() as sc:
            self.listen_ctx = sc
            while True:
                sock = await server.accept()
                if not hasattr(sock, "read"):
                    setattr(sock, "read", sock.receive_some)
                if not hasattr(sock, "write"):
                    setattr(sock, "write", sock.send_all)
                await server_mock(sock)

    def run_(self, server_mock, test_coro):
        async def runner():
            async with anyio.create_task_group() as tg:
                self.plugin_manager = PluginManager(
                    tg, "distmqtt.test.plugins", context=None
                )
                async with await anyio.create_tcp_server(
                    port=8888, interface="127.0.0.1"
                ) as server:
                    await tg.spawn(self.listen_, server_mock, server)
                    async with await anyio.connect_tcp(
                        "127.0.0.1", server.port
                    ) as conn:
                        sr = adapt(conn)
                        await test_coro(sr)
                        await self.listen_ctx.cancel()

        anyio.run(runner)

    def test_start_stop(self):
        async def server_mock(stream):  # pylint: disable=unused-argument
            pass

        async def test_coro(stream_adapted):
            s = Session(None)
            handler = ProtocolHandler(self.plugin_manager)
            await handler.attach(s, stream_adapted)
            await self.start_handler(handler, s)
            await self.stop_handler(handler, s)

        self.run_(server_mock, test_coro)

    def test_publish_qos0(self):
        async def server_mock(stream):
            packet = await PublishPacket.from_stream(stream)
            self.assertEqual(packet.variable_header.topic_name, "/topic")
            self.assertEqual(packet.qos, QOS_0)
            self.assertIsNone(packet.packet_id)

        async def test_coro(stream_adapted):
            s = Session(None)
            handler = ProtocolHandler(self.plugin_manager)
            await handler.attach(s, stream_adapted)
            await self.start_handler(handler, s)
            message = await handler.mqtt_publish("/topic", b"test_data", QOS_0, False)
            self.assertIsInstance(message, OutgoingApplicationMessage)
            self.assertIsNotNone(message.publish_packet)
            self.assertIsNone(message.puback_packet)
            self.assertIsNone(message.pubrec_packet)
            self.assertIsNone(message.pubrel_packet)
            self.assertIsNone(message.pubcomp_packet)
            await self.stop_handler(handler, s)

        self.run_(server_mock, test_coro)

    def test_publish_qos1(self):
        async def server_mock(stream):
            packet = await PublishPacket.from_stream(stream)
            self.assertEqual(packet.variable_header.topic_name, "/topic")
            self.assertEqual(packet.qos, QOS_1)
            self.assertIsNotNone(packet.packet_id)
            self.assertIn(packet.packet_id, self.session.inflight_out)
            self.assertIn(packet.packet_id, self.handler._puback_waiters)
            puback = PubackPacket.build(packet.packet_id)
            await puback.to_stream(stream)

        async def test_coro(stream_adapted):
            self.session = Session(None)
            self.handler = ProtocolHandler(self.plugin_manager)
            await self.handler.attach(self.session, stream_adapted)
            await self.start_handler(self.handler, self.session)
            message = await self.handler.mqtt_publish(
                "/topic", b"test_data", QOS_1, False
            )
            self.assertIsInstance(message, OutgoingApplicationMessage)
            self.assertIsNotNone(message.publish_packet)
            self.assertIsNotNone(message.puback_packet)
            self.assertIsNone(message.pubrec_packet)
            self.assertIsNone(message.pubrel_packet)
            self.assertIsNone(message.pubcomp_packet)
            await self.stop_handler(self.handler, self.session)

        self.handler = None
        self.run_(server_mock, test_coro)

    def test_publish_qos2(self):
        async def server_mock(stream):
            packet = await PublishPacket.from_stream(stream)
            self.assertEqual(packet.topic_name, "/topic")
            self.assertEqual(packet.qos, QOS_2)
            self.assertIsNotNone(packet.packet_id)
            self.assertIn(packet.packet_id, self.session.inflight_out)
            self.assertIn(packet.packet_id, self.handler._pubrec_waiters)
            pubrec = PubrecPacket.build(packet.packet_id)
            await pubrec.to_stream(stream)

            await PubrelPacket.from_stream(stream)
            self.assertIn(packet.packet_id, self.handler._pubcomp_waiters)
            pubcomp = PubcompPacket.build(packet.packet_id)
            await pubcomp.to_stream(stream)

        async def test_coro(stream_adapted):
            self.session = Session(None)
            self.handler = ProtocolHandler(self.plugin_manager)
            await self.handler.attach(self.session, stream_adapted)
            await self.start_handler(self.handler, self.session)
            message = await self.handler.mqtt_publish(
                "/topic", b"test_data", QOS_2, False
            )
            self.assertIsInstance(message, OutgoingApplicationMessage)
            self.assertIsNotNone(message.publish_packet)
            self.assertIsNone(message.puback_packet)
            self.assertIsNotNone(message.pubrec_packet)
            self.assertIsNotNone(message.pubrel_packet)
            self.assertIsNotNone(message.pubcomp_packet)
            await self.stop_handler(self.handler, self.session)

        self.handler = None

        self.run_(server_mock, test_coro)

    def test_receive_qos0(self):
        async def server_mock(stream):
            packet = PublishPacket.build(
                "/topic", b"test_data", rand_packet_id(), False, QOS_0, False
            )
            await packet.to_stream(stream)

        async def test_coro(stream_adapted):
            self.session = Session(None)
            self.handler = ProtocolHandler(self.plugin_manager)
            await self.handler.attach(self.session, stream_adapted)
            await self.start_handler(self.handler, self.session)
            message = await self.session.get_next_message()
            self.assertIsInstance(message, IncomingApplicationMessage)
            self.assertIsNotNone(message.publish_packet)
            self.assertIsNone(message.puback_packet)
            self.assertIsNone(message.pubrec_packet)
            self.assertIsNone(message.pubrel_packet)
            self.assertIsNone(message.pubcomp_packet)
            await self.stop_handler(self.handler, self.session)

        self.handler = None
        self.run_(server_mock, test_coro)

    def test_receive_qos1(self):
        async def server_mock(stream):
            packet = PublishPacket.build(
                "/topic", b"test_data", rand_packet_id(), False, QOS_1, False
            )
            await packet.to_stream(stream)
            puback = await PubackPacket.from_stream(stream)
            self.assertIsNotNone(puback)
            self.assertEqual(packet.packet_id, puback.packet_id)

        async def test_coro(stream_adapted):
            self.session = Session(None)
            self.handler = ProtocolHandler(self.plugin_manager)
            await self.handler.attach(self.session, stream_adapted)
            await self.start_handler(self.handler, self.session)
            message = await self.session.get_next_message()
            self.assertIsInstance(message, IncomingApplicationMessage)
            self.assertIsNotNone(message.publish_packet)
            self.assertIsNotNone(message.puback_packet)
            self.assertIsNone(message.pubrec_packet)
            self.assertIsNone(message.pubrel_packet)
            self.assertIsNone(message.pubcomp_packet)
            await self.stop_handler(self.handler, self.session)

        self.handler = None
        self.run_(server_mock, test_coro)

    def test_receive_qos2(self):
        async def server_mock(stream):
            packet = PublishPacket.build(
                "/topic", b"test_data", rand_packet_id(), False, QOS_2, False
            )
            await packet.to_stream(stream)
            pubrec = await PubrecPacket.from_stream(stream)
            self.assertIsNotNone(pubrec)
            self.assertEqual(packet.packet_id, pubrec.packet_id)
            self.assertIn(packet.packet_id, self.handler._pubrel_waiters)
            pubrel = PubrelPacket.build(packet.packet_id)
            await pubrel.to_stream(stream)
            pubcomp = await PubcompPacket.from_stream(stream)
            self.assertIsNotNone(pubcomp)
            self.assertEqual(packet.packet_id, pubcomp.packet_id)

        async def test_coro(stream_adapted):
            self.session = Session(None)
            self.handler = ProtocolHandler(self.plugin_manager)
            await self.handler.attach(self.session, stream_adapted)
            await self.start_handler(self.handler, self.session)
            message = await self.session.get_next_message()
            self.assertIsInstance(message, IncomingApplicationMessage)
            self.assertIsNotNone(message.publish_packet)
            self.assertIsNone(message.puback_packet)
            self.assertIsNotNone(message.pubrec_packet)
            self.assertIsNotNone(message.pubrel_packet)
            self.assertIsNotNone(message.pubcomp_packet)
            await self.stop_handler(self.handler, self.session)

        self.handler = None
        self.run_(server_mock, test_coro)

    async def start_handler(self, handler, session):
        self.check_empty_waiters(handler)
        self.check_no_message(session)
        await handler.start()

    async def stop_handler(self, handler, session):
        await handler.stop()
        self.assertTrue(handler._reader_stopped)
        self.check_empty_waiters(handler)
        self.check_no_message(session)

    def check_empty_waiters(self, handler):
        self.assertFalse(handler._puback_waiters)
        self.assertFalse(handler._pubrec_waiters)
        self.assertFalse(handler._pubrel_waiters)
        self.assertFalse(handler._pubcomp_waiters)

    def check_no_message(self, session):
        self.assertFalse(session.inflight_out)
        self.assertFalse(session.inflight_in)

    def test_publish_qos1_retry(self):
        async def server_mock(stream):
            packet = await PublishPacket.from_stream(stream)
            self.assertEqual(packet.topic_name, "/topic")
            self.assertEqual(packet.qos, QOS_1)
            self.assertIsNotNone(packet.packet_id)
            self.assertIn(packet.packet_id, self.session.inflight_out)
            self.assertIn(packet.packet_id, self.handler._puback_waiters)
            puback = PubackPacket.build(packet.packet_id)
            await puback.to_stream(stream)

        async def test_coro(stream_adapted):
            self.session = Session(None)
            message = OutgoingApplicationMessage(
                1, "/topic", QOS_1, b"test_data", False
            )
            message.publish_packet = PublishPacket.build(
                "/topic", b"test_data", rand_packet_id(), False, QOS_1, False
            )
            self.session.inflight_out[1] = message
            self.handler = ProtocolHandler(self.plugin_manager)
            await self.handler.attach(self.session, stream_adapted)
            await self.handler.start()
            await self.stop_handler(self.handler, self.session)

        self.handler = None

        self.run_(server_mock, test_coro)

    def test_publish_qos2_retry(self):
        async def server_mock(stream):
            packet = await PublishPacket.from_stream(stream)
            self.assertEqual(packet.topic_name, "/topic")
            self.assertEqual(packet.qos, QOS_2)
            self.assertIsNotNone(packet.packet_id)
            self.assertIn(packet.packet_id, self.session.inflight_out)
            self.assertIn(packet.packet_id, self.handler._pubrec_waiters)
            pubrec = PubrecPacket.build(packet.packet_id)
            await pubrec.to_stream(stream)

            await PubrelPacket.from_stream(stream)
            self.assertIn(packet.packet_id, self.handler._pubcomp_waiters)
            pubcomp = PubcompPacket.build(packet.packet_id)
            await pubcomp.to_stream(stream)

        async def test_coro(stream_adapted):
            self.session = Session(None)
            message = OutgoingApplicationMessage(
                1, "/topic", QOS_2, b"test_data", False
            )
            message.publish_packet = PublishPacket.build(
                "/topic", b"test_data", rand_packet_id(), False, QOS_2, False
            )
            self.session.inflight_out[1] = message
            self.handler = ProtocolHandler(self.plugin_manager)
            await self.handler.attach(self.session, stream_adapted)
            await self.handler.start()
            await self.stop_handler(self.handler, self.session)

        self.handler = None

        self.run_(server_mock, test_coro)
