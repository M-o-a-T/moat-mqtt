# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import unittest

from moat.mqtt.adapters import BufferAdapter
from moat.mqtt.mqtt.connect import ConnectPacket, ConnectPayload, ConnectVariableHeader
from moat.mqtt.mqtt.packet import CONNECT, MQTTFixedHeader

from .. import anyio_run


class ConnectPacketTest(unittest.TestCase):
    def test_decode_ok(self):
        data = b"\x10\x3e\x00\x04MQTT\x04\xce\x00\x00\x00\x0a0123456789\x00\x09WillTopic\x00\x0bWillMessage\x00\x04user\x00\x08password"
        stream = BufferAdapter(data)
        message = anyio_run(ConnectPacket.from_stream, stream)
        self.assertEqual(message.variable_header.proto_name, "MQTT")
        self.assertEqual(message.variable_header.proto_level, 4)
        self.assertTrue(message.variable_header.username_flag)
        self.assertTrue(message.variable_header.password_flag)
        self.assertFalse(message.variable_header.will_retain_flag)
        self.assertEqual(message.variable_header.will_qos, 1)
        self.assertTrue(message.variable_header.will_flag)
        self.assertTrue(message.variable_header.clean_session_flag)
        self.assertFalse(message.variable_header.reserved_flag)
        self.assertEqual(message.payload.client_id, "0123456789")
        self.assertEqual(message.payload.will_topic, "WillTopic")
        self.assertEqual(message.payload.will_message, b"WillMessage")
        self.assertEqual(message.payload.username, "user")
        self.assertEqual(message.payload.password, "password")

    def test_decode_ok_will_flag(self):
        data = (
            b"\x10\x26\x00\x04MQTT\x04\xca\x00\x00\x00\x0a0123456789\x00\x04user\x00\x08password"
        )
        stream = BufferAdapter(data)
        message = anyio_run(ConnectPacket.from_stream, stream)
        self.assertEqual(message.variable_header.proto_name, "MQTT")
        self.assertEqual(message.variable_header.proto_level, 4)
        self.assertTrue(message.variable_header.username_flag)
        self.assertTrue(message.variable_header.password_flag)
        self.assertFalse(message.variable_header.will_retain_flag)
        self.assertEqual(message.variable_header.will_qos, 1)
        self.assertFalse(message.variable_header.will_flag)
        self.assertTrue(message.variable_header.clean_session_flag)
        self.assertFalse(message.variable_header.reserved_flag)
        self.assertEqual(message.payload.client_id, "0123456789")
        self.assertEqual(message.payload.will_topic, None)
        self.assertEqual(message.payload.will_message, None)
        self.assertEqual(message.payload.username, "user")
        self.assertEqual(message.payload.password, "password")

    def test_decode_fail_reserved_flag(self):
        data = b"\x10\x3e\x00\x04MQTT\x04\xcf\x00\x00\x00\x0a0123456789\x00\x09WillTopic\x00\x0bWillMessage\x00\x04user\x00\x08password"
        stream = BufferAdapter(data)
        message = anyio_run(ConnectPacket.from_stream, stream)
        self.assertTrue(message.variable_header.reserved_flag)

    def test_decode_fail_miss_clientId(self):
        data = b"\x10\x0a\x00\x04MQTT\x04\xce\x00\x00"
        stream = BufferAdapter(data)
        message = anyio_run(ConnectPacket.from_stream, stream)
        self.assertIsNot(message.payload.client_id, None)

    def test_decode_fail_miss_willtopic(self):
        data = b"\x10\x16\x00\x04MQTT\x04\xce\x00\x00\x00\x0a0123456789"
        stream = BufferAdapter(data)
        message = anyio_run(ConnectPacket.from_stream, stream)
        self.assertIs(message.payload.will_topic, None)

    def test_decode_fail_miss_username(self):
        data = b"\x10\x2e\x00\x04MQTT\x04\xce\x00\x00\x00\x0a0123456789\x00\x09WillTopic\x00\x0bWillMessage"
        stream = BufferAdapter(data)
        message = anyio_run(ConnectPacket.from_stream, stream)
        self.assertIs(message.payload.username, None)

    def test_decode_fail_miss_password(self):
        data = b"\x10\x34\x00\x04MQTT\x04\xce\x00\x00\x00\x0a0123456789\x00\x09WillTopic\x00\x0bWillMessage\x00\x04user"
        stream = BufferAdapter(data)
        message = anyio_run(ConnectPacket.from_stream, stream)
        self.assertIs(message.payload.password, None)

    def test_encode(self):
        header = MQTTFixedHeader(CONNECT, 0x00, 0)
        variable_header = ConnectVariableHeader(0xCE, 0, "MQTT", 4)
        payload = ConnectPayload("0123456789", "WillTopic", b"WillMessage", "user", "password")
        message = ConnectPacket(header, variable_header, payload)
        encoded = message.to_bytes()
        self.assertEqual(
            encoded,
            b"\x10\x3e\x00\x04MQTT\x04\xce\x00\x00\x00\x0a0123456789\x00\x09WillTopic\x00\x0bWillMessage\x00\x04user\x00\x08password",
        )

    def test_getattr_ok(self):
        data = b"\x10\x3e\x00\x04MQTT\x04\xce\x00\x00\x00\x0a0123456789\x00\x09WillTopic\x00\x0bWillMessage\x00\x04user\x00\x08password"
        stream = BufferAdapter(data)
        message = anyio_run(ConnectPacket.from_stream, stream)
        self.assertEqual(message.variable_header.proto_name, "MQTT")
        self.assertEqual(message.proto_name, "MQTT")
        self.assertEqual(message.variable_header.proto_level, 4)
        self.assertEqual(message.proto_level, 4)
        self.assertTrue(message.variable_header.username_flag)
        self.assertTrue(message.username_flag)
        self.assertTrue(message.variable_header.password_flag)
        self.assertTrue(message.password_flag)
        self.assertFalse(message.variable_header.will_retain_flag)
        self.assertFalse(message.will_retain_flag)
        self.assertEqual(message.variable_header.will_qos, 1)
        self.assertEqual(message.will_qos, 1)
        self.assertTrue(message.variable_header.will_flag)
        self.assertTrue(message.will_flag)
        self.assertTrue(message.variable_header.clean_session_flag)
        self.assertTrue(message.clean_session_flag)
        self.assertFalse(message.variable_header.reserved_flag)
        self.assertFalse(message.reserved_flag)
        self.assertEqual(message.payload.client_id, "0123456789")
        self.assertEqual(message.client_id, "0123456789")
        self.assertEqual(message.payload.will_topic, "WillTopic")
        self.assertEqual(message.will_topic, "WillTopic")
        self.assertEqual(message.payload.will_message, b"WillMessage")
        self.assertEqual(message.will_message, b"WillMessage")
        self.assertEqual(message.payload.username, "user")
        self.assertEqual(message.username, "user")
        self.assertEqual(message.payload.password, "password")
        self.assertEqual(message.password, "password")
