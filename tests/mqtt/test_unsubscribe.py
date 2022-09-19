# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import unittest

from moat.mqtt.adapters import BufferAdapter
from moat.mqtt.mqtt.packet import PacketIdVariableHeader
from moat.mqtt.mqtt.unsubscribe import UnsubscribePacket, UnubscribePayload

from .. import anyio_run


class UnsubscribePacketTest(unittest.TestCase):
    def test_from_stream(self):
        data = b"\xa2\x0c\x00\n\x00\x03a/b\x00\x03c/d"
        stream = BufferAdapter(data)
        message = anyio_run(UnsubscribePacket.from_stream, stream)
        self.assertEqual(message.payload.topics[0], "a/b")
        self.assertEqual(message.payload.topics[1], "c/d")

    def test_to_stream(self):
        variable_header = PacketIdVariableHeader(10)
        payload = UnubscribePayload(["a/b", "c/d"])
        publish = UnsubscribePacket(variable_header=variable_header, payload=payload)
        out = publish.to_bytes()
        self.assertEqual(out, b"\xa2\x0c\x00\n\x00\x03a/b\x00\x03c/d")
