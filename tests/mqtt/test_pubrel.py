# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import anyio
import unittest

from distmqtt.mqtt.pubrel import PubrelPacket, PacketIdVariableHeader
from distmqtt.adapters import BufferAdapter


class PubrelPacketTest(unittest.TestCase):
    def test_from_stream(self):
        data = b"\x60\x02\x00\x0a"
        stream = BufferAdapter(data)
        message = anyio.run(PubrelPacket.from_stream, stream)
        self.assertEqual(message.variable_header.packet_id, 10)

    def test_to_bytes(self):
        variable_header = PacketIdVariableHeader(10)
        publish = PubrelPacket(variable_header=variable_header)
        out = publish.to_bytes()
        self.assertEqual(out, b"\x62\x02\x00\x0a")
