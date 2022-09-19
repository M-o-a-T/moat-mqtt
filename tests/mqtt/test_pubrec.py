# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import unittest

from moat.mqtt.adapters import BufferAdapter
from moat.mqtt.mqtt.pubrec import PacketIdVariableHeader, PubrecPacket

from .. import anyio_run


class PubrecPacketTest(unittest.TestCase):
    def test_from_stream(self):
        data = b"\x50\x02\x00\x0a"
        stream = BufferAdapter(data)
        message = anyio_run(PubrecPacket.from_stream, stream)
        self.assertEqual(message.variable_header.packet_id, 10)

    def test_to_bytes(self):
        variable_header = PacketIdVariableHeader(10)
        publish = PubrecPacket(variable_header=variable_header)
        out = publish.to_bytes()
        self.assertEqual(out, b"\x50\x02\x00\x0a")
