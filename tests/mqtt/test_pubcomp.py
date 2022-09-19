# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import unittest

from moat.mqtt.adapters import BufferAdapter
from moat.mqtt.mqtt.pubcomp import PacketIdVariableHeader, PubcompPacket

from .. import anyio_run


class PubcompPacketTest(unittest.TestCase):
    def test_from_stream(self):
        data = b"\x70\x02\x00\x0a"
        stream = BufferAdapter(data)
        message = anyio_run(PubcompPacket.from_stream, stream)
        self.assertEqual(message.variable_header.packet_id, 10)

    def test_to_bytes(self):
        variable_header = PacketIdVariableHeader(10)
        publish = PubcompPacket(variable_header=variable_header)
        out = publish.to_bytes()
        self.assertEqual(out, b"\x70\x02\x00\x0a")
