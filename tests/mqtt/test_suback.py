# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import anyio
import unittest

from distmqtt.mqtt.suback import SubackPacket, SubackPayload
from distmqtt.mqtt.packet import PacketIdVariableHeader
from distmqtt.adapters import BufferAdapter


class SubackPacketTest(unittest.TestCase):
    def test_from_stream(self):
        data = b"\x90\x06\x00\x0a\x00\x01\x02\x80"
        stream = BufferAdapter(data)
        message = anyio.run(SubackPacket.from_stream, stream)
        self.assertEqual(message.payload.return_codes[0], SubackPayload.RETURN_CODE_00)
        self.assertEqual(message.payload.return_codes[1], SubackPayload.RETURN_CODE_01)
        self.assertEqual(message.payload.return_codes[2], SubackPayload.RETURN_CODE_02)
        self.assertEqual(message.payload.return_codes[3], SubackPayload.RETURN_CODE_80)

    def test_to_stream(self):
        variable_header = PacketIdVariableHeader(10)
        payload = SubackPayload(
            [
                SubackPayload.RETURN_CODE_00,
                SubackPayload.RETURN_CODE_01,
                SubackPayload.RETURN_CODE_02,
                SubackPayload.RETURN_CODE_80,
            ]
        )
        suback = SubackPacket(variable_header=variable_header, payload=payload)
        out = suback.to_bytes()
        self.assertEqual(out, b"\x90\x06\x00\x0a\x00\x01\x02\x80")
