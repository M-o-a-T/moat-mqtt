# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import unittest
import anyio

from distmqtt.codecs import bytes_to_hex_str, bytes_to_int, decode_string, encode_string
from distmqtt.adapters import BufferAdapter


class TestCodecs(unittest.TestCase):
    def test_bytes_to_hex_str(self):
        ret = bytes_to_hex_str(b"\x7f")
        self.assertEqual(ret, "0x7f")

    def test_bytes_to_int(self):
        ret = bytes_to_int(b"\x7f")
        self.assertEqual(ret, 127)
        ret = bytes_to_int(b"\xff\xff")
        self.assertEqual(ret, 65535)

    def test_decode_string(self):
        async def test_coro():
            stream = BufferAdapter(b"\x00\x02AA")
            ret = await decode_string(stream)
            self.assertEqual(ret, "AA")

        anyio.run(test_coro)

    def test_encode_string(self):
        encoded = encode_string("AA")
        self.assertEqual(b"\x00\x02AA", encoded)
