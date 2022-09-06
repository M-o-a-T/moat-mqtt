# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import anyio

from distmqtt.codecs import (
    bytes_to_hex_str,
    decode_packet_id,
    int_to_bytes,
    read_or_raise,
)
from distmqtt.errors import CodecException, MQTTException, NoDataException
from distmqtt.adapters import StreamAdapter
from datetime import datetime


RESERVED_0 = 0x00
CONNECT = 0x01
CONNACK = 0x02
PUBLISH = 0x03
PUBACK = 0x04
PUBREC = 0x05
PUBREL = 0x06
PUBCOMP = 0x07
SUBSCRIBE = 0x08
SUBACK = 0x09
UNSUBSCRIBE = 0x0A
UNSUBACK = 0x0B
PINGREQ = 0x0C
PINGRESP = 0x0D
DISCONNECT = 0x0E
RESERVED_15 = 0x0F


class MQTTFixedHeader:

    __slots__ = ("packet_type", "remaining_length", "flags")

    def __init__(self, packet_type, flags=0, length=0):
        self.packet_type = packet_type
        self.remaining_length = length
        self.flags = flags

    def to_bytes(self):
        def encode_remaining_length(length: int):
            encoded = bytearray()
            while True:
                length_byte = length % 0x80
                length //= 0x80
                if length > 0:
                    length_byte |= 0x80
                encoded.append(length_byte)
                if length <= 0:
                    break
            return encoded

        out = bytearray()
        packet_type = 0
        try:
            packet_type = (self.packet_type << 4) | self.flags
            out.append(packet_type)
        except OverflowError:
            raise CodecException(  # pylint:disable=W0707
                "packet_type encoding exceed 1 byte length: value=%d" % (packet_type,)
            )

        encoded_length = encode_remaining_length(self.remaining_length)
        out.extend(encoded_length)

        return out

    async def to_stream(self, writer: StreamAdapter):
        await writer.write(self.to_bytes())

    @property
    def bytes_length(self):
        return len(self.to_bytes())

    @classmethod
    async def from_stream(cls, reader: StreamAdapter):
        """
        Read and decode MQTT message fixed header from stream
        :return: FixedHeader instance
        """

        async def decode_remaining_length():
            """
            Decode message length according to MQTT specifications
            :return:
            """
            shift = 0
            value = 0
            buffer = bytearray()
            while True:
                int_byte = (await reader.read(1))[0]
                buffer.append(int_byte)
                value |= (int_byte & 0x7F) << shift
                if int_byte & 0x80:
                    shift += 7
                    if shift > 21:
                        raise MQTTException(
                            "Invalid remaining length bytes:%s, packet_type=%d"
                            % (bytes_to_hex_str(buffer), msg_type)
                        )
                else:
                    break
            return value

        try:
            int1 = (await read_or_raise(reader, 1))[0]
            msg_type = int1 >> 4
            flags = int1 & 0x0F
            remain_length = await decode_remaining_length()

            return cls(msg_type, flags, remain_length)
        except NoDataException:
            return None

    def __repr__(self):
        return type(self).__name__ + "(length={0}, flags={1})".format(
            self.remaining_length, hex(self.flags)
        )


class MQTTVariableHeader:
    def __init__(self):
        pass

    async def to_stream(self, writer: anyio.abc.ByteStream):
        await writer.write(self.to_bytes())

    def to_bytes(self) -> bytes:
        """
        Serialize header data to a byte array conforming to MQTT protocol
        :return: serialized data
        """

    @property
    def bytes_length(self):
        return len(self.to_bytes())

    @classmethod
    async def from_stream(cls, reader: StreamAdapter, fixed_header: MQTTFixedHeader):
        pass


class PacketIdVariableHeader(MQTTVariableHeader):

    __slots__ = ("packet_id",)

    def __init__(self, packet_id):
        super().__init__()
        self.packet_id = packet_id

    def to_bytes(self):
        out = b""
        out += int_to_bytes(self.packet_id, 2)
        return out

    @classmethod
    async def from_stream(cls, reader: StreamAdapter, fixed_header: MQTTFixedHeader):
        packet_id = await decode_packet_id(reader)
        return cls(packet_id)

    def __repr__(self):
        return type(self).__name__ + "(packet_id={0})".format(self.packet_id)


class MQTTPayload:
    def __init__(self):
        pass

    def to_bytes(self, fixed_header: MQTTFixedHeader, variable_header: MQTTVariableHeader):
        raise NotImplementedError()

    @classmethod
    async def from_stream(
        cls,
        reader: anyio.abc.ByteStream,
        fixed_header: MQTTFixedHeader,
        variable_header: MQTTVariableHeader,
    ):
        pass


class MQTTPacket:

    __slots__ = ("fixed_header", "variable_header", "payload", "protocol_ts")

    FIXED_HEADER = MQTTFixedHeader
    VARIABLE_HEADER = None
    PAYLOAD = None

    def __init__(
        self,
        fixed: MQTTFixedHeader,
        variable_header: MQTTVariableHeader = None,
        payload: MQTTPayload = None,
    ):
        self.fixed_header = fixed
        self.variable_header = variable_header
        self.payload = payload
        self.protocol_ts = None

    async def to_stream(self, writer: anyio.abc.ByteStream):
        await writer.write(self.to_bytes())
        self.protocol_ts = datetime.now()

    def to_bytes(self) -> bytes:
        if self.variable_header:
            variable_header_bytes = self.variable_header.to_bytes()
        else:
            variable_header_bytes = b""
        if self.payload:
            payload_bytes = self.payload.to_bytes(self.fixed_header, self.variable_header)
        else:
            payload_bytes = b""

        self.fixed_header.remaining_length = len(variable_header_bytes) + len(payload_bytes)
        fixed_header_bytes = self.fixed_header.to_bytes()

        return fixed_header_bytes + variable_header_bytes + payload_bytes

    @classmethod
    async def from_stream(cls, reader: StreamAdapter, fixed_header=None, variable_header=None):
        if fixed_header is None:
            fixed_header = await cls.FIXED_HEADER.from_stream(reader)
        if variable_header is None and cls.VARIABLE_HEADER:
            variable_header = await cls.VARIABLE_HEADER.from_stream(reader, fixed_header)
        if cls.PAYLOAD:
            payload = await cls.PAYLOAD.from_stream(reader, fixed_header, variable_header)
        else:
            payload = None

        if payload:
            instance = cls(fixed_header, variable_header, payload)
        elif variable_header:
            instance = cls(fixed_header, variable_header)
        else:
            instance = cls(fixed_header)
        instance.protocol_ts = datetime.now()
        return instance

    @property
    def bytes_length(self):
        return len(self.to_bytes())

    def __repr__(self):
        return type(
            self
        ).__name__ + "(ts={0!s}, fixed={1!r}, variable={2!r}, payload={3!r})".format(
            self.protocol_ts, self.fixed_header, self.variable_header, self.payload
        )
