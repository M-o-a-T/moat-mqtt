# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import io
from asyncwebsockets import Websocket
from wsproto.events import CloseConnection, BytesMessage
import anyio
import logging


class ReaderAdapter:
    """
    Base class for all network protocol reader adapter.

    Reader adapters are used to adapt read operations on the network depending on the protocol used
    """

    async def read(self, n=-1) -> bytes:
        """
        Read up to n bytes. If n is not provided, or set to -1, read until EOF and return all read bytes.
        If the EOF was received and the internal buffer is empty, return an empty bytes object.
        :return: packet read as bytes data
        """

    def feed_eof(self):
        """
        Acknowleddge EOF
        """


class WriterAdapter:
    """
    Base class for all network protocol writer adapter.

    Writer adapters are used to adapt write operations on the network depending on the protocol used
    """

    def write(self, data):
        """
        write some data to the protocol layer
        """

    def get_peer_info(self):
        """
        Return peer socket info (remote address and remote port as tuple
        """

    async def close(self):
        """
        Close the protocol connection
        """


class WebSocketsReader(ReaderAdapter):
    """
    WebSockets API reader adapter
    """
    def __init__(self, protocol: Websocket):
        self._protocol = protocol
        self._stream = io.BytesIO(b'')

    async def read(self, n=-1) -> bytes:
        await self._feed_buffer(n)
        data = self._stream.read(n)
        return data

    async def _feed_buffer(self, n=1):
        """
        Feed the data buffer by reading a Websocket message.
        :param n: if given, feed buffer until it contains at least n bytes
        """
        buffer = bytearray(self._stream.read())
        while len(buffer) < n:
            try:
                message = await self._protocol._next_event()
            except anyio.exceptions.ClosedResourceError:
                message = None
            if isinstance(message, CloseConnection):
                message = None
            if message is None:
                self._stream = None
                break
            if not isinstance(message, BytesMessage):
                raise TypeError("message must be bytes")
            buffer.extend(message.data)
        self._stream = io.BytesIO(buffer)


class WebSocketsWriter(WriterAdapter):
    """
    WebSockets API writer adapter
    """
    def __init__(self, protocol: Websocket):
        self._protocol = protocol

    async def write(self, data):
        """
        write some data to the protocol layer
        """
        await self._protocol.send(data)

    def get_peer_info(self):
        sock = self._protocol._sock._socket._raw_socket
        extra_info = sock.getpeername()
        return extra_info[0], extra_info[1]

    async def close(self):
        await self._protocol.close()


class StreamReaderAdapter(ReaderAdapter):
    """
    Asyncio Streams API protocol adapter
    This adapter relies on anyio.Stream to read from a TCP socket.
    Because API is very close, this class is trivial
    """
    def __init__(self, reader: anyio.abc.Stream):
        self._reader = reader

    async def read(self, n=-1) -> bytes:
        if n == -1:
            data = await self._reader.receive_some(4096)
        else:
            data = await self._reader.receive_exactly(n)
        return data


class StreamWriterAdapter(WriterAdapter):
    """
    Asyncio Streams API protocol adapter
    This adapter relies on StreamWriter to write to a TCP socket.
    Because API is very close, this class is trivial
    """
    def __init__(self, writer: anyio.abc.Stream):
        self.logger = logging.getLogger(__name__)
        self._writer = writer

    async def write(self, data):
        await self._writer.send_all(data)

    def get_peer_info(self):
        sock = self._writer._socket._raw_socket
        extra_info = sock.getpeername()
        return extra_info[0], extra_info[1]

    async def close(self):
        await self._writer.close()


class BufferReader(ReaderAdapter):
    """
    Byte Buffer reader adapter
    This adapter simply adapt reading a byte buffer.
    """
    def __init__(self, buffer: bytes):
        self._stream = io.BytesIO(buffer)

    async def read(self, n=-1) -> bytes:
        return self._stream.read(n)


class BufferWriter(WriterAdapter):
    """
    ByteBuffer writer adapter
    This adapter simply adapt writing to a byte buffer
    """
    def __init__(self, buffer=b''):
        self._stream = io.BytesIO(buffer)

    async def write(self, data):
        """
        write some data to the protocol layer
        """
        await self._stream.write(data)

    def get_buffer(self):
        return self._stream.getvalue()

    def get_peer_info(self):
        return "BufferWriter", 0

    async def close(self):
        self._stream.close()
