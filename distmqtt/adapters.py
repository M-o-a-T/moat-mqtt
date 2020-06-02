# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import io
from asyncwebsockets import Websocket
from wsproto.events import CloseConnection, BytesMessage
import anyio
import logging


class BaseAdapter:
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


class WebSocketsAdapter(BaseAdapter):
    """
    WebSockets API reader adapter
    """

    def __init__(self, websocket: Websocket):
        self._websocket = websocket
        self._buffer = io.BytesIO(b"")

    async def read(self, n=-1) -> bytes:
        await self._feed_buffer(n)
        data = self._buffer.read(n)
        return data

    async def _feed_buffer(self, n=1):
        """
        Feed the data buffer by reading a Websocket message.
        :param n: if given, feed buffer until it contains at least n bytes
        """
        buffer = bytearray(self._buffer.read())
        while len(buffer) < n:
            try:
                message = await self._websocket._next_event()
            except anyio.exceptions.ClosedResourceError:
                message = None
            if isinstance(message, CloseConnection):
                message = None
            if message is None:
                self._buffer = None
                break
            if not isinstance(message, BytesMessage):
                raise TypeError("message must be bytes")
            buffer.extend(message.data)
        self._buffer = io.BytesIO(buffer)

    async def write(self, data):
        """
        write some data to the protocol layer
        """
        await self._websocket.send(data)

    def get_peer_info(self):
        sock = self._websocket._sock._socket._raw_socket
        extra_info = sock.getpeername()
        return extra_info[0], extra_info[1]

    async def close(self):
        await self._websocket.close()


class StreamAdapter(BaseAdapter):
    """
    Asyncio Streams API protocol adapter
    This adapter relies on anyio.Stream to read from a TCP socket.
    Because API is very close, this class is trivial
    """

    def __init__(self, stream: anyio.abc.Stream):
        self.logger = logging.getLogger(__name__)
        self._stream = stream

    async def read(self, n=-1) -> bytes:
        if n == -1:
            data = await self._stream.receive_some(4096)
        else:
            data = await self._stream.receive_exactly(n)
        return data

    async def write(self, data):
        await self._stream.send_all(data)

    def get_peer_info(self):
        sock = self._stream._socket._raw_socket
        extra_info = sock.getpeername()
        return extra_info[0], extra_info[1]

    async def close(self):
        await self._stream.close()


class BufferAdapter(BaseAdapter):
    """
    Byte Buffer reader adapter
    This adapter simply adapt reading a byte buffer.
    """

    def __init__(self, buffer: bytes):
        self._rstream = io.BytesIO(buffer)
        self._wstream = io.BytesIO(b"")

    async def read(self, n=-1) -> bytes:
        return self._rstream.read(n)

    async def write(self, data):
        """
        write some data to the protocol layer
        """
        await self._wstream.write(data)

    def get_buffer(self):
        return self._wstream.getvalue()

    def get_peer_info(self):
        return "BufferWriter", 0

    async def close(self):
        self._rstream.close()
        self._wstream.close()
