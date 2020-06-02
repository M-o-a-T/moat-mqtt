# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.

import anyio
import logging
import ssl
import copy
from urllib.parse import urlparse, urlunparse
from functools import wraps

try:
    from contextlib import asynccontextmanager
except ImportError:
    from async_generator import asynccontextmanager
from wsproto.utilities import ProtocolError
from asyncwebsockets import create_websocket

from distmqtt.utils import match_topic
from distmqtt.session import Session
from distmqtt.errors import NoDataException
from distmqtt.mqtt.connack import CONNECTION_ACCEPTED, CLIENT_ERROR
from distmqtt.mqtt.protocol.client_handler import ClientProtocolHandler
from distmqtt.adapters import StreamAdapter, WebSocketsAdapter
from distmqtt.plugins.manager import PluginManager, BaseContext
from distmqtt.mqtt.protocol.handler import ProtocolHandlerException
from distmqtt.mqtt.constants import QOS_0, QOS_1, QOS_2
from distmqtt import codecs


_defaults = {
    "keep_alive": 10,
    "ping_delay": 1,
    "default_qos": 0,
    "default_retain": False,
    "auto_reconnect": True,
    "reconnect_max_interval": 10,
    "reconnect_retries": 2,
    "codec": "noop",
}

_codecs = {}
for _t in dir(codecs):
    _c = getattr(codecs, _t)
    if isinstance(_c, type) and issubclass(_c, codecs.BaseCodec):
        try:
            _codecs[_c.name] = _c
        except AttributeError:
            pass


def get_codec(
    codec, fallback=None, config={}
):  # pylint: disable=dangerous-default-value
    if codec is None:
        codec = fallback
    if codec is None:
        codec = config["codec"]
    if isinstance(codec, str):
        codec = config.get("codec_params", {}).get("name", codec)
        codec = _codecs[codec]
    if isinstance(codec, type):
        codec = codec(**config.get("codec_params", {}).get(codec.name, {}))
    return codec


_handler_id = 0


class _set(set):
    topic = None
    qos = None


class ClientException(Exception):
    pass


class ConnectException(ClientException):
    def __init__(self, text=None, return_code=CLIENT_ERROR):
        if text is None:
            text = self.__class__.__name__
        super().__init__(text)
        self.return_code = return_code

    pass


class ClientContext(BaseContext):
    """
        ClientContext is used as the context passed to plugins interacting with the client.
        It act as an adapter to client services from plugins
    """

    def __init__(self, config):
        super().__init__()
        self.config = config


base_logger = logging.getLogger(__name__)


def mqtt_connected(func):
    """
        MQTTClient coroutines decorator which will wait until connection before calling the decorated method.
        :param func: coroutine to be called once connected
        :return: coroutine result
    """

    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        if not self._connected_state.is_set():
            base_logger.warning("Client not connected, waiting for it")
            async with anyio.create_task_group() as tg:

                async def wait_no_more():
                    await self._no_more_connections.wait()
                    raise ClientException("Will not reconnect")

                await tg.spawn(wait_no_more)

                await self._connected_state.wait()
                await tg.cancel_scope.cancel()
        return await func(self, *args, **kwargs)

    return wrapper


@asynccontextmanager
async def open_mqttclient(uri=None, client_id=None, config={}, codec=None):
    # pylint: disable=dangerous-default-value
    """
        MQTT client implementation.

        MQTTClient instances provides API for connecting to a broker and send/receive messages using the MQTT protocol.

        :param client_id: MQTT client ID to use when connecting to the broker. If none, it will generated randomly by :func:`distmqtt.utils.gen_client_id`
        :param config: Client configuration
        :param codec: Codec to default to, the config or "no-op" if not given.
        :return: async context manager returning a class instance

        Example usage::

            async with open_mqttclient(config=dict(uri="mqtt://my-broker.example")) as client:
                # await client.connect("mqtt://my-broker.example")  # alternate use
                await C.subscribe([
                        ('$SYS/broker/uptime', QOS_1),
                        ('$SYS/broker/load/#', QOS_2),
                    ])
                async for msg in client:
                    packet = message.publish_packet
                    print("%d:  %s => %s" % (i, packet.variable_header.topic_name, str(packet.payload.data)))

    """
    async with anyio.create_task_group() as tg:
        C = MQTTClient(tg, client_id, config=config, codec=codec)
        try:
            if uri is not None:
                config["uri"] = uri
            if "uri" in config:
                await C.connect(**config)
            yield C
        finally:
            async with anyio.fail_after(2, shield=True):
                await C.disconnect()
                await tg.cancel_scope.cancel()


class MQTTClient:
    """
        MQTT client implementation.

        MQTTClient instances provides API for connecting to a broker and send/receive messages using the MQTT protocol.

        :param tg: The task group in which to run open-ended subtasks.
        :param client_id: MQTT client ID to use when connecting to the broker. If none, it will generated randomly by :func:`distmqtt.utils.gen_client_id`
        :param config: Client configuration
        :param codec: Codec to default to, the config or "no-op" if not given.
        :return: class instance

        You should use :func:`open_mqttclient` to create an instance of
        this class.
    """

    def __init__(
        self, tg: anyio.abc.TaskGroup, client_id=None, config=None, codec=None
    ):
        self.logger = logging.getLogger(__name__)
        self.config = copy.deepcopy(_defaults)
        if config is not None:
            self.config.update(config)
        if client_id is not None:
            self.client_id = client_id
        else:
            from distmqtt.utils import gen_client_id

            self.client_id = gen_client_id()
            self.logger.debug("Using generated client ID : %s", self.client_id)

        self.session = None
        self._tg = tg
        self._handler = None
        self._disconnect_task = None
        self._connected_state = anyio.create_event()
        self._no_more_connections = anyio.create_event()
        self.extra_headers = {}
        self.codec = get_codec(codec, config=self.config)

        self._subscriptions = None

        # Init plugins manager
        context = ClientContext(self.config)
        self.plugins_manager = PluginManager(tg, "distmqtt.client.plugins", context)
        self.client_task = None

    async def connect(
        self,
        uri=None,
        cleansession=None,
        cafile=None,
        capath=None,
        cadata=None,
        extra_headers={},
    ):
        # pylint: disable=dangerous-default-value
        """
            Connect to a remote broker.

            At first, a network connection is established with the server using the given protocol (``mqtt``, ``mqtts``, ``ws`` or ``wss``). Once the socket is connected, a `CONNECT <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718028>`_ message is sent with the requested informations.

            This method is a *coroutine*.

            :param uri: Broker URI connection, conforming to `MQTT URI scheme <https://github.com/mqtt/mqtt.github.io/wiki/URI-Scheme>`_. Uses ``uri`` config attribute by default.
            :param cleansession: MQTT CONNECT clean session flag
            :param cafile: server certificate authority file (optional, used for secured connection)
            :param capath: server certificate authority path (optional, used for secured connection)
            :param cadata: server certificate authority data (optional, used for secured connection)
            :param extra_headers: a dictionary with additional http headers that should be sent on the initial connection (optional, used only with websocket connections)
            :return: `CONNACK <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718033>`_ return code
            :raise: :class:`distmqtt.client.ConnectException` if connection fails
        """
        self.session = self._initsession(uri, cleansession, cafile, capath, cadata)
        self.extra_headers = extra_headers
        self.logger.debug("Connect to: %s", uri)

        try:
            return await self._do_connect()
        except SyntaxError as be:
            self.logger.warning("Connection failed: %r", be)
            auto_reconnect = self.config.get("auto_reconnect", False)
            if not auto_reconnect:
                raise
            else:
                return await self.reconnect()

    async def disconnect(self):
        """
            Disconnect from the connected broker.

            This method sends a `DISCONNECT <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718090>`_ message and closes the network socket.

            This method is a *coroutine*.
        """

        # do not reconnect any more
        self.config["auto_reconnect"] = False
        await self.cancel_tasks()

        if self.session is not None and self.session.transitions.is_connected():
            if self._disconnect_task is not None:
                await self._disconnect_task.cancel()
            await self._handler.mqtt_disconnect()
            self._connected_state.clear()
            await self._handler.stop()
            self.session.transitions.disconnect()
        else:
            self.logger.warning(
                "Client session is not currently connected, ignoring call"
            )

    async def cancel_tasks(self):
        """
        Before disconnection need to cancel all pending tasks
        :return:
        """
        if self.client_task is not None:
            task, self.client_task = self.client_task, None
            await task.cancel()

    async def reconnect(self, cleansession=None):
        """
            Reconnect a previously connected broker.

            Reconnection tries to establish a network connection and send a `CONNECT <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718028>`_ message.
            Retries interval and attempts can be controled with the ``reconnect_max_interval`` and ``reconnect_retries`` configuration parameters.

            This method is a *coroutine*.

            :param cleansession: clean session flag used in MQTT CONNECT messages sent for reconnections.
            :return: `CONNACK <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718033>`_ return code
            :raise: :class:`distmqtt.client.ConnectException` if re-connection fails after max retries.
        """

        if self.session.transitions.is_connected():
            self.logger.warning("Client already connected")
            return CONNECTION_ACCEPTED

        if cleansession:
            self.session.clean_session = cleansession
        self.logger.debug("Reconnecting with session parameters: %r", self.session)
        reconnect_max_interval = self.config.get("reconnect_max_interval", 10)
        reconnect_retries = self.config.get("reconnect_retries", 5)
        nb_attempt = 1
        await anyio.sleep(1)
        while True:
            try:
                self.logger.debug("Reconnect attempt %d ...", nb_attempt)
                return await self._do_connect()
            except SyntaxError as e:  # no you can't reconnect on every exception
                self.logger.warning("Reconnection attempt failed: %r", e)
                if reconnect_retries >= 0 and nb_attempt > reconnect_retries:
                    self.logger.error(
                        "Maximum number of connection attempts reached. Reconnection aborted"
                    )
                    raise ConnectException("Too many connection attempts failed")
                exp = 2 ** nb_attempt
                delay = exp if exp < reconnect_max_interval else reconnect_max_interval
                self.logger.debug("Waiting %d second before next attempt", delay)
                await anyio.sleep(delay)
                nb_attempt += 1

    async def _do_connect(self):
        return_code = await self._connect_coro()
        evt = anyio.create_event()
        await self._tg.spawn(self.handle_connection_close, evt)
        await evt.wait()
        return return_code

    @mqtt_connected
    async def ping(self):
        """
            Ping the broker.

            Send a MQTT `PINGREQ <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718081>`_ message for response.

            This method is a *coroutine*.
        """

        if self.session.transitions.is_connected():
            await self._handler.mqtt_ping()
        else:
            self.logger.warning(
                "MQTT PING request incompatible with current session state '%s'",
                self.session.transitions.state,
            )

    @mqtt_connected
    async def publish(self, topic, message, qos=None, retain=None, codec=None):
        """
            Publish a message to the broker.

            Send a MQTT `PUBLISH <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718037>`_ message and wait for acknowledgment depending on Quality Of Service

            This method is a *coroutine*.

            :param topic: topic name to which message data is published
            :param message: payload message (as bytes) to send.
            :param qos: requested publish quality of service : QOS_0, QOS_1 or QOS_2. Defaults to ``default_qos`` config parameter or QOS_0.
            :param retain: retain flag. Defaults to ``default_retain`` config parameter or False.
            :param codec: Codec to encode the message with. Defaults to the connection's.
        """

        codec = get_codec(codec, self.codec, config=self.config)
        message = codec.encode(message)

        if qos is not None:
            assert qos in (QOS_0, QOS_1, QOS_2)
        else:
            try:
                qos = self.config["topics"][topic]["qos"]
            except KeyError:
                qos = self.config["default_qos"]
        if retain is None:
            try:
                retain = self.config["topics"][topic]["retain"]
            except KeyError:
                retain = self.config["default_retain"]
        return await self._handler.mqtt_publish(topic, message, qos, retain)

    @mqtt_connected
    async def subscribe(self, topics):
        """
            Subscribe to some topics.

            Send a MQTT `SUBSCRIBE <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718063>`_ message and wait for broker acknowledgment.

            This method is a *coroutine*.

            :param topics: array of topics pattern to subscribe with associated QoS.
            :return: `SUBACK <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718068>`_ message return code.

            Example of ``topics`` argument expected structure:
            ::

                [
                    ('$SYS/broker/uptime', QOS_1),
                    ('$SYS/broker/load/#', QOS_2),
                ]
        """
        return await self._handler.mqtt_subscribe(topics, self.session.next_packet_id)

    @mqtt_connected
    async def unsubscribe(self, topics):
        """
            Unsubscribe from some topics.

            Send a MQTT `UNSUBSCRIBE <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718072>`_ message and wait for broker `UNSUBACK <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718077>`_ message.

            This method is a *coroutine*.

            :param topics: array of topics to unsubscribe from.

            Example of ``topics`` argument expected structure:
            ::

                ['$SYS/broker/uptime', '$SYS/broker/load/#']
        """
        await self._handler.mqtt_unsubscribe(topics, self.session.next_packet_id)

    def subscription(self, topic, qos=QOS_0, codec=None):
        """
        Iterate over a message.

            :param topic: topic (including wildcards) to receive from.
            :param qos: minimum QOS to receive with.
            :param codec: Codec to decode incoming mssages with.

            Usage::

                async with client.subscription("/foo/bar") as sub:
                    async for msg in sub:
                        await process(msg)

        You can use either multiple calls to `subscription`, or manage
        message dispatching your self with `subscribe` and `deliver_message`.
        Using both methods in parallel are not supported.

        """

        class _Subscription:
            def __init__(self, client, topic, qos, codec=None):
                self.client = client
                self.topic = topic
                self.qos = qos
                if codec is None:
                    codec = client.codec
                elif isinstance(codec, str):
                    codec = _codecs[codec]()
                self.codec = codec
                self._q = None

                global _handler_id
                _handler_id += 1
                self._id = _handler_id

            def __hash__(self):
                return self._id

            def __eq__(self, other):
                return self._id == getattr(other, "_id", other)

            async def __aenter__(self):
                self._q = anyio.create_queue(100)
                await self.client._subscribe(self)
                return self

            async def __aexit__(self, *tb):
                self._q = None
                try:
                    async with anyio.move_on_after(2, shield=True):
                        await self.client._unsubscribe(self)
                except ClientException:
                    pass

            def __aiter__(self):
                return self

            async def __anext__(self):
                if self._q is None:
                    raise RuntimeError("Overflow. Please resubscribe.")
                message = await self._q.get()
                message.data = self.codec.decode(message.publish_packet.data)
                return message

            async def publish(self, topic, message, *a, **kw):
                """
                    Publish a message.

                    :param topic: topic name to which message data is published
                    :param message: payload message (as bytes) to send.
                    :param qos: requested publish quality of service : QOS_0, QOS_1 or QOS_2. Defaults to ``default_qos`` config parameter or QOS_0.
                    :param retain: retain flag. Defaults to ``default_retain`` config parameter or False.
                    :param codec: Codec to encode the message with. Defaults to the subscription's.
                """
                if topic is None:
                    topic = self.topic
                if "codec" not in kw:
                    kw["codec"] = self.codec
                await self.client.publish(topic, message, *a, **kw)

        return _Subscription(self, topic, qos, codec=codec)

    async def _subscribe(self, handler):
        topic = handler.topic
        if self._subscriptions is None:
            self._subscriptions = dict()
            await self._tg.spawn(self._deliver_loop)
        clients = self._subscriptions.get(topic, None)
        if clients is None:
            self._subscriptions[topic] = clients = _set()
            clients.topic = tuple(topic.split("/"))
            clients.qos = handler.qos
            await self.subscribe([(topic, handler.qos)])
        elif clients.qos < handler.qos:
            clients.qos = handler.qos
            await self.subscribe([(topic, handler.qos)])

        clients.add(handler)

    async def _unsubscribe(self, handler):
        topic = handler.topic
        clients = self._subscriptions[topic]
        clients.remove(handler)
        if not clients:
            del self._subscriptions[topic]
            await self.unsubscribe([topic])

    async def _dispatch(self, msg):
        t = msg.topic.split("/")
        for clients in list(self._subscriptions.values()):
            if not match_topic(t, clients.topic):
                continue
            for c in list(clients):
                if c._q is None:
                    continue
                try:
                    async with anyio.fail_after(0.01):
                        await c._q.put(msg)
                except TimeoutError:
                    c._q = None

    async def _deliver_loop(self):
        """
            Dispatch incoming messages to subscriptions.
        """
        async with anyio.open_cancel_scope() as scope:
            if self.client_task is not None:
                raise RuntimeError("You can't listen in more than one task")
            self.client_task = scope
            try:
                while True:
                    if self.session is None:
                        return
                    msg = await self.session.get_next_message()
                    if msg is None:
                        return
                    await self._dispatch(msg)

            finally:
                self.client_task = None

    @mqtt_connected
    async def deliver_message(self, codec=None):
        """
            Deliver next received message.

            Deliver next message received from the broker. If no message is available, this methods waits until next message arrives or ``timeout`` occurs.

            This method is a *coroutine*.

            :return: instance of :class:`distmqtt.session.ApplicationMessage` containing received message information flow.
            :raises: :class:`TimeoutError` if timeout occurs before a message is delivered

            :param codec: Codec to decode the message with.

            This method returns ``None`` if it is cancelled by closing the
            connection.
        """
        if codec is None:
            codec = self.codec
        elif isinstance(codec, str):
            codec = _codecs[codec]()

        async with anyio.open_cancel_scope() as scope:
            if self.session is None:
                return None
            if self.client_task is not None:
                raise RuntimeError("You can't listen in more than one task")
            self.client_task = scope
            try:
                msg = await self.session.get_next_message()
                msg.data = codec.decode(msg.publish_packet.data)
                return msg

            finally:
                self.client_task = None

    async def _connect_coro(self):
        kwargs = dict()

        # Decode URI attributes
        uri_attributes = urlparse(self.session.broker_uri)
        scheme = uri_attributes.scheme
        secure = True if scheme in ("mqtts", "wss") else False
        self.session.username = (
            self.session.username if self.session.username else uri_attributes.username
        )
        self.session.password = (
            self.session.password if self.session.password else uri_attributes.password
        )
        self.session.remote_address = uri_attributes.hostname
        self.session.remote_port = uri_attributes.port
        if scheme in ("mqtt", "mqtts") and not self.session.remote_port:
            self.session.remote_port = 8883 if scheme == "mqtts" else 1883
        if scheme in ("ws", "wss") and not self.session.remote_port:
            self.session.remote_port = 443 if scheme == "wss" else 80
        if scheme in ("ws", "wss"):
            # Rewrite URI to conform to https://tools.ietf.org/html/rfc6455#section-3
            uri = (
                scheme,
                self.session.remote_address + ":" + str(self.session.remote_port),
                uri_attributes[2],
                uri_attributes[3],
                uri_attributes[4],
                uri_attributes[5],
            )
            self.session.broker_uri = urlunparse(uri)
        # Init protocol handler
        # if not self._handler:
        self._handler = ClientProtocolHandler(self.plugins_manager)

        if secure:
            sc = ssl.create_default_context(
                ssl.Purpose.SERVER_AUTH,
                cafile=self.session.cafile,
                capath=self.session.capath,
                cadata=self.session.cadata,
            )
            if "certfile" in self.config and "keyfile" in self.config:
                sc.load_cert_chain(self.config["certfile"], self.config["keyfile"])
            if "check_hostname" in self.config and isinstance(
                self.config["check_hostname"], bool
            ):
                sc.check_hostname = self.config["check_hostname"]
            kwargs["ssl_context"] = sc
            kwargs["autostart_tls"] = True

        try:
            adapter = None
            self._connected_state.clear()
            # Open connection
            if scheme in ("mqtt", "mqtts"):
                conn = await anyio.connect_tcp(
                    self.session.remote_address, self.session.remote_port, **kwargs
                )
                if secure:
                    await conn.start_tls()
                adapter = StreamAdapter(conn)
            elif scheme in ("ws", "wss"):
                if kwargs.pop("autostart_tls", False):
                    kwargs["ssl"] = kwargs.pop("ssl_context")
                websocket = await create_websocket(
                    self.session.broker_uri,
                    subprotocols=["mqtt"],
                    headers=self.extra_headers,
                    **kwargs
                )
                adapter = WebSocketsAdapter(websocket)
            # Start MQTT protocol
            await self._handler.attach(self.session, adapter)
            try:
                return_code = await self._handler.mqtt_connect()
            except NoDataException:
                self.logger.warning("Connection broken by broker")
                exc = ConnectException("Connection broken by broker")
                raise exc
            if return_code is not CONNECTION_ACCEPTED:
                self.session.transitions.disconnect()
                self.logger.warning("Connection rejected with code '%s'", return_code)
                exc = ConnectException("Connection rejected by broker", return_code)
                raise exc
            else:
                # Handle MQTT protocol
                await self._handler.start()
                self.session.transitions.connect()
                await self._connected_state.set()
                self.logger.debug(
                    "connected to %s:%s",
                    self.session.remote_address,
                    self.session.remote_port,
                )
            return return_code
        except ProtocolError as exc:
            self.logger.warning("connection failed: invalid websocket handshake")
            self.session.transitions.disconnect()
            raise ConnectException(
                "connection failed: invalid websocket handshake"
            ) from exc
        except (ProtocolHandlerException, ConnectionError, OSError) as exc:
            self.logger.warning("MQTT connection failed")
            self.session.transitions.disconnect()
            raise ConnectException from exc

    async def handle_connection_close(self, evt):
        async def cancel_tasks():
            await self._no_more_connections.set()
            if self.client_task:
                task, self.client_task = self.client_task, None
                await task.cancel()

        async with anyio.open_cancel_scope() as scope:
            self._disconnect_task = scope
            await evt.set()
            self.logger.debug("Wait for broker disconnection")
            # Wait for disconnection from broker (like connection lost)
            await self._handler.wait_disconnect()
        self.logger.warning("Disconnected from broker")

        # Block client API
        self._connected_state.clear()

        # stop and clean handler
        await self._handler.stop()
        await self._handler.detach()
        self.session.transitions.disconnect()

        if self.config.get("auto_reconnect", False):
            # Try reconnection
            self.logger.debug("Auto-reconnecting")
            try:
                await self.reconnect()
            except ConnectException:
                # Cancel client pending tasks
                await cancel_tasks()
        else:
            # Cancel client pending tasks
            await cancel_tasks()

    def _initsession(
        self, uri=None, cleansession=None, cafile=None, capath=None, cadata=None
    ) -> Session:
        # Load config
        broker_conf = self.config.get("broker", dict()).copy()
        if uri:
            broker_conf["uri"] = uri
        if cafile:
            broker_conf["cafile"] = cafile
        elif "cafile" not in broker_conf:
            broker_conf["cafile"] = None
        if capath:
            broker_conf["capath"] = capath
        elif "capath" not in broker_conf:
            broker_conf["capath"] = None
        if cadata:
            broker_conf["cadata"] = cadata
        elif "cadata" not in broker_conf:
            broker_conf["cadata"] = None

        if cleansession is not None:
            broker_conf["cleansession"] = cleansession

        for key in ["uri"]:
            if broker_conf.get(key, None) is None:
                raise ClientException("Missing connection parameter '%s'" % key)

        s = Session(self.plugins_manager)
        s.broker_uri = broker_conf["uri"]
        s.client_id = self.client_id
        s.cafile = broker_conf["cafile"]
        s.capath = broker_conf["capath"]
        s.cadata = broker_conf["cadata"]
        if cleansession is not None:
            s.clean_session = cleansession
        else:
            s.clean_session = self.config.get("cleansession", True)
        s.keep_alive = self.config["keep_alive"] - self.config["ping_delay"]
        if "will" in self.config:
            s.will_flag = True
            s.will_retain = self.config["will"]["retain"]
            s.will_topic = self.config["will"]["topic"]
            s.will_message = self.config["will"]["message"]
            s.will_qos = self.config["will"]["qos"]
        else:
            s.will_flag = False
            s.will_retain = False
            s.will_topic = None
            s.will_message = None
        return s
