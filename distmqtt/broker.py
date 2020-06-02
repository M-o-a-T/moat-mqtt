# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import logging
import ssl
import anyio
from copy import deepcopy
from collections import deque

try:
    from contextlib import asynccontextmanager
except ImportError:
    from async_generator import asynccontextmanager

from functools import partial
from transitions import Machine, MachineError
from distmqtt.session import Session, EVENT_BROKER_MESSAGE_RECEIVED  # noqa: F401

# EVENT_BROKER_MESSAGE_RECEIVED is re-exported
from distmqtt.mqtt.protocol.broker_handler import BrokerProtocolHandler
from distmqtt.errors import DistMQTTException, MQTTException
from distmqtt.utils import format_client_message, gen_client_id, Future, match_topic
from distmqtt.mqtt.constants import QOS_0
from distmqtt.adapters import StreamAdapter, BaseAdapter, WebSocketsAdapter
from .plugins.manager import PluginManager, BaseContext
from asyncwebsockets import create_websocket_server


_defaults = {
    "listeners": {"default": {"type": "tcp", "bind": "0.0.0.0:1883"}},
    "timeout-disconnect-delay": 2,
    "auth": {"allow-anonymous": True, "password-file": None},
}

EVENT_BROKER_PRE_START = "broker_pre_start"
EVENT_BROKER_POST_START = "broker_post_start"
EVENT_BROKER_PRE_SHUTDOWN = "broker_pre_shutdown"
EVENT_BROKER_POST_SHUTDOWN = "broker_post_shutdown"
EVENT_BROKER_CLIENT_CONNECTED = "broker_client_connected"
EVENT_BROKER_CLIENT_DISCONNECTED = "broker_client_disconnected"
EVENT_BROKER_CLIENT_SUBSCRIBED = "broker_client_subscribed"
EVENT_BROKER_CLIENT_UNSUBSCRIBED = "broker_client_unsubscribed"


class BrokerException(Exception):
    pass


class RetainedApplicationMessage:

    __slots__ = ("source_session", "topic", "data", "qos")

    def __init__(self, source_session, topic, data, qos=None):
        self.source_session = source_session
        self.topic = topic
        self.data = data
        self.qos = qos


class Server:
    def __init__(self, listener_name, server_instance, max_connections=-1):
        self.logger = logging.getLogger(__name__)
        self.instance = server_instance
        self.conn_count = 0
        self.listener_name = listener_name

        self.max_connections = max_connections
        if self.max_connections > 0:
            self.semaphore = anyio.create_semaphore(self.max_connections)
        else:
            self.semaphore = None

    @asynccontextmanager
    async def _client_limit_(self):
        if self.semaphore:
            async with self.semaphore:
                yield self
        else:
            yield self

    @asynccontextmanager
    async def _client_limit(self):
        async with self._client_limit_():

            self.conn_count += 1
            if self.max_connections > 0:
                self.logger.info(
                    "Listener '%s': %d/%d connections acquired",
                    self.listener_name,
                    self.conn_count,
                    self.max_connections,
                )
            else:
                self.logger.info(
                    "Listener '%s': %d connections acquired",
                    self.listener_name,
                    self.conn_count,
                )
            yield self

            self.conn_count -= 1
            if self.max_connections > 0:
                self.logger.info(
                    "Listener '%s': %d/%d connections acquired",
                    self.listener_name,
                    self.conn_count,
                    self.max_connections,
                )
            else:
                self.logger.info(
                    "Listener '%s': %d connections acquired",
                    self.listener_name,
                    self.conn_count,
                )

    async def close_instance(self):
        await self.instance.cancel()


class BrokerContext(BaseContext):
    """
    BrokerContext is used as the context passed to plugins interacting with the broker.
    It act as an adapter to broker services from plugins developed for DistMQTT broker
    """

    def __init__(self, broker, config):
        super().__init__()
        self._broker_instance = broker
        self.config = config

    async def broadcast_message(self, topic, data, qos=None, retain=False):
        await self._broker_instance.internal_message_broadcast(
            topic, data, qos, retain=retain
        )

    @property
    def sessions(self):
        for session in self._broker_instance._sessions.values():
            yield session[0]

    @property
    def retained_messages(self):
        return self._broker_instance._retained_messages

    @property
    def subscriptions(self):
        return self._broker_instance._subscriptions


@asynccontextmanager
async def create_broker(config=None, plugin_namespace=None):
    """MQTT 3.1.1 compliant broker implementation
    :param config: Example Yaml config
    :param plugin_namespace: Plugin namespace to use when loading plugin entry_points. Defaults to ``distmqtt.broker.plugins``

    This is an async context manager::

        async with create_broker() as broker:
            while True:
                anyio.sleep(99999)
    """

    async with anyio.create_task_group() as tg:
        if "distkv" in (config or {}):
            from .distkv_broker import DistKVbroker

            B = DistKVbroker
        else:
            B = Broker
        b = B(tg, config, plugin_namespace)
        try:
            await b.start()
            yield b
        finally:
            async with anyio.fail_after(2, shield=True):
                await b.shutdown()
                await tg.cancel_scope.cancel()


class Broker:
    """
    MQTT 3.1.1 compliant broker implementation

    :param tg: The task group used to run the broker's tasks.
    :param config: Example Yaml config
    :param plugin_namespace: Plugin namespace to use when loading plugin entry_points. Defaults to ``distmqtt.broker.plugins``

    Usage::

        async with anyio.create_task_group() as tg:
            b = Broker(tg, config, plugin_namespace)
            try:
                await b.start()
                pass ## do something with the broker
            finally:
                async with anyio.fail_after(2, shield=True):
                    await b.shutdown()
                    await tg.cancel_scope.cancel()

    Typically, though, you'll want to use :func:`create_broker`, which does
    this for you.
    """

    states = [
        "new",
        "starting",
        "started",
        "not_started",
        "stopping",
        "stopped",
        "not_stopped",
        "stopped",
    ]

    def __init__(self, tg: anyio.abc.TaskGroup, config=None, plugin_namespace=None):
        self.logger = logging.getLogger(__name__)
        self.config = deepcopy(_defaults)
        if config is not None:
            self.config.update(config)
        self._build_listeners_config(self.config)

        self._servers = dict()
        self._init_states()
        self._sessions = dict()
        self._subscriptions = dict()

        self._broadcast_queue = anyio.create_queue(100)
        self._tg = tg
        self._do_retain = self.config.get("retain", True)
        if self._do_retain:
            self._retained_messages = dict()

        # Init plugins manager
        context = BrokerContext(self, self.config)
        if plugin_namespace:
            namespace = plugin_namespace
        else:
            namespace = "distmqtt.broker.plugins"
        self.plugins_manager = PluginManager(tg, namespace, context)

    def _build_listeners_config(self, broker_config):
        self.listeners_config = dict()
        try:
            listeners_config = broker_config["listeners"]
            defaults = listeners_config.get("default", {})
            for listener in listeners_config:
                config = dict(defaults)
                config.update(listeners_config[listener])
                self.listeners_config[listener] = config
        except KeyError as ke:
            raise BrokerException("Listener config not found or invalid") from ke

    def _init_states(self):
        self.transitions = Machine(states=Broker.states, initial="new")
        self.transitions.add_transition(trigger="start", source="new", dest="starting")
        self.transitions.add_transition(
            trigger="starting_fail", source="starting", dest="not_started"
        )
        self.transitions.add_transition(
            trigger="starting_success", source="starting", dest="started"
        )
        self.transitions.add_transition(
            trigger="shutdown", source="starting", dest="not_started"
        )
        self.transitions.add_transition(
            trigger="shutdown", source="started", dest="stopping"
        )
        self.transitions.add_transition(
            trigger="shutdown", source="not_started", dest="stopping"
        )
        self.transitions.add_transition(
            trigger="stopping_success", source="stopped", dest="stopped"
        )
        self.transitions.add_transition(
            trigger="stopping_success", source="not_started", dest="not_started"
        )
        self.transitions.add_transition(
            trigger="stopping_failure", source="stopping", dest="not_stopped"
        )
        self.transitions.add_transition(
            trigger="start", source="stopped", dest="starting"
        )
        self.transitions.add_transition(
            trigger="shutdown", source="new", dest="stopped"
        )
        self.transitions.add_transition(
            trigger="stopping_success", source="stopping", dest="stopped"
        )

    async def start(self):
        """
            Start the broker to serve with the given configuration

            Start method opens network sockets and will start listening for incoming connections.

            This method is a *coroutine*.
        """
        try:
            self._sessions = dict()
            self._subscriptions = dict()
            if self._do_retain:
                self._retained_messages = dict()
            self.transitions.start()
            self.logger.debug("Broker starting")
        except (MachineError, ValueError) as exc:
            # Backwards compat: MachineError is raised by transitions < 0.5.0.
            self.logger.warning(
                "[WARN-0001] Invalid method call at this moment: %r", exc
            )
            raise BrokerException("Broker instance can't be started: %s" % exc)

        await self.plugins_manager.fire_event(EVENT_BROKER_PRE_START)
        try:
            # Start network listeners
            for listener_name in self.listeners_config:
                listener = self.listeners_config[listener_name]

                if "bind" not in listener:
                    self.logger.debug(
                        "Listener configuration '%s' is not bound", listener_name
                    )
                else:
                    # Max connections
                    try:
                        max_connections = listener["max_connections"]
                    except KeyError:
                        max_connections = -1

                    # SSL Context
                    sc = None

                    # accept string "on" / "off" or boolean
                    ssl_active = listener.get("ssl", False)
                    if isinstance(ssl_active, str):
                        ssl_active = ssl_active.upper() == "ON"

                    if ssl_active:
                        try:
                            sc = ssl.create_default_context(
                                ssl.Purpose.CLIENT_AUTH,
                                cafile=listener.get("cafile"),
                                capath=listener.get("capath"),
                                cadata=listener.get("cadata"),
                            )
                            sc.load_cert_chain(
                                listener["certfile"], listener["keyfile"]
                            )
                            sc.verify_mode = ssl.CERT_OPTIONAL
                        except KeyError as ke:
                            raise BrokerException(
                                "'certfile' or 'keyfile' configuration parameter missing: %s"
                                % ke
                            )
                        except FileNotFoundError as fnfe:
                            raise BrokerException(
                                "Can't read cert files '%s' or '%s' : %s"
                                % (listener["certfile"], listener["keyfile"], fnfe)
                            )

                    address, s_port = listener["bind"].split(":")
                    port = 0
                    try:
                        port = int(s_port)
                    except ValueError:
                        raise BrokerException(
                            "Invalid port value in bind value: %s" % listener["bind"]
                        )

                    async def server_task(evt, cb, address, port, ssl_context):
                        async with anyio.open_cancel_scope() as scope:
                            await evt.set(scope)
                            async with await anyio.create_tcp_server(
                                port, interface=address, ssl_context=ssl_context
                            ) as server:
                                async for conn in server.accept_connections():
                                    await self._tg.spawn(cb, conn)

                    if listener["type"] == "tcp":
                        cb_partial = partial(
                            self.stream_connected, listener_name=listener_name
                        )
                    elif listener["type"] == "ws":
                        cb_partial = partial(
                            self.ws_connected, listener_name=listener_name
                        )
                    else:
                        self.logger.error(
                            "Listener '%s': unknown type '%s'",
                            listener_name,
                            listener["type"],
                        )
                        continue
                    fut = Future()
                    await self._tg.spawn(
                        server_task,
                        fut,
                        cb_partial,
                        address,
                        port,
                        sc,
                        name=listener_name,
                    )
                    instance = await fut.get()
                    self._servers[listener_name] = Server(
                        listener_name, instance, max_connections
                    )

                    self.logger.info(
                        "Listener '%s' bind to %s (max_connections=%d)",
                        listener_name,
                        listener["bind"],
                        max_connections,
                    )

            self.transitions.starting_success()
            await self.plugins_manager.fire_event(EVENT_BROKER_POST_START)

            # Start broadcast loop
            await self._tg.spawn(self._broadcast_loop)

            self.logger.debug("Broker started")
        except Exception as e:
            if "Cancel" in repr(e):
                raise  # bah
            self.logger.error("Broker startup failed: %r", e)
            self.transitions.starting_fail()
            raise BrokerException("Broker instance can't be started") from e

    async def shutdown(self):
        """
            Stop broker instance.

            Closes all connected session, stop listening on network socket and free resources.
        """
        for s in self._sessions.values():
            await s[0].stop()

        self._sessions = dict()
        self._subscriptions = dict()
        if self._do_retain:
            self._retained_messages = dict()
        try:
            self.transitions.shutdown()
        except MachineError as exc:
            # Backwards compat: MachineError is raised by transitions < 0.5.0.
            raise BrokerException("Broker instance can't be stopped") from exc

        # Fire broker_shutdown event to plugins
        await self.plugins_manager.fire_event(EVENT_BROKER_PRE_SHUTDOWN)

        # Stop broadcast loop
        if self._broadcast_queue.qsize() > 0:
            self.logger.warning(
                "%d messages not broadcasted", self._broadcast_queue.qsize()
            )

        for listener_name in self._servers:
            server = self._servers[listener_name]
            await server.close_instance()
        self.logger.debug("Broker closing")
        self.logger.info("Broker closed")
        await self.plugins_manager.fire_event(EVENT_BROKER_POST_SHUTDOWN)
        self.transitions.stopping_success()

    async def internal_message_broadcast(self, topic, data, qos=None, retain=None):
        return await self.broadcast_message(None, topic, data, qos=qos, retain=retain)

    async def ws_connected(self, conn, listener_name):
        async def subpro(req):
            if "mqtt" not in req.subprotocols:
                return False
            return "mqtt"

        websocket = await create_websocket_server(conn, filter=subpro)
        await self.client_connected(listener_name, WebSocketsAdapter(websocket))

    async def stream_connected(self, conn, listener_name):
        await self.client_connected(listener_name, StreamAdapter(conn))

    async def client_connected(self, listener_name, adapter: BaseAdapter):
        server = self._servers.get(listener_name, None)
        if not server:
            raise BrokerException("Invalid listener name '%s'" % listener_name)

        async with server._client_limit():
            return await self.client_connected_(listener_name, adapter)

    async def client_connected_(self, listener_name, adapter: BaseAdapter):
        # Wait for connection available on listener

        remote_address, remote_port = adapter.get_peer_info()
        self.logger.info(
            "Connection from %s:%d on listener '%s'",
            remote_address,
            remote_port,
            listener_name,
        )

        # Wait for first packet and expect a CONNECT
        try:
            handler, client_session = await BrokerProtocolHandler.init_from_connect(
                adapter, self.plugins_manager
            )
        except DistMQTTException as exc:
            self.logger.warning(
                "[MQTT-3.1.0-1] %s: Can't read first packet an CONNECT: %s",
                format_client_message(address=remote_address, port=remote_port),
                exc,
            )
            # await writer.close()
            self.logger.debug("Connection closed")
            return
        except MQTTException as me:
            self.logger.error(
                "Invalid connection from %s : %s",
                format_client_message(address=remote_address, port=remote_port),
                me,
            )
            await adapter.close()
            self.logger.debug("Connection closed")
            return

        if client_session.clean_session:
            # Delete existing session and create a new one
            if client_session.client_id is not None and client_session.client_id != "":
                await self.delete_session(client_session.client_id)
            else:
                client_session.client_id = gen_client_id()
            client_session.parent = 0
        else:
            # Get session from cache
            if client_session.client_id in self._sessions:
                self.logger.debug(
                    "Found old session %r", self._sessions[client_session.client_id]
                )
                client_session = self._sessions[client_session.client_id][0]
                client_session.parent = 1
            else:
                client_session.parent = 0
        if not client_session.parent:
            await client_session.start(self)
        if client_session.keep_alive > 0 and not client_session.parent:
            # MQTT 3.1.2.10: one and a half keepalive times, plus configurable grace
            client_session.keep_alive += (
                client_session.keep_alive / 2 + self.config["timeout-disconnect-delay"]
            )
        self.logger.debug("Keep-alive timeout=%d", client_session.keep_alive)

        await handler.attach(client_session, adapter)
        self._sessions[client_session.client_id] = (client_session, handler)

        authenticated = await self.authenticate(client_session)
        if not authenticated:
            await adapter.close()
            return

        while True:
            try:
                client_session.transitions.connect()
                break
            except (MachineError, ValueError) as exc:
                # Backwards compat: MachineError is raised by transitions < 0.5.0.
                self.logger.warning(
                    "Client %s is reconnecting too quickly, make it wait",
                    client_session.client_id,
                    exc_info=exc,
                )
                # Wait a bit may be client is reconnecting too fast
                await anyio.sleep(1)
        await handler.mqtt_connack_authorize(authenticated)

        await self.plugins_manager.fire_event(
            EVENT_BROKER_CLIENT_CONNECTED, client_id=client_session.client_id
        )

        self.logger.debug("%s Start messages handling", client_session.client_id)
        await handler.start()
        if self._do_retain:
            self.logger.debug(
                "Retained messages queue size: %d",
                client_session.retained_messages.qsize(),
            )
            await self.publish_session_retained_messages(client_session)

        # Init and start loop for handling client messages (publish, subscribe/unsubscribe, disconnect)
        async with anyio.create_task_group() as tg:

            async def handle_unsubscribe():
                while True:
                    unsubscription = await handler.get_next_pending_unsubscription()
                    self.logger.debug(
                        "%s handling unsubscription", client_session.client_id
                    )
                    for topic in unsubscription["topics"]:
                        self._del_subscription(topic, client_session)
                        await self.plugins_manager.fire_event(
                            EVENT_BROKER_CLIENT_UNSUBSCRIBED,
                            client_id=client_session.client_id,
                            topic=topic,
                        )
                    await handler.mqtt_acknowledge_unsubscription(
                        unsubscription["packet_id"]
                    )

            async def handle_subscribe():
                while True:
                    subscriptions = await handler.get_next_pending_subscription()
                    self.logger.debug(
                        "%s handling subscription", client_session.client_id
                    )
                    return_codes = []
                    for subscription in subscriptions["topics"]:
                        result = await self.add_subscription(
                            subscription, client_session
                        )
                        return_codes.append(result)
                    await handler.mqtt_acknowledge_subscription(
                        subscriptions["packet_id"], return_codes
                    )
                    for index, subscription in enumerate(subscriptions["topics"]):
                        if return_codes[index] != 0x80:
                            await self.plugins_manager.fire_event(
                                EVENT_BROKER_CLIENT_SUBSCRIBED,
                                client_id=client_session.client_id,
                                topic=subscription[0],
                                qos=subscription[1],
                            )
                            if self._do_retain:
                                await self.publish_retained_messages_for_subscription(
                                    subscription, client_session
                                )
                    self.logger.debug(repr(self._subscriptions))

            await tg.spawn(handle_unsubscribe)
            await tg.spawn(handle_subscribe)

            try:
                await handler.wait_disconnect()
                self.logger.debug(
                    "%s wait_diconnect: %sclean",
                    client_session.client_id,
                    "" if handler.clean_disconnect else "un",
                )

                if not handler.clean_disconnect:
                    # Connection closed anormally, send will message
                    self.logger.debug("Will flag: %s", client_session.will_flag)
                    if client_session.will_flag:
                        if self.logger.isEnabledFor(logging.DEBUG):
                            self.logger.debug(
                                "Client %s disconnected abnormally, sending will message",
                                format_client_message(session=client_session),
                            )
                        await self.broadcast_message(
                            client_session,
                            client_session.will_topic,
                            client_session.will_message,
                            client_session.will_qos,
                            retain=client_session.will_retain,
                        )
                self.logger.debug("%s Disconnecting session", client_session.client_id)
                await self._stop_handler(handler)
                client_session.transitions.disconnect()
                await self.plugins_manager.fire_event(
                    EVENT_BROKER_CLIENT_DISCONNECTED, client_id=client_session.client_id
                )
            finally:
                async with anyio.fail_after(2, shield=True):
                    await tg.cancel_scope.cancel()
            pass  # end taskgroup

        self.logger.debug("%s Client disconnected", client_session.client_id)

    async def _stop_handler(self, handler):
        """
        Stop a running handler and detach if from the session
        :param handler:
        :return:
        """
        await handler.detach()
        await handler.stop()

    async def authenticate(self, session: Session):
        """
        This method call the authenticate method on registered plugins to test user authentication.
        User is considered authenticated if all plugins called returns True.
        Plugins authenticate() method are supposed to return :
         - True if user is authentication succeed
         - False if user authentication fails
         - None if authentication can't be achieved (then plugin result is then ignored)
        :param session:
        :param listener:
        :return:
        """
        auth_plugins = None
        auth_config = self.config.get("auth", None)
        if auth_config:
            auth_plugins = auth_config.get("plugins", None)
        returns = await self.plugins_manager.map_plugin_coro(
            "authenticate", session=session, filter_plugins=auth_plugins
        )
        auth_result = True
        if returns:
            for plugin in returns:
                res = returns[plugin]
                if res is False:
                    auth_result = False
                    self.logger.debug(
                        "Authentication failed due to '%s' plugin result: %s",
                        plugin.name,
                        res,
                    )
                else:
                    self.logger.debug("'%s' plugin result: %s", plugin.name, res)
        # If all plugins returned True, authentication is success
        return auth_result

    async def topic_filtering(self, session: Session, topic):
        """
        This method call the topic_filtering method on registered plugins to check that the subscription is allowed.
        User is considered allowed if all plugins called return True.
        Plugins topic_filtering() method are supposed to return :
         - True if MQTT client can be subscribed to the topic
         - False if MQTT client is not allowed to subscribe to the topic
         - None if topic filtering can't be achieved (then plugin result is then ignored)
        :param session:
        :param listener:
        :param topic: Topic in which the client wants to subscribe
        :return:
        """
        topic_plugins = None
        topic_config = self.config.get("topic-check", None)
        if topic_config and topic_config.get("enabled", False):
            topic_plugins = topic_config.get("plugins", None)
        returns = await self.plugins_manager.map_plugin_coro(
            "topic_filtering",
            session=session,
            topic=topic,
            filter_plugins=topic_plugins,
        )

        topic_result = True
        if returns:
            for plugin in returns:
                res = returns[plugin]
                if res is False:
                    topic_result = False
                    self.logger.debug(
                        "Topic filtering failed due to '%s' plugin result: %s",
                        plugin.name,
                        res,
                    )
        # If all plugins returned True, authentication is success
        return topic_result

    def retain_message(self, source_session, topic_name, data, qos=None):
        if not self._do_retain:
            raise RuntimeError("Support for retained messages is turned off")
        if data is not None and data != b"":
            # If retained flag set, store the message for further subscriptions
            self.logger.debug("Retaining %s: %r", topic_name, data)
            retained_message = RetainedApplicationMessage(
                source_session, topic_name, data, qos
            )
            self._retained_messages[topic_name] = retained_message
        else:
            # [MQTT-3.3.1-10]
            if topic_name in self._retained_messages:
                self.logger.debug("Retaining %s:‹deleted›", topic_name)
                del self._retained_messages[topic_name]

    async def add_subscription(self, subscription, session):
        a_filter = subscription[0]
        if "#" in a_filter and not a_filter.endswith("#"):
            # [MQTT-4.7.1-2] Wildcard character '#' is only allowed as last character in filter
            return 0x80
        if a_filter != "+":
            if "+" in a_filter:
                if "/+" not in a_filter and "+/" not in a_filter:
                    # [MQTT-4.7.1-3] + wildcard character must occupy entire level
                    return 0x80

        # Check if the client is authorised to connect to the topic
        permitted = await self.topic_filtering(session, topic=a_filter)
        if not permitted:
            return 0x80

        a_filter = tuple(a_filter.split("/"))
        qos = subscription[1]
        if "max-qos" in self.config and qos > self.config["max-qos"]:
            qos = self.config["max-qos"]
        if a_filter not in self._subscriptions:
            self._subscriptions[a_filter] = []
        already_subscribed = next(
            (
                s
                for (s, qos) in self._subscriptions[a_filter]
                if s.client_id == session.client_id
            ),
            None,
        )
        if not already_subscribed:
            self._subscriptions[a_filter].append((session, qos))
        else:
            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug(
                    "Client %s has already subscribed to %s",
                    format_client_message(session=session),
                    a_filter,
                )
        return qos

    def _del_subscription(self, a_filter, session):
        """
        Delete a session subscription on a given topic
        :param a_filter:
        :param session:
        :return:
        """
        deleted = 0
        if isinstance(a_filter, str):
            a_filter = tuple(a_filter.split("/"))
        try:
            subscriptions = self._subscriptions[a_filter]
            for index, (sub_session, qos) in enumerate(
                subscriptions
            ):  # pylint: disable=unused-variable
                if sub_session.client_id == session.client_id:
                    if self.logger.isEnabledFor(logging.DEBUG):
                        self.logger.debug(
                            "Removing subscription on topic '%s' for client %s",
                            a_filter,
                            format_client_message(session=session),
                        )
                    subscriptions.pop(index)
                    deleted += 1
                    break
        except KeyError:
            # Unsubscribe topic not found in current subscribed topics
            pass
        return deleted

    def _del_all_subscriptions(self, session):
        """
        Delete all topic subscriptions for a given session
        :param session:
        :return:
        """
        filter_queue = deque()
        for topic in self._subscriptions:
            if self._del_subscription(topic, session):
                filter_queue.append(topic)
        for topic in filter_queue:
            if not self._subscriptions[topic]:
                del self._subscriptions[topic]

    async def _broadcast_loop(self):
        async with anyio.create_task_group() as tg:
            while True:
                broadcast = await self._broadcast_queue.get()
                # self.logger.debug("broadcasting %r", broadcast)
                topic = broadcast["topic"]
                if isinstance(topic, str):
                    topic = topic.split("/")

                targets = {}
                for k_filter, subscriptions in self._subscriptions.items():
                    if match_topic(topic, k_filter):
                        for (target_session, qos) in subscriptions:
                            qos = max(
                                qos,
                                broadcast.get("qos", QOS_0),
                                targets.get(target_session, QOS_0),
                            )
                            targets[target_session] = qos

                for target_session, qos in targets.items():
                    if target_session.transitions.state == "connected":
                        if False and self.logger.isEnabledFor(logging.DEBUG):
                            self.logger.debug(
                                "broadcasting application message from %s on topic '%s' to %s",
                                format_client_message(session=broadcast["session"]),
                                broadcast["topic"],
                                format_client_message(session=target_session),
                            )
                        handler = self._get_handler(target_session)
                        await tg.spawn(
                            partial(
                                handler.mqtt_publish,
                                broadcast["topic"],
                                broadcast["data"],
                                qos,
                                retain=False,
                            )
                        )
                    else:
                        if self.logger.isEnabledFor(logging.DEBUG):
                            self.logger.debug(
                                "retaining application message from %s on topic '%s' to client '%s'",
                                format_client_message(session=broadcast["session"]),
                                broadcast["topic"],
                                format_client_message(session=target_session),
                            )
                        retained_message = RetainedApplicationMessage(
                            broadcast["session"],
                            broadcast["topic"],
                            broadcast["data"],
                            qos,
                        )
                        await target_session.retained_messages.put(retained_message)

    async def broadcast_message(
        self, session, topic, data, force_qos=None, qos=None, retain=False
    ):
        if not isinstance(data, (bytes, bytearray)):
            self.logger.error("Not bytes %s:%r", topic, data)
            return
        if retain and not self._do_retain:
            raise RuntimeError("Support for retained messages is off")
        broadcast = {"session": session, "topic": topic, "data": data}
        if force_qos:
            broadcast["qos"] = force_qos
        await self._broadcast_queue.put(broadcast)
        if retain:
            self.retain_message(session, topic, data, force_qos or qos)

    async def publish_session_retained_messages(self, session):
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(
                "Publishing %d messages retained for session %s",
                session.retained_messages.qsize(),
                format_client_message(session=session),
            )
        handler = self._get_handler(session)
        async with anyio.create_task_group() as tg:
            while not session.retained_messages.empty():
                retained = await session.retained_messages.get()
                await tg.spawn(
                    handler.mqtt_publish,
                    retained.topic,
                    retained.data,
                    retained.qos,
                    True,
                )

    async def publish_retained_messages_for_subscription(self, subscription, session):
        #       if self.logger.isEnabledFor(logging.DEBUG):
        #           self.logger.debug("Begin broadcasting messages retained due to subscription on '%s' from %s",
        #                             subscription[0], format_client_message(session=session))
        sub = subscription[0].split("/")
        handler = self._get_handler(session)
        async with anyio.create_task_group() as tg:
            for d_topic in self._retained_messages:
                topic = d_topic.split("/")
                self.logger.debug("matching : %s %s", d_topic, subscription[0])
                if match_topic(topic, sub):
                    self.logger.debug("%s and %s match", d_topic, subscription[0])
                    retained = self._retained_messages[d_topic]
                    await tg.spawn(
                        handler.mqtt_publish,
                        retained.topic,
                        retained.data,
                        subscription[1],
                        True,
                    )
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(
                "End broadcasting messages retained due to subscription on '%s' from %s",
                subscription[0],
                format_client_message(session=session),
            )

    async def delete_session(self, client_id):
        """
        Delete an existing session data, for example due to clean session set in CONNECT
        :param client_id:
        :return:
        """
        try:
            session = self._sessions[client_id][0]
        except KeyError:
            session = None
        if session is None:
            self.logger.debug("Delete session : session %s doesn't exist", client_id)
            return

        # Delete subscriptions
        self.logger.debug("deleting session %r subscriptions", session)
        self._del_all_subscriptions(session)

        self.logger.debug("deleting existing session %r", self._sessions[client_id])
        await session.stop()
        del self._sessions[client_id]

    def _get_handler(self, session):
        client_id = session.client_id
        if client_id:
            try:
                return self._sessions[client_id][1]
            except KeyError:
                pass
        return None
