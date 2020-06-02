# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import anyio
import logging

from transitions import Machine
from collections import OrderedDict

from distmqtt.mqtt.publish import PublishPacket
from distmqtt.errors import DistMQTTException, MQTTException
from distmqtt.mqtt.constants import QOS_0

OUTGOING = 0
INCOMING = 1

EVENT_BROKER_MESSAGE_RECEIVED = "broker_message_received"

logger = logging.getLogger(__name__)


class ApplicationMessage:

    """
        ApplicationMessage and subclasses are used to store published message information flow. These objects can contain different information depending on the way they were created (incoming or outgoing) and the quality of service used between peers.
    """

    __slots__ = (
        "packet_id",
        "topic",
        "qos",
        "data",
        "retain",
        "publish_packet",
        "puback_packet",
        "pubrec_packet",
        "pubrel_packet",
        "pubcomp_packet",
    )

    def __init__(self, packet_id, topic, qos, data, retain):
        if not isinstance(data, (bytes, bytearray)):
            raise RuntimeError("Non-bytes data for %s: %r" % (topic, data))

        self.packet_id = packet_id
        """ Publish message `packet identifier <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718025>`_"""

        self.topic = topic
        """ Publish message topic"""

        self.qos = qos
        """ Publish message Quality of Service"""

        self.data = data
        """ Publish message payload data"""

        self.retain = retain
        """ Publish message retain flag"""

        self.publish_packet = None
        """ :class:`distmqtt.mqtt.publish.PublishPacket` instance corresponding to the `PUBLISH <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718037>`_ packet in the messages flow. ``None`` if the PUBLISH packet has not already been received or sent."""

        self.puback_packet = None
        """ :class:`distmqtt.mqtt.puback.PubackPacket` instance corresponding to the `PUBACK <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718043>`_ packet in the messages flow. ``None`` if QoS != QOS_1 or if the PUBACK packet has not already been received or sent."""

        self.pubrec_packet = None
        """ :class:`distmqtt.mqtt.puback.PubrecPacket` instance corresponding to the `PUBREC <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718048>`_ packet in the messages flow. ``None`` if QoS != QOS_2 or if the PUBREC packet has not already been received or sent."""

        self.pubrel_packet = None
        """ :class:`distmqtt.mqtt.puback.PubrelPacket` instance corresponding to the `PUBREL <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718053>`_ packet in the messages flow. ``None`` if QoS != QOS_2 or if the PUBREL packet has not already been received or sent."""

        self.pubcomp_packet = None
        """ :class:`distmqtt.mqtt.puback.PubrelPacket` instance corresponding to the `PUBCOMP <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718058>`_ packet in the messages flow. ``None`` if QoS != QOS_2 or if the PUBCOMP packet has not already been received or sent."""

    def build_publish_packet(self, dup=False):
        """
            Build :class:`distmqtt.mqtt.publish.PublishPacket` from attributes

        :param dup: force dup flag
        :return: :class:`distmqtt.mqtt.publish.PublishPacket` built from ApplicationMessage instance attributes
        """
        return PublishPacket.build(
            self.topic, self.data, self.packet_id, dup, self.qos, self.retain
        )

    def __eq__(self, other):
        return self.packet_id == other.packet_id

    def __getstate__(self):
        res = dict((k, getattr(self, k)) for k in ("topic", "qos", "data", "retain"))
        res["id"] = self.packet_id
        return res

    def __setstate__(self, state):
        self.packet_id = state["id"]
        for k in ("topic", "qos", "data", "retain"):
            setattr(self, k, state[k])


class IncomingApplicationMessage(ApplicationMessage):

    """
        Incoming :class:`~distmqtt.session.ApplicationMessage`.
    """

    __slots__ = ("direction",)

    def __init__(self, packet_id, topic, qos, data, retain):
        super().__init__(packet_id, topic, qos, data, retain)
        self.direction = INCOMING


class OutgoingApplicationMessage(ApplicationMessage):

    """
        Outgoing :class:`~distmqtt.session.ApplicationMessage`.
    """

    __slots__ = ("direction",)

    def __init__(self, packet_id, topic, qos, data, retain):
        super().__init__(packet_id, topic, qos, data, retain)
        self.direction = OUTGOING


class Session:
    states = ["new", "connected", "disconnected"]

    def __init__(self, plugins_manager):
        self._init_states()
        self._plugins_manager = plugins_manager
        self.remote_address = None
        self.remote_port = None
        self.client_id = None
        self.clean_session = None
        self.will_flag = False
        self.will_message = None
        self.will_qos = None
        self.will_retain = None
        self.will_topic = None
        self.keep_alive = 0
        self.publish_retry_delay = 0
        self.broker_uri = None
        self.username = None
        self.password = None
        self.cafile = None
        self.capath = None
        self.cadata = None
        self._packet_id = 0
        self.parent = 0

        self.logger = logging.getLogger(__name__)

        # Used to store outgoing ApplicationMessage while publish protocol flows
        self.inflight_out = OrderedDict()

        # Used to store incoming ApplicationMessage while publish protocol flows
        self.inflight_in = OrderedDict()

        # Stores messages retained for this session
        self.retained_messages = anyio.create_queue(9999)

        # Stores PUBLISH messages ID received in order and ready for application process
        self._delivered_message_queue = anyio.create_queue(9999)

        # The actual delivery process
        self._delivery_task = None
        self._delivery_stopped = anyio.create_event()

        # The broker we're attached to
        self._broker = None

    def _init_states(self):
        self.transitions = Machine(states=Session.states, initial="new")
        self.transitions.add_transition(
            trigger="connect", source="new", dest="connected"
        )
        self.transitions.add_transition(
            trigger="connect", source="disconnected", dest="connected"
        )
        self.transitions.add_transition(
            trigger="disconnect", source="connected", dest="disconnected"
        )
        self.transitions.add_transition(
            trigger="disconnect", source="new", dest="disconnected"
        )
        self.transitions.add_transition(
            trigger="disconnect", source="disconnected", dest="disconnected"
        )

    def __hash__(self):
        return hash(self.client_id)

    def __eq__(self, other):
        other = getattr(other, "client_id", other)
        return self.client_id == other

    async def start(self, broker=None):
        if broker is not None:
            self._broker = broker
            if self._delivery_task is not None:
                raise RuntimeError("Already running")
            await broker._tg.spawn(self._delivery_loop)

    async def stop(self):
        if self._delivery_task is not None:
            await self._delivery_task.cancel()
            await self._delivery_stopped.wait()
        self._broker = None  # break ref loop

    async def put_message(self, app_message):
        if (
            app_message.retain
            and self._broker is not None
            and not self._broker._do_retain
        ):
            raise RuntimeError(
                "The broker doesn't do retains", repr(app_message.__getstate__())
            )
        if not app_message.topic:
            self.logger.warning(
                "[MQTT-4.7.3-1] - %s invalid TOPIC sent in PUBLISH message,closing connection",
                self.client_id,
            )
            raise MQTTException(
                "[MQTT-4.7.3-1] - %s invalid TOPIC sent in PUBLISH message,closing connection"
                % self.client_id
            )
        if "#" in app_message.topic or "+" in app_message.topic:
            self.logger.warning(
                "[MQTT-3.3.2-2] - %s invalid TOPIC sent in PUBLISH message, closing connection",
                self.client_id,
            )
            raise MQTTException(
                "[MQTT-3.3.2-2] - %s invalid TOPIC sent in PUBLISH message, closing connection"
                % self.client_id
            )
        if app_message.qos == QOS_0 and self._delivered_message_queue.qsize() >= 9999:
            self.logger.warning(
                "delivered messages queue full. QOS_0 message discarded"
            )
        else:
            await self._delivered_message_queue.put(app_message)

    async def get_next_message(self):
        """Client: get the next message"""
        m = await self._delivered_message_queue.get()
        # split up so that a breakpoint may be set
        return m

    async def _delivery_loop(self):
        """Server: process incoming messages"""
        try:
            async with anyio.open_cancel_scope() as scope:
                self._delivery_task = scope
                broker = self._broker
                broker.logger.debug("%s handling message delivery", self.client_id)
                while True:
                    app_message = await self.get_next_message()
                    await self._plugins_manager.fire_event(
                        EVENT_BROKER_MESSAGE_RECEIVED,
                        client_id=self.client_id,
                        message=app_message,
                    )

                    await broker.broadcast_message(
                        self,
                        app_message.topic,
                        app_message.data,
                        qos=app_message.qos,
                        retain=app_message.publish_packet.retain_flag,
                    )
        finally:
            async with anyio.fail_after(2, shield=True):
                broker.logger.debug("%s finished message delivery", self.client_id)
                self._delivery_task = None
                await self._delivery_stopped.set()

    @property
    def next_packet_id(self):
        self._packet_id += 1
        if self._packet_id > 65535:
            self._packet_id = 1
        limit = self._packet_id
        while (
            self._packet_id in self.inflight_in or self._packet_id in self.inflight_out
        ):
            self._packet_id += 1
            if self._packet_id > 65535:
                self._packet_id = 1
            if self._packet_id == limit:
                raise DistMQTTException(
                    "More than 65535 messages pending. No free packet ID"
                )

        return self._packet_id

    @property
    def inflight_in_count(self):
        return len(self.inflight_in)

    @property
    def inflight_out_count(self):
        return len(self.inflight_out)

    @property
    def retained_messages_count(self):
        return self.retained_messages.qsize()

    def __repr__(self):
        return type(self).__name__ + "(clientId={0}, state={1})".format(
            self.client_id, self.transitions.state
        )

    def __getstate__(self):
        state = self.__dict__.copy()
        # Remove the unpicklable entries.
        # del state['transitions']
        del state["retained_messages"]
        del state["_delivered_message_queue"]
        del state["_delivery_task"]
        del state["_delivery_stopped"]
        del state["_broker"]
        return state

    def __setstate(self, state):
        self.__dict__.update(state)
        self.retained_messages = anyio.create_queue(9999)
        self._delivered_message_queue = anyio.create_queue(9999)
