# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import anyio
from distmqtt.mqtt.protocol.handler import ProtocolHandler, EVENT_MQTT_PACKET_RECEIVED
from distmqtt.mqtt.disconnect import DisconnectPacket
from distmqtt.mqtt.pingreq import PingReqPacket
from distmqtt.mqtt.pingresp import PingRespPacket
from distmqtt.mqtt.subscribe import SubscribePacket
from distmqtt.mqtt.suback import SubackPacket
from distmqtt.mqtt.unsubscribe import UnsubscribePacket
from distmqtt.mqtt.unsuback import UnsubackPacket
from distmqtt.mqtt.connect import ConnectVariableHeader, ConnectPayload, ConnectPacket
from distmqtt.mqtt.connack import ConnackPacket
from distmqtt.session import Session
from distmqtt.plugins.manager import PluginManager
from distmqtt.utils import Future


class ClientProtocolHandler(ProtocolHandler):
    def __init__(self, plugins_manager: PluginManager, session: Session = None):
        super().__init__(plugins_manager, session)
        self._ping_task = None
        self._pingresp_queue = anyio.create_queue(9999)
        self._subscriptions_waiter = dict()
        self._unsubscriptions_waiter = dict()
        self._disconnect_waiter = None

    async def stop(self):
        try:
            await super().stop()
        finally:
            async with anyio.fail_after(2, shield=True):
                t, self._ping_task = self._ping_task, None
                if t:
                    self.logger.debug("Cancel ping task")
                    await t.cancel()

    def _build_connect_packet(self):
        vh = ConnectVariableHeader()
        payload = ConnectPayload()

        vh.keep_alive = self.session.keep_alive
        vh.clean_session_flag = self.session.clean_session
        vh.will_retain_flag = self.session.will_retain
        payload.client_id = self.session.client_id

        if self.session.username:
            vh.username_flag = True
            payload.username = self.session.username
        else:
            vh.username_flag = False

        if self.session.password:
            vh.password_flag = True
            payload.password = self.session.password
        else:
            vh.password_flag = False
        if self.session.will_flag:
            vh.will_flag = True
            vh.will_qos = self.session.will_qos
            payload.will_message = self.session.will_message
            payload.will_topic = self.session.will_topic
        else:
            vh.will_flag = False

        packet = ConnectPacket(vh=vh, payload=payload)
        return packet

    async def mqtt_connect(self):
        connect_packet = self._build_connect_packet()
        await self._send_packet(connect_packet)
        connack = await ConnackPacket.from_stream(self.stream)
        self.logger.debug("< C %r", connack)
        await self.plugins_manager.fire_event(
            EVENT_MQTT_PACKET_RECEIVED, packet=connack, session=self.session
        )
        return connack.return_code

    async def handle_write_timeout(self):
        try:
            if self.session is not None and not self._ping_task:
                self.logger.debug("Scheduling Ping")
                evt = anyio.create_event()
                await self._tg.spawn(self.mqtt_ping, evt)
                await evt.wait()
        except BaseException as be:
            self.logger.debug("Exception in ping task: %r", be)
            raise

    async def handle_read_timeout(self):
        pass

    async def mqtt_subscribe(self, topics, packet_id):
        """
        :param topics: array of topics [{'filter':'/a/b', 'qos': 0x00}, ...]
        :return:
        """

        waiter = Future()
        self._subscriptions_waiter[packet_id] = waiter
        try:
            # Build and send SUBSCRIBE message
            subscribe = SubscribePacket.build(topics, packet_id)
            await self._send_packet(subscribe)

            # Wait for SUBACK is received
            return_codes = await waiter.get()

        finally:
            del self._subscriptions_waiter[packet_id]
        return return_codes

    async def handle_suback(self, suback: SubackPacket):
        packet_id = suback.variable_header.packet_id
        try:
            waiter = self._subscriptions_waiter.get(packet_id)
            await waiter.set(suback.payload.return_codes)
        except KeyError:
            self.logger.warning(
                "Received SUBACK for unknown pending subscription with Id: %s",
                packet_id,
            )

    async def mqtt_unsubscribe(self, topics, packet_id):
        """

        :param topics: array of topics ['/a/b', ...]
        :return:
        """
        waiter = Future()
        self._unsubscriptions_waiter[packet_id] = waiter
        try:
            unsubscribe = UnsubscribePacket.build(topics, packet_id)
            await self._send_packet(unsubscribe)

            await waiter.get()
        finally:
            del self._unsubscriptions_waiter[packet_id]

    async def handle_unsuback(self, unsuback: UnsubackPacket):
        packet_id = unsuback.variable_header.packet_id
        try:
            waiter = self._unsubscriptions_waiter.get(packet_id)
            if waiter is not None:
                await waiter.set(None)
        except KeyError:
            self.logger.warning(
                "Received UNSUBACK for unknown pending subscription with Id: %s",
                packet_id,
            )

    async def mqtt_disconnect(self):
        disconnect_packet = DisconnectPacket()
        await self._send_packet(disconnect_packet)

    async def mqtt_ping(self, evt=None):
        async with anyio.open_cancel_scope() as scope:
            self._ping_task = scope
            if evt is not None:
                await evt.set()

            try:
                ping_packet = PingReqPacket()
                await self._send_packet(ping_packet)
                resp = await self._pingresp_queue.get()
                return resp
            finally:
                self._ping_task = None

    async def handle_pingresp(self, pingresp: PingRespPacket):
        await self._pingresp_queue.put(pingresp)

    async def handle_connection_closed(self):
        self.logger.debug("Broker closed connection")
        if self._disconnect_waiter:
            await self._disconnect_waiter.set()
