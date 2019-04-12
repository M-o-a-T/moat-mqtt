# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import logging
import collections
import itertools

import anyio

from hbmqtt.mqtt import packet_class
from hbmqtt.mqtt.connack import ConnackPacket
from hbmqtt.mqtt.connect import ConnectPacket
from hbmqtt.mqtt.packet import (
    RESERVED_0, CONNECT, CONNACK, PUBLISH, PUBACK, PUBREC, PUBREL, PUBCOMP,
    SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK, PINGREQ, PINGRESP, DISCONNECT,
    RESERVED_15, MQTTFixedHeader)
from hbmqtt.mqtt.pingresp import PingRespPacket
from hbmqtt.mqtt.pingreq import PingReqPacket
from hbmqtt.mqtt.publish import PublishPacket
from hbmqtt.mqtt.pubrel import PubrelPacket
from hbmqtt.mqtt.puback import PubackPacket
from hbmqtt.mqtt.pubrec import PubrecPacket
from hbmqtt.mqtt.pubcomp import PubcompPacket
from hbmqtt.mqtt.suback import SubackPacket
from hbmqtt.mqtt.subscribe import SubscribePacket
from hbmqtt.mqtt.unsubscribe import UnsubscribePacket
from hbmqtt.mqtt.unsuback import UnsubackPacket
from hbmqtt.mqtt.disconnect import DisconnectPacket
from hbmqtt.adapters import ReaderAdapter, WriterAdapter
from hbmqtt.session import Session, OutgoingApplicationMessage, IncomingApplicationMessage, INCOMING, OUTGOING
from hbmqtt.mqtt.constants import QOS_0, QOS_1, QOS_2
from hbmqtt.plugins.manager import PluginManager
from hbmqtt.errors import HBMQTTException, MQTTException, NoDataException, InvalidStateError
from hbmqtt.utils import Future


EVENT_MQTT_PACKET_SENT = 'mqtt_packet_sent'
EVENT_MQTT_PACKET_RECEIVED = 'mqtt_packet_received'

_spi=0

class ProtocolHandlerException(Exception):
    pass


class ProtocolHandler:
    """
    Class implementing the MQTT communication protocol using async features
    """

    def __init__(self, plugins_manager: PluginManager, session: Session=None):
        self.logger = logging.getLogger(__name__)
        if session:
            self._init_session(session)
        else:
            self.session = None
        self.reader = None
        self.writer = None
        self.plugins_manager = plugins_manager
        self._tg = plugins_manager._tg

        self._reader_task = None
        self._keepalive_task = None
        self._reader_ready = None
        self._reader_stopped = anyio.create_event()

        self._puback_waiters = dict()
        self._pubrec_waiters = dict()
        self._pubrel_waiters = dict()
        self._pubcomp_waiters = dict()

        self._write_lock = anyio.create_lock()

    def _init_session(self, session: Session):
        assert session
        log = logging.getLogger(__name__)
        self.session = session
        self.logger = logging.LoggerAdapter(log, {'client_id': self.session.client_id})
        self.keepalive_timeout = self.session.keep_alive
        if self.keepalive_timeout <= 0:
            self.keepalive_timeout = None

    def attach(self, session, reader: ReaderAdapter, writer: WriterAdapter):
        if self.session:
            raise ProtocolHandlerException("Handler is already attached to a session")
        self._init_session(session)
        self.reader = reader
        self.writer = writer

    def detach(self):
        self.session = None
        self.reader = None
        self.writer = None

    def _is_attached(self):
        if self.session:
            return True
        else:
            return False

    async def _call_write_timeout(self, evt):
        async with anyio.open_cancel_scope() as scope:
            self._keepalive_task = scope
            await evt.set()
            await anyio.sleep(self.keepalive_timeout)
            if self._keepalive_task is scope:
                self._keepalive_task = None
        await self.handle_write_timeout()

    async def start(self):
        if not self._is_attached():
            raise ProtocolHandlerException("Handler is not attached to a stream")
        self._reader_ready = anyio.create_event()
        await self._tg.spawn(self._reader_loop)
        await self._reader_ready.wait()
        if self.keepalive_timeout:
            evt = anyio.create_event()
            await self._tg.spawn(self._call_write_timeout, evt)
            await evt.wait()

        self.logger.debug("Handler tasks started")
        await self._retry_deliveries()
        self.logger.debug("Handler ready")

    async def stop(self):
        # Stop messages flow waiter
        await self._stop_waiters()
        t,self._keepalive_task = self._keepalive_task,None
        if t:
            await t.cancel()
        self.logger.debug("waiting for tasks to be stopped")
        t, self._reader_task = self._reader_task, None
        if t:
            await t.cancel()
            await self._reader_stopped.wait()
        self.logger.debug("closing writer")
        if self.writer is not None:
            await self.writer.close()

    async def _stop_waiters(self):
        self.logger.debug("Stopping %d puback waiters" % len(self._puback_waiters))
        self.logger.debug("Stopping %d pucomp waiters" % len(self._pubcomp_waiters))
        self.logger.debug("Stopping %d purec waiters" % len(self._pubrec_waiters))
        self.logger.debug("Stopping %d purel waiters" % len(self._pubrel_waiters))
        for waiter in itertools.chain(
                self._puback_waiters.values(),
                self._pubcomp_waiters.values(),
                self._pubrec_waiters.values(),
                self._pubrel_waiters.values()):
            await waiter.cancel()

    async def _retry_deliveries(self):
        """
        Handle [MQTT-4.4.0-1] by resending PUBLISH and PUBREL messages for pending out messages
        :return:
        """
        done = pending = 0
        self.logger.debug("Begin messages delivery retries")

        async def process_one(message):
            async with anyio.move_on_after(10):
                await self._handle_message_flow(message)

                nonlocal done
                done += 1

        async with anyio.create_task_group() as tg:
            for message in itertools.chain(self.session.inflight_in.values(), self.session.inflight_out.values()):
                pending += 1
                await tg.spawn(process_one, message)
        pending -= done

        self.logger.debug("%d messages redelivered" % done)
        self.logger.debug("%d messages not redelivered due to timeout" % pending)
        self.logger.debug("End messages delivery retries")

    async def mqtt_publish(self, topic, data, qos, retain, ack_timeout=None):
        """
        Sends a MQTT publish message and manages messages flows.
        This methods doesn't return until the message has been acknowledged by receiver or timeout occur
        :param topic: MQTT topic to publish
        :param data:  data to send on topic
        :param qos: quality of service to use for message flow. Can be QOS_0, QOS_1 or QOS_2
        :param retain: retain message flag
        :param ack_timeout: acknowledge timeout. If set, this method will return a TimeOut error if the acknowledgment
        is not completed before ack_timeout second
        :return: ApplicationMessage used during inflight operations
        """
        if qos in (QOS_1, QOS_2):
            packet_id = self.session.next_packet_id
            if packet_id in self.session.inflight_out:
                raise HBMQTTException("A message with the same packet ID '%d' is already in flight" % packet_id)
        else:
            packet_id = None

        message = OutgoingApplicationMessage(packet_id, topic, qos, data, retain)
        # Handle message flow
        if ack_timeout is not None and ack_timeout > 0:
            async with anyio.move_on_after(ack_timeout):
                await self._handle_message_flow(message)
        else:
            await self._handle_message_flow(message)

        return message

    async def _handle_message_flow(self, app_message):
        """
        Handle protocol flow for incoming and outgoing messages, depending on service level and according to MQTT
        spec. paragraph 4.3-Quality of Service levels and protocol flows
        :param app_message: PublishMessage to handle
        :return: nothing.
        """
        if app_message.qos == QOS_0:
            await self._handle_qos0_message_flow(app_message)
        elif app_message.qos == QOS_1:
            await self._handle_qos1_message_flow(app_message)
        elif app_message.qos == QOS_2:
            await self._handle_qos2_message_flow(app_message)
        else:
            raise HBMQTTException("Unexcepted QOS value '%d" % str(app_message.qos))

    async def _handle_qos0_message_flow(self, app_message):
        """
        Handle QOS_0 application message acknowledgment
        For incoming messages, this method stores the message
        For outgoing messages, this methods sends PUBLISH
        :param app_message:
        :return:
        """
        assert app_message.qos == QOS_0
        if app_message.direction == OUTGOING:
            packet = app_message.build_publish_packet()
            # Send PUBLISH packet
            await self._send_packet(packet)
            app_message.publish_packet = packet
        elif app_message.direction == INCOMING:
            if app_message.publish_packet.dup_flag:
                self.logger.warning("[MQTT-3.3.1-2] DUP flag must set to 0 for QOS 0 message. Message ignored: %s" %
                                    repr(app_message.publish_packet))
            else:
                if self.session.delivered_message_queue.qsize() < 9999:
                    await self.session.delivered_message_queue.put(app_message)
                else:
                    self.logger.warning("delivered messages queue full. QOS_0 message discarded")

    async def _handle_qos1_message_flow(self, app_message):
        """
        Handle QOS_1 application message acknowledgment
        For incoming messages, this method stores the message and reply with PUBACK
        For outgoing messages, this methods sends PUBLISH and waits for the corresponding PUBACK
        :param app_message:
        :return:
        """
        assert app_message.qos == QOS_1
        if app_message.puback_packet:
            raise HBMQTTException("Message '%d' has already been acknowledged" % app_message.packet_id)
        if app_message.direction == OUTGOING:
            if app_message.packet_id not in self.session.inflight_out:
                # Store message in session
                self.session.inflight_out[app_message.packet_id] = app_message
            if app_message.publish_packet is not None:
                # A Publish packet has already been sent, this is a retry
                publish_packet = app_message.build_publish_packet(dup=True)
            else:
                publish_packet = app_message.build_publish_packet()

            # Wait for puback
            waiter = Future()
            self._puback_waiters[app_message.packet_id] = waiter
            # Send PUBLISH packet
            try:
                await self._send_packet(publish_packet)
                app_message.publish_packet = publish_packet
                app_message.puback_packet = await waiter.get()
            finally:
                del self._puback_waiters[app_message.packet_id]

            # Discard inflight message
            del self.session.inflight_out[app_message.packet_id]
        elif app_message.direction == INCOMING:
            # Initiate delivery
            self.logger.debug("Add message to delivery")
            await self.session.delivered_message_queue.put(app_message)
            # Send PUBACK
            puback = PubackPacket.build(app_message.packet_id)
            await self._send_packet(puback)
            app_message.puback_packet = puback

    async def _handle_qos2_message_flow(self, app_message):
        """
        Handle QOS_2 application message acknowledgment
        For incoming messages, this method stores the message, sends PUBREC, waits for PUBREL, initiate delivery
        and send PUBCOMP
        For outgoing messages, this methods sends PUBLISH, waits for PUBREC, discards messages and wait for PUBCOMP
        :param app_message:
        :return:
        """
        assert app_message.qos == QOS_2
        if app_message.direction == OUTGOING:
            if app_message.pubrel_packet and app_message.pubcomp_packet:
                raise HBMQTTException("Message '%d' has already been acknowledged" % app_message.packet_id)
            if not app_message.pubrel_packet:
                # Store message
                if app_message.publish_packet is not None:
                    # This is a retry flow, no need to store just check the message exists in session
                    if app_message.packet_id not in self.session.inflight_out:
                        raise HBMQTTException("Unknown inflight message '%d' in session" % app_message.packet_id)
                    publish_packet = app_message.build_publish_packet(dup=True)
                else:
                    # Store message in session
                    self.session.inflight_out[app_message.packet_id] = app_message
                    publish_packet = app_message.build_publish_packet()

                # Wait PUBREC
                if app_message.packet_id in self._pubrec_waiters:
                    # PUBREC waiter already exists for this packet ID
                    message = "Can't add PUBREC waiter, a waiter already exists for message Id '%s'" \
                              % app_message.packet_id
                    self.logger.warning(message)
                    raise HBMQTTException(message)
                waiter = Future()
                self._pubrec_waiters[app_message.packet_id] = waiter
                # Send PUBLISH packet
                try:
                    await self._send_packet(publish_packet)
                    app_message.publish_packet = publish_packet
                    app_message.pubrec_packet = await waiter.get()
                finally:
                    del self._pubrec_waiters[app_message.packet_id]

            if not app_message.pubcomp_packet:
                # Wait for PUBCOMP
                waiter = Future()
                self._pubcomp_waiters[app_message.packet_id] = waiter
                # Send pubrel
                try:
                    app_message.pubrel_packet = PubrelPacket.build(app_message.packet_id)
                    await self._send_packet(app_message.pubrel_packet)
                    app_message.pubcomp_packet = await waiter.get()
                finally:
                    del self._pubcomp_waiters[app_message.packet_id]
            # Discard inflight message
            del self.session.inflight_out[app_message.packet_id]
        elif app_message.direction == INCOMING:
            self.session.inflight_in[app_message.packet_id] = app_message
            # Wait PUBREL
            if app_message.packet_id in self._pubrel_waiters and not self._pubrel_waiters[app_message.packet_id].done():
                # PUBREL waiter already exists for this packet ID
                message = "A waiter already exists for message Id '%s', canceling it" \
                          % app_message.packet_id
                self.logger.warning(message)
                await self._pubrel_waiters[app_message.packet_id].cancel()
            waiter = Future()
            self._pubrel_waiters[app_message.packet_id] = waiter
            # Send pubrec
            try:
                pubrec_packet = PubrecPacket.build(app_message.packet_id)
                await self._send_packet(pubrec_packet)
                app_message.pubrec_packet = pubrec_packet
                app_message.pubrel_packet = await waiter.get()
            finally:
                if self._pubrel_waiters.get(app_message.packet_id) is waiter:
                    del self._pubrel_waiters[app_message.packet_id]
            # Initiate delivery and discard message
            await self.session.delivered_message_queue.put(app_message)
            del self.session.inflight_in[app_message.packet_id]
            # Send pubcomp
            pubcomp_packet = PubcompPacket.build(app_message.packet_id)
            await self._send_packet(pubcomp_packet)
            app_message.pubcomp_packet = pubcomp_packet

    async def _reader_loop(self):
        self.logger.debug("%s Starting reader coro" % self.session.client_id)
        keepalive_timeout = self.session.keep_alive
        if keepalive_timeout <= 0:
            keepalive_timeout = None
        try:
            async with anyio.create_task_group() as tg:
                self._reader_task = tg.cancel_scope
                while True:
                    try:
                        await self._reader_ready.set()
                        try:
                            running_tasks = tg.cancel_scope._tasks
                        except AttributeError:
                            running_tasks = ()
                        if len(running_tasks):
                            self.logger.debug("handler running tasks: %d" % len(running_tasks))

                        async with anyio.fail_after(keepalive_timeout):
                            fixed_header = await MQTTFixedHeader.from_stream(self.reader)
                        if fixed_header is None:
                            self.logger.debug("%s No more data (EOF received), stopping reader coro" % self.session.client_id)
                            break

                        if fixed_header.packet_type == RESERVED_0 or fixed_header.packet_type == RESERVED_15:
                            self.logger.warning("%s Received reserved packet, which is forbidden: closing connection" %
                                                (self.session.client_id))
                            await self.handle_connection_closed()
                            break
                        cls = packet_class(fixed_header)
                        packet = await cls.from_stream(self.reader, fixed_header=fixed_header)
                        await self.plugins_manager.fire_event(
                            EVENT_MQTT_PACKET_RECEIVED, packet=packet, session=self.session)
                        if packet.fixed_header.packet_type == CONNACK:
                            await tg.spawn(self.handle_connack, packet)
                        elif packet.fixed_header.packet_type == SUBSCRIBE:
                            await tg.spawn(self.handle_subscribe, packet)
                        elif packet.fixed_header.packet_type == UNSUBSCRIBE:
                            await tg.spawn(self.handle_unsubscribe, packet)
                        elif packet.fixed_header.packet_type == SUBACK:
                            await tg.spawn(self.handle_suback, packet)
                        elif packet.fixed_header.packet_type == UNSUBACK:
                            await tg.spawn(self.handle_unsuback, packet)
                        elif packet.fixed_header.packet_type == PUBACK:
                            await tg.spawn(self.handle_puback, packet)
                        elif packet.fixed_header.packet_type == PUBREC:
                            await tg.spawn(self.handle_pubrec, packet)
                        elif packet.fixed_header.packet_type == PUBREL:
                            await tg.spawn(self.handle_pubrel, packet)
                        elif packet.fixed_header.packet_type == PUBCOMP:
                            await tg.spawn(self.handle_pubcomp, packet)
                        elif packet.fixed_header.packet_type == PINGREQ:
                            await tg.spawn(self.handle_pingreq, packet)
                        elif packet.fixed_header.packet_type == PINGRESP:
                            await tg.spawn(self.handle_pingresp, packet)
                        elif packet.fixed_header.packet_type == PUBLISH:
                            await tg.spawn(self.handle_publish, packet)
                        elif packet.fixed_header.packet_type == DISCONNECT:
                            await tg.spawn(self.handle_disconnect, packet)
                        elif packet.fixed_header.packet_type == CONNECT:
                            self.handle_connect(packet)
                        else:
                            self.logger.warning("%s Unhandled packet type: %s" %
                                                (self.session.client_id, packet.fixed_header.packet_type))
                    except MQTTException:
                        self.logger.debug("Message discarded")
                    except TimeoutError:
                        self.logger.debug("%s Input stream read timeout", self.session.client_id if self.session else '?')
                        await self.handle_read_timeout()
                    except NoDataException:
                        self.logger.debug("%s No data available" % self.session.client_id)
                        # break # XXX

                    except BaseException as e:
                        self.logger.warning("%s Unhandled exception in reader coro", type(self).__name__, exc_info=e)
                        raise
                await tg.cancel_scope.cancel()
        finally:
            async with anyio.open_cancel_scope(shield=True):
                self.logger.debug("%s Reader coro stopped", self.session.client_id if self.session else '?')
                await self._reader_stopped.set()
                await self.handle_connection_closed()
                await self.stop()

    async def _send_packet(self, packet):
        try:
            async with self._write_lock:
                if self.writer is None:
                    return
                await packet.to_stream(self.writer)
            t,self._keepalive_task = self._keepalive_task,None
            if t:
                await t.cancel()
                evt = anyio.create_event()
                await self._tg.spawn(self._call_write_timeout, evt)
                await evt.wait()

            await self.plugins_manager.fire_event(EVENT_MQTT_PACKET_SENT, packet=packet, session=self.session)
        except ConnectionResetError as cre:
            await self.handle_connection_closed()
            raise
        except BaseException as e:
            import pdb;pdb.set_trace()
            self.logger.warning("Unhandled exception %d", sp, exc_info=e)
            raise

    async def mqtt_deliver_next_message(self):
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug("%d message(s) available for delivery" % self.session.delivered_message_queue.qsize())
        message = await self.session.delivered_message_queue.get()
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug("Delivering message %s" % message)
        return message

    async def handle_write_timeout(self):
        self.logger.debug('%s write timeout unhandled' % self.session.client_id)

    async def handle_read_timeout(self):
        self.logger.debug('%s read timeout unhandled' % self.session.client_id)

    async def handle_connack(self, connack: ConnackPacket):
        self.logger.debug('%s CONNACK unhandled' % self.session.client_id)

    async def handle_connect(self, connect: ConnectPacket):
        self.logger.debug('%s CONNECT unhandled' % self.session.client_id)

    async def handle_subscribe(self, subscribe: SubscribePacket):
        self.logger.debug('%s SUBSCRIBE unhandled' % self.session.client_id)

    async def handle_unsubscribe(self, subscribe: UnsubscribePacket):
        self.logger.debug('%s UNSUBSCRIBE unhandled' % self.session.client_id)

    async def handle_suback(self, suback: SubackPacket):
        self.logger.debug('%s SUBACK unhandled' % self.session.client_id)

    async def handle_unsuback(self, unsuback: UnsubackPacket):
        self.logger.debug('%s UNSUBACK unhandled' % self.session.client_id)

    async def handle_pingresp(self, pingresp: PingRespPacket):
        self.logger.debug('%s PINGRESP unhandled' % self.session.client_id)

    async def handle_pingreq(self, pingreq: PingReqPacket):
        self.logger.debug('%s PINGREQ unhandled' % self.session.client_id)

    async def handle_disconnect(self, disconnect: DisconnectPacket):
        self.logger.debug('%s DISCONNECT unhandled' % self.session.client_id)

    async def handle_connection_closed(self):
        self.logger.debug('%s Connection closed unhandled' % self.session.client_id)

    async def handle_puback(self, puback: PubackPacket):
        packet_id = puback.variable_header.packet_id
        try:
            waiter = self._puback_waiters[packet_id]
            await waiter.set(puback)
        except KeyError:
            self.logger.warning("Received PUBACK for unknown pending message Id: '%d'" % packet_id)
        except InvalidStateError:
            self.logger.warning("PUBACK waiter with Id '%d' already done" % packet_id)

    async def handle_pubrec(self, pubrec: PubrecPacket):
        packet_id = pubrec.packet_id
        try:
            waiter = self._pubrec_waiters[packet_id]
            await waiter.set(pubrec)
        except KeyError:
            self.logger.warning("Received PUBREC for unknown pending message with Id: %d" % packet_id)
        except InvalidStateError:
            self.logger.warning("PUBREC waiter with Id '%d' already done" % packet_id)

    async def handle_pubcomp(self, pubcomp: PubcompPacket):
        packet_id = pubcomp.packet_id
        try:
            waiter = self._pubcomp_waiters[packet_id]
            await waiter.set(pubcomp)
        except KeyError:
            self.logger.warning("Received PUBCOMP for unknown pending message with Id: %d" % packet_id)
        except InvalidStateError:
            self.logger.warning("PUBCOMP waiter with Id '%d' already done" % packet_id)

    async def handle_pubrel(self, pubrel: PubrelPacket):
        packet_id = pubrel.packet_id
        try:
            waiter = self._pubrel_waiters[packet_id]
            await waiter.set(pubrel)
        except KeyError:
            self.logger.warning("Received PUBREL for unknown pending message with Id: %d" % packet_id)
        except InvalidStateError:
            self.logger.warning("PUBREL waiter with Id '%d' already done" % packet_id)

    async def handle_publish(self, publish_packet: PublishPacket):
        packet_id = publish_packet.variable_header.packet_id
        qos = publish_packet.qos

        incoming_message = IncomingApplicationMessage(packet_id, publish_packet.topic_name, qos, publish_packet.data, publish_packet.retain_flag)
        incoming_message.publish_packet = publish_packet
        await self._handle_message_flow(incoming_message)
        if self.session is not None:
            self.logger.debug("Message queue size: %d" % self.session.delivered_message_queue.qsize())
