# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
from distmqtt.errors import DistMQTTException
from distmqtt.mqtt.packet import (
    CONNECT,
    CONNACK,
    PUBLISH,
    PUBACK,
    PUBREC,
    PUBREL,
    PUBCOMP,
    SUBSCRIBE,
    SUBACK,
    UNSUBSCRIBE,
    UNSUBACK,
    PINGREQ,
    PINGRESP,
    DISCONNECT,
    MQTTFixedHeader,
)
from distmqtt.mqtt.connect import ConnectPacket
from distmqtt.mqtt.connack import ConnackPacket
from distmqtt.mqtt.disconnect import DisconnectPacket
from distmqtt.mqtt.pingreq import PingReqPacket
from distmqtt.mqtt.pingresp import PingRespPacket
from distmqtt.mqtt.publish import PublishPacket
from distmqtt.mqtt.puback import PubackPacket
from distmqtt.mqtt.pubrec import PubrecPacket
from distmqtt.mqtt.pubrel import PubrelPacket
from distmqtt.mqtt.pubcomp import PubcompPacket
from distmqtt.mqtt.subscribe import SubscribePacket
from distmqtt.mqtt.suback import SubackPacket
from distmqtt.mqtt.unsubscribe import UnsubscribePacket
from distmqtt.mqtt.unsuback import UnsubackPacket

packet_dict = {
    CONNECT: ConnectPacket,
    CONNACK: ConnackPacket,
    PUBLISH: PublishPacket,
    PUBACK: PubackPacket,
    PUBREC: PubrecPacket,
    PUBREL: PubrelPacket,
    PUBCOMP: PubcompPacket,
    SUBSCRIBE: SubscribePacket,
    SUBACK: SubackPacket,
    UNSUBSCRIBE: UnsubscribePacket,
    UNSUBACK: UnsubackPacket,
    PINGREQ: PingReqPacket,
    PINGRESP: PingRespPacket,
    DISCONNECT: DisconnectPacket,
}


def packet_class(fixed_header: MQTTFixedHeader):
    try:
        cls = packet_dict[fixed_header.packet_type]
        return cls
    except KeyError:
        raise DistMQTTException(
            "Unexpected packet Type '%s'" % fixed_header.packet_type
        )
