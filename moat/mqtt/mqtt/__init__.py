# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
from ..errors import MoatMQTTException
from .packet import (
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
from .connect import ConnectPacket
from .connack import ConnackPacket
from .disconnect import DisconnectPacket
from .pingreq import PingReqPacket
from .pingresp import PingRespPacket
from .publish import PublishPacket
from .puback import PubackPacket
from .pubrec import PubrecPacket
from .pubrel import PubrelPacket
from .pubcomp import PubcompPacket
from .subscribe import SubscribePacket
from .suback import SubackPacket
from .unsubscribe import UnsubscribePacket
from .unsuback import UnsubackPacket

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
        raise MoatMQTTException(  # pylint:disable=W0707
            "Unexpected packet Type '%s'" % fixed_header.packet_type
        )
