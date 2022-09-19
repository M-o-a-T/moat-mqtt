# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
from ..errors import MoatMQTTException
from .packet import DISCONNECT, MQTTFixedHeader, MQTTPacket


class DisconnectPacket(MQTTPacket):
    VARIABLE_HEADER = None
    PAYLOAD = None

    def __init__(self, fixed: MQTTFixedHeader = None):
        if fixed is None:
            header = MQTTFixedHeader(DISCONNECT, 0x00)
        else:
            if fixed.packet_type != DISCONNECT:
                raise MoatMQTTException(
                    "Invalid fixed packet type %s for DisconnectPacket init" % fixed.packet_type
                )
            header = fixed
        super().__init__(header)
        self.variable_header = None
        self.payload = None
