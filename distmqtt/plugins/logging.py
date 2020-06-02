# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.

from functools import partial


class EventLoggerPlugin:
    def __init__(self, context):
        self.context = context

    async def log_event(self, *args, **kwargs):  # pylint: disable=unused-argument
        self.context.logger.info(
            "### '%s' EVENT FIRED ###", kwargs["event_name"].replace("old", "")
        )

    def __getattr__(self, name):
        if name.startswith("on_"):
            return partial(self.log_event, event_name=name)


class PacketLoggerPlugin:
    def __init__(self, context):
        self.context = context

    async def on_mqtt_packet_received(
        self, *args, **kwargs
    ):  # pylint: disable=unused-argument
        packet = kwargs.get("packet")
        session = kwargs.get("session", None)
        if session:
            self.context.logger.debug("%s <-in-- %r", session.client_id, packet)
        else:
            self.context.logger.debug("<-in-- %r", packet)

    async def on_mqtt_packet_sent(
        self, *args, **kwargs
    ):  # pylint: disable=unused-argument
        packet = kwargs.get("packet")
        session = kwargs.get("session", None)
        if session:
            self.context.logger.debug("%s -out-> %r", session.client_id, packet)
        else:
            self.context.logger.debug("-out-> %r", packet)
