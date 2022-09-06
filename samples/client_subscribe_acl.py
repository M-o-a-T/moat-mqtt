import logging
import anyio

from moat.mqtt.client import open_mqttclient, ClientException
from moat.mqtt.mqtt.constants import QOS_1


#
# This sample shows how to subscbribe a topic and receive data from incoming messages
# It subscribes to '$SYS/broker/uptime' topic and displays the first ten values returned
# by the broker.
#

logger = logging.getLogger(__name__)


async def uptime_coro():
    async with open_mqttclient() as C:
        await C.connect("mqtt://test:test@0.0.0.0:1883")
        # Subscribe to '$SYS/broker/uptime' with QOS=1
        try:
            await C.subscribe(
                [
                    ("data/memes", QOS_1),  # Topic allowed
                    ("data/classified", QOS_1),  # Topic forbidden
                    ("repositories/mqtt/master", QOS_1),  # Topic allowed
                    ("repositories/mqtt/devel", QOS_1),  # Topic forbidden
                    ("calendar/mqtt/releases", QOS_1),  # Topic allowed
                ]
            )
            logger.info("Subscribed")
            for i in range(1, 100):
                message = await C.deliver_message()
                packet = message.publish_packet
                print(
                    "%d: %s => %s"
                    % (i, packet.variable_header.topic_name, str(packet.payload.data))
                )
            await C.unsubscribe(["$SYS/broker/uptime", "$SYS/broker/load/#"])
            logger.info("UnSubscribed")
        except ClientException as ce:
            logger.error("Client exception: %r", ce)


if __name__ == "__main__":
    formatter = "[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=formatter)
    anyio.run(uptime_coro)
