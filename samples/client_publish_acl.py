import logging
import anyio

from distmqtt.client import open_mqttclient, ConnectException


#
# This sample shows how to publish messages to broker using different QOS
# Debug outputs shows the message flows
#

logger = logging.getLogger(__name__)


async def test_coro():
    try:
        async with open_mqttclient() as C:
            await C.connect("mqtt://0.0.0.0:1883")

            await C.publish("data/classified", b"TOP SECRET", qos=0x01)
            await C.publish("data/memes", b"REAL FUN", qos=0x01)
            await C.publish(
                "repositories/distmqtt/master", b"NEW STABLE RELEASE", qos=0x01
            )
            await C.publish(
                "repositories/distmqtt/devel", b"THIS NEEDS TO BE CHECKED", qos=0x01
            )
            await C.publish("calendar/distmqtt/releases", b"NEW RELEASE", qos=0x01)
            logger.info("messages published")
    except ConnectException as ce:
        logger.error("Connection failed: %r", ce)


if __name__ == "__main__":
    formatter = (
        "[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
    )
    formatter = "%(message)s"
    logging.basicConfig(level=logging.DEBUG, format=formatter)
    anyio.run(test_coro)
