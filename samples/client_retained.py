import logging
import anyio

from distmqtt.client import open_mqttclient
from distmqtt.mqtt.constants import QOS_0, QOS_1, QOS_2


#
# This sample shows how to publish messages to broker using different QOS
# Debug outputs shows the message flows
#

logger = logging.getLogger(__name__)

config = {}


async def test_coro():
    async with open_mqttclient(config=config) as sub_client:
        await sub_client.connect("mqtt://127.0.0.1", cleansession=False)
        await sub_client.subscribe(
            [("/qos0", QOS_0), ("/qos1", QOS_1), ("/qos2", QOS_2)]
        )
        await sub_client.disconnect()
        await anyio.sleep(0.1)

        await _client_publish("/qos0", b"data", QOS_0, retain=True)
        await _client_publish("/qos1", b"data", QOS_1, retain=True)
        await _client_publish("/qos2", b"data", QOS_2, retain=True)
        import pdb

        pdb.set_trace()
        await sub_client.reconnect()
        for qos in [QOS_0, QOS_1, QOS_2]:
            logger.debug("TEST QOS: %d", qos)
            message = await sub_client.deliver_message()
            logger.debug("Message: %r", message.publish_packet)
    await anyio.sleep(0.1)


async def _client_publish(topic, data, qos, retain=False):
    async with open_mqttclient() as pub_client:
        ret = await pub_client.connect("mqtt://127.0.0.1/")
        ret = await pub_client.publish(topic, data, qos, retain)
    return ret


if __name__ == "__main__":
    formatter = (
        "[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
    )
    # formatter = "%(message)s"
    logging.basicConfig(level=logging.DEBUG, format=formatter)
    anyio.run(test_coro)
