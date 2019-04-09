import logging
import anyio

from hbmqtt.client import MQTTClient
from hbmqtt.mqtt.constants import QOS_1, QOS_2


#
# This sample shows how to publish messages to broker using different QOS
# Debug outputs shows the message flows
#

logger = logging.getLogger(__name__)

config = {
    'will': {
        'topic': '/will/client',
        'message': b'Dead or alive',
        'qos': 0x01,
        'retain': True
    },
    'capath': '.',
}
C = MQTTClient(config=config)
#C = MQTTClient()


async def test_coro():
    await C.connect('wss://test.mosquitto.org:8081/', cafile='mosquitto.org.crt')
    try:
        with anyio.open_task_group() as tg:
            await tg.spawn(C.publish('a/b', b'TEST MESSAGE WITH QOS_0')
            await tg.spawn(C.publish('a/b', b'TEST MESSAGE WITH QOS_1', qos=QOS_1)
            await tg.spawn(C.publish('a/b', b'TEST MESSAGE WITH QOS_2', qos=QOS_2)
        logger.info("messages published")
    finally:
        await C.disconnect()


if __name__ == '__main__':
    formatter = "[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.DEBUG, format=formatter)
    anyio.run(test_coro)
