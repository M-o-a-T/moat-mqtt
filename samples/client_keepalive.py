import logging
import anyio

from distmqtt.client import open_mqttclient


#
# This sample shows a client running idle.
# Meanwhile, keepalive is managed through PING messages sent every 5 seconds
#


logger = logging.getLogger(__name__)

config = {"keep_alive": 5, "ping_delay": 1}


async def test_coro():
    async with open_mqttclient() as C:
        await C.connect("mqtt://test.mosquitto.org:1883/")
        await anyio.sleep(18)


if __name__ == "__main__":
    formatter = "[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.DEBUG, format=formatter)
    anyio.run(test_coro)
