import logging
import anyio
import os
from distmqtt.broker import create_broker

logger = logging.getLogger(__name__)

config = {
    "listeners": {
        "default": {"type": "tcp", "bind": "0.0.0.0:1883"},
        "ws-mqtt": {"bind": "127.0.0.1:8080", "type": "ws", "max_connections": 10},
    },
    "sys_interval": 10,
    "auth": {
        "allow-anonymous": True,
        "password-file": os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "passwd"
        ),
        "plugins": ["auth_file", "auth_anonymous"],
    },
    "topic-check": {
        "enabled": True,
        "plugins": ["topic_acl"],
        "acl": {
            # username: [list of allowed topics]
            "test": ["repositories/+/master", "calendar/#", "data/memes"],
            "anonymous": [],
        },
    },
}


async def test_coro():
    async with create_broker(
        config=config
    ) as broker:  # noqa: F841, pylint: disable=W0612
        while True:
            await anyio.sleep(99999)


if __name__ == "__main__":
    formatter = "[%(asctime)s] :: %(levelname)s :: %(name)s :: %(message)s"
    logging.basicConfig(level=logging.INFO, format=formatter)
    anyio.run(test_coro)
