# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
"""
DistMQTT - MQTT 3.1.1 broker

Usage:
    distmqtt --help
"""

import logging
import anyio
import os
import asyncclick as click
from distmqtt.broker import create_broker
from distmqtt.utils import read_yaml_config


default_config = {
    "listeners": {"default": {"type": "tcp", "bind": "0.0.0.0:1883"}},
    "sys_interval": 10,
    "auth": {
        "allow-anonymous": True,
        "password-file": os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "passwd"
        ),
        "plugins": ["auth_file", "auth_anonymous"],
    },
    "topic-check": {"enabled": False},
}

logger = logging.getLogger(__name__)


@click.command()
@click.version_option()
@click.option("-c", "--config", help="Name of config file (YAML)")
@click.option("-d", "--debug", is_flag=True, help="Debug?")
async def main(config, debug):
    formatter = "[%(asctime)s] :: %(levelname)s - %(message)s"

    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level, format=formatter)

    if not config:
        config = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "default_broker.yaml"
        )
        logger.debug("Using default configuration")
    config = read_yaml_config(config)

    from distkv.util import as_service

    async with as_service() as evt:
        async with create_broker(config):
            await evt.set()
            while True:
                await anyio.sleep(99999)


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
