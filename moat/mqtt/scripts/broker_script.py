# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
"""
MoaT-MQTT - MQTT 3.1.1 broker

Usage:
    moat mqtt broker --help
"""

import logging
import anyio
import os
from contextlib import AsyncExitStack
import asyncclick as click
from ..broker import create_broker
from ..utils import read_yaml_config
from asyncscope import main_scope


default_config = {
    "listeners": {"default": {"type": "tcp", "bind": "0.0.0.0:1883"}},
    "sys_interval": 10,
    "auth": {
        "allow-anonymous": True,
        "password-file": os.path.join(os.path.dirname(os.path.realpath(__file__)), "passwd"),
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
        config = os.path.join(os.path.dirname(os.path.realpath(__file__)), "default_broker.yaml")
        logger.debug("Using default configuration")
    config = read_yaml_config(config)

    try:
        from distkv.util import as_service
    except ImportError:
        as_service = None

    async with AsyncExitStack() as stack:
        await stack.enter_async_context(main_scope())
        await stack.enter_async_context(create_broker(config))
        try:
            from distkv.util import as_service
        except ImportError:
            pass
        else:
            evt = await stack.enter_async_context(as_service())
            evt.set()
        while True:
            await anyio.sleep(99999)


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
