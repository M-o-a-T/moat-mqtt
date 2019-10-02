# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
"""
DistMQTT - MQTT 3.1.1 broker

Usage:
    distmqtt --version
    distmqtt (-h | --help)
    distmqtt [-c <config_file> ] [-d]

Options:
    -h --help           Show this screen.
    --version           Show version.
    -c <config_file>    Broker configuration file (YAML format)
    -d                  Enable debug messages
"""

import sys
import logging
import anyio
import os
from distmqtt.broker import create_broker
from distmqtt.version import get_version
from docopt import docopt
from distmqtt.utils import read_yaml_config


default_config = {
    'listeners': {
        'default': {
            'type': 'tcp',
            'bind': '0.0.0.0:1883',
        },
    },
    'sys_interval': 10,
    'auth': {
        'allow-anonymous': True,
        'password-file': os.path.join(os.path.dirname(os.path.realpath(__file__)), "passwd"),
        'plugins': [
            'auth_file', 'auth_anonymous'
        ]
    },
    'topic-check': {
        'enabled': False
    }
}

logger = logging.getLogger(__name__)


async def main(*args, **kwargs):
    arguments = docopt(__doc__, version=get_version())
    formatter = "[%(asctime)s] :: %(levelname)s - %(message)s"

    if arguments['-d']:
        level = logging.DEBUG
    else:
        level = logging.INFO
    logging.basicConfig(level=level, format=formatter)

    config = None
    if arguments['-c']:
        config = read_yaml_config(arguments['-c'])
    else:
        config = read_yaml_config(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'default_broker.yaml'))
        logger.debug("Using default configuration")

    async with create_broker(config) as broker:
        while True:
            await anyio.sleep(99999)


if __name__ == "__main__":
    anyio.run(main)
