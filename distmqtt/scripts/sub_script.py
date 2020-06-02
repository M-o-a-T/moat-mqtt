# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
"""
distmqtt_sub - MQTT 3.1.1 publisher

Usage:
    distmqtt_sub --version
    distmqtt_sub (-h | --help)
    distmqtt_sub --url BROKER_URL -t TOPIC... [-n COUNT] [-c CONFIG_FILE] [-C codec] [-i CLIENT_ID] [-q | --qos QOS] [-d] [-k KEEP_ALIVE] [--clean-session] [--ca-file CAFILE] [--ca-path CAPATH] [--ca-data CADATA] [ --will-topic WILL_TOPIC [--will-message WILL_MESSAGE] [--will-qos WILL_QOS] [--will-retain] ] [--extra-headers HEADER]

Options:
    -h --help           Show this screen.
    --version           Show version.
    --url BROKER_URL    Broker connection URL (musr conform to MQTT URI scheme (see https://github.com/mqtt/mqtt.github.io/wiki/URI-Scheme>)
    -c CONFIG_FILE      Broker configuration file (YAML format)
    -i CLIENT_ID        Id to use as client ID.
    -n COUNT            Number of messages to read before ending.
    -q | --qos QOS      Quality of service desired to receive messages, from 0, 1 and 2. Defaults to 0.
    -t TOPIC...         Topic filter to subcribe
    -k KEEP_ALIVE       Keep alive timeout in second
    -C codec            use this codec
    --clean-session     Clean session on connect (defaults to False)
    --ca-file CAFILE    CA file
    --ca-path CAPATH    CA Path
    --ca-data CADATA    CA data
    --will-topic WILL_TOPIC
    --will-message WILL_MESSAGE
    --will-qos WILL_QOS
    --will-retain
    --extra-headers EXTRA_HEADERS      JSON object with key-value pairs of additional headers for websocket connections
    -d                  Enable debug messages
"""

import logging
import anyio
import os
import json
import socket
from distmqtt.client import open_mqttclient, ConnectException
from distmqtt.version import get_version
from docopt import docopt
from distmqtt.mqtt.constants import QOS_0
from distmqtt.utils import read_yaml_config

logger = logging.getLogger(__name__)


def _gen_client_id():
    pid = os.getpid()
    hostname = socket.gethostname()
    return "distmqtt_sub/%d-%s" % (pid, hostname)


def _get_qos(arguments):
    try:
        return int(arguments["--qos"])
    except KeyError:
        return QOS_0


def _get_extra_headers(arguments):
    try:
        return json.loads(arguments["--extra-headers"])
    except Exception:
        return {}


async def do_sub(client, arguments):

    try:
        await client.connect(
            uri=arguments["--url"],
            cleansession=arguments["--clean-session"],
            cafile=arguments["--ca-file"],
            capath=arguments["--ca-path"],
            cadata=arguments["--ca-data"],
            extra_headers=_get_extra_headers(arguments),
        )
        async with anyio.create_task_group() as tg:
            for topic in arguments["-t"]:
                await tg.spawn(run_sub, client, topic, arguments)

    except KeyboardInterrupt:
        pass
    except ConnectException as ce:
        logger.fatal("connection to '%s' failed: %r", arguments["--url"], ce)
    finally:
        async with anyio.fail_after(2, shield=True):
            await client.disconnect()


async def run_sub(client, topic, arguments):
    qos = _get_qos(arguments)
    if arguments["-n"]:
        max_count = int(arguments["-n"])
    else:
        max_count = None
    count = 0

    async with client.subscription(topic, qos) as sub:
        async for message in sub:
            count += 1
            print(message.topic, message.data, sep="\t")
            if max_count and count >= max_count:
                break


async def main():
    arguments = docopt(__doc__, version=get_version())
    # print(arguments)
    formatter = "[%(asctime)s] :: %(levelname)s - %(message)s"

    if arguments["-d"]:
        level = logging.DEBUG
    else:
        level = logging.INFO
    logging.basicConfig(level=level, format=formatter)

    config = None
    if arguments["-c"]:
        config = read_yaml_config(arguments["-c"])
    else:
        config = read_yaml_config(
            os.path.join(
                os.path.dirname(os.path.realpath(__file__)), "default_client.yaml"
            )
        )
        logger.debug(
            os.path.join(
                os.path.dirname(os.path.realpath(__file__)), "default_client.yaml"
            )
        )
        logger.debug("Using default configuration")

    client_id = arguments.get("-i", None)
    if not client_id:
        client_id = _gen_client_id()

    if arguments["-k"]:
        config["keep_alive"] = int(arguments["-k"])

    if arguments["--will-topic"] and arguments["--will-message"]:
        config["will"] = dict()
        config["will"]["topic"] = arguments["--will-topic"]
        config["will"]["message"] = arguments["--will-message"].encode("utf-8")
        config["will"]["qos"] = _get_qos(arguments)
        config["will"]["retain"] = arguments["--will-retain"]

    async with open_mqttclient(
        client_id=client_id, config=config, codec=arguments["-C"]
    ) as C:
        await do_sub(C, arguments)


if __name__ == "__main__":
    anyio.run(main)
