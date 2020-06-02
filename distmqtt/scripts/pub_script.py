# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
"""
distmqtt_pub - MQTT 3.1.1 publisher

Usage:
    distmqtt_pub --version
    distmqtt_pub (-h | --help)
    distmqtt_pub --url BROKER_URL -t TOPIC (-f FILE | -l | -m MESSAGE | -M message | -n | -s | -S) [-C codec] [-c CONFIG_FILE] [-i CLIENT_ID] [-q | --qos QOS] [-d] [-k KEEP_ALIVE] [--clean-session] [--ca-file CAFILE] [--ca-path CAPATH] [--ca-data CADATA] [ --will-topic WILL_TOPIC [--will-message WILL_MESSAGE] [--will-qos WILL_QOS] [--will-retain] ] [--extra-headers HEADER] [-r]

Options:
    -h --help           Show this screen.
    --version           Show version.
    --url BROKER_URL    Broker connection URL (musr conform to MQTT URI scheme (see https://github.com/mqtt/mqtt.github.io/wiki/URI-Scheme>)
    -c CONFIG_FILE      Broker configuration file (YAML format)
    -i CLIENT_ID        Id to use as client ID.
    -q | --qos QOS      Quality of service to use for the message, from 0, 1 and 2. Defaults to 0.
    -r                  Set retain flag on connect
    -t TOPIC            Message topic
    -m MESSAGE          Message data to send
    -M MESSAGE          data to evaluate and send
    -f FILE             Read file by line and publish message for each line
    -s                  Read from stdin and publish message for each line
    -S                  Read from stdin, eval, send the result
    -C codec            Codec to send the result with (default: UTF8-encode)
    -k KEEP_ALIVE       Keep alive timeout in second
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

import sys
import logging
import anyio
import os
import json
import socket
from distmqtt.client import open_mqttclient, ConnectException, _codecs
from distmqtt.version import get_version
from docopt import docopt
from distmqtt.utils import read_yaml_config


logger = logging.getLogger(__name__)


def _gen_client_id():
    pid = os.getpid()
    hostname = socket.gethostname()
    return "distmqtt_pub/%d-%s" % (pid, hostname)


def _get_qos(arguments):
    try:
        return int(arguments["--qos"])
    except Exception:
        return None


def _get_extra_headers(arguments):
    try:
        return json.loads(arguments["--extra-headers"])
    except Exception:
        return {}


def _get_message(arguments):
    codec = arguments["-C"] or "utf-8"
    codec = _codecs[codec]()

    if arguments["-n"]:
        yield b""
    if arguments["-m"]:
        yield arguments["-m"].encode(encoding="utf-8")
    if arguments["-M"]:
        yield codec.encode(eval(arguments["-M"]))  # pylint: disable=eval-used
    if arguments["-f"]:
        try:
            with open(arguments["-f"], "r") as f:
                for line in f:
                    yield line.encode(encoding="utf-8")
        except Exception:
            logger.exception("Failed to read file '%s'", arguments["-f"])
    if arguments["-l"]:
        for line in sys.stdin:
            if line:
                yield line.encode(encoding="utf-8")
    if arguments["-s"]:
        message = bytearray()
        for line in sys.stdin:
            message.extend(line.encode(encoding="utf-8"))
        yield message
    if arguments["-S"]:
        message = sys.stdin.read()
        yield codec.encode(eval(message))  # pylint: disable=eval-used


async def do_pub(client, arguments):
    logger.info("%s Connecting to broker", client.client_id)

    await client.connect(
        uri=arguments["--url"],
        cleansession=arguments["--clean-session"],
        cafile=arguments["--ca-file"],
        capath=arguments["--ca-path"],
        cadata=arguments["--ca-data"],
        extra_headers=_get_extra_headers(arguments),
    )
    try:
        qos = _get_qos(arguments)
        topic = arguments["-t"]
        retain = arguments["-r"]
        async with anyio.create_task_group() as tg:
            for message in _get_message(arguments):
                logger.info("%s Publishing to '%s'", client.client_id, topic)
                await tg.spawn(client.publish, topic, message, qos, retain)
        logger.info("%s Disconnected from broker", client.client_id)
    except KeyboardInterrupt:
        logger.info("%s Disconnected from broker", client.client_id)
    except ConnectException as ce:
        logger.fatal("connection to '%s' failed: %r", arguments["--url"], ce)
    finally:
        async with anyio.fail_after(2, shield=True):
            await client.disconnect()


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
        config["will"]["retain"] = arguments.get("--will-retain", False)

    async with open_mqttclient(client_id=client_id, config=config) as C:
        await do_pub(C, arguments)


if __name__ == "__main__":
    anyio.run(main)
