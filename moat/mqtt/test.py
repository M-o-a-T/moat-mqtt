"""
This module contains code that helps with DistKV testing.
"""
import os
from contextlib import asynccontextmanager
from functools import partial

import anyio
from distkv.client import client_scope, open_client
from distkv.server import Server as _Server

from .broker import create_broker


class Server(_Server):
    @asynccontextmanager
    async def test_client(self, name=None):
        """
        An async context manager that returns a client that's connected to
        this server.
        """
        async with open_client(
            connect=dict(host="127.0.0.1", port=self.distkv_port, name=name)
        ) as c:
            yield c

    async def test_client_scope(self, name=None):
        return await client_scope(connect=dict(host="127.0.0.1", port=self.distkv_port, name=name))


@asynccontextmanager
async def server(mqtt_port: int = None, distkv_port: int = None):
    """
    An async context manager which creates a stand-alone DistKV server.

    The server has a `test_client` method: an async context manager that
    returns a client taht's connected to this server.

    Ports are allocated based on the current process's PID.
    """
    if mqtt_port is None:
        mqtt_port = 40000 + os.getpid() % 10000
    if distkv_port is None:
        distkv_port = 40000 + (os.getpid() + 1) % 10000

    broker_cfg = {
        "listeners": {"default": {"type": "tcp", "bind": "127.0.0.1:%d" % mqtt_port}},
        "timeout-disconnect-delay": 2,
        "auth": {"allow-anonymous": True, "password-file": None},
    }
    server_cfg = {
        "server": {
            "bind_default": {"host": "127.0.0.1", "port": distkv_port},
            "backend": "mqtt",
            "mqtt": {"uri": "mqtt://127.0.0.1:%d/" % mqtt_port},
        }
    }

    s = Server(name="gpio_test", cfg=server_cfg, init="GPIO")
    async with create_broker(config=broker_cfg) as broker:
        evt = anyio.Event()
        broker._tg.start_soon(partial(s.serve, ready_evt=evt))
        await evt.wait()

        s.distkv_port = distkv_port  # pylint: disable=attribute-defined-outside-init
        yield s


@asynccontextmanager
async def client(mqtt_port: int = None, distkv_port: int = None):
    """
    An async context manager which creates a stand-alone DistKV client.
    """
    async with server(mqtt_port=mqtt_port, distkv_port=distkv_port) as s:
        async with s.test_client() as c:
            yield c
