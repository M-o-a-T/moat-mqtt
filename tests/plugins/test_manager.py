# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import unittest
import pytest
import logging
import anyio
from distmqtt.plugins.manager import PluginManager

formatter = (
    "[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
)
logging.basicConfig(level=logging.INFO, format=formatter)

pytestmark = pytest.mark.skip


class SimpleTestPlugin:
    def __init__(self, context):
        self.context = context


class EventTestPlugin:
    def __init__(self, context):
        self.context = context
        self.test_flag = False
        self.coro_flag = False

    async def on_test(self, *args, **kwargs):  # pylint: disable=unused-argument
        self.test_flag = True
        self.context.logger.info("on_test")

    async def test_coro(self, *args, **kwargs):  # pylint: disable=unused-argument
        self.coro_flag = True

    async def ret_coro(self, *args, **kwargs):  # pylint: disable=unused-argument
        return "TEST"


class TestPluginManager(unittest.TestCase):
    def test_load_plugin(self):
        async def coro():
            async with anyio.create_task_group() as tg:
                manager = PluginManager(tg, "distmqtt.test.plugins", context=None)
                self.assertTrue(len(manager._plugins) > 0)

        anyio.run(coro)

    def test_fire_event(self):
        async def fire_event(manager):
            await manager.fire_event("test")
            await anyio.sleep(1)
            await manager.close()

        async def coro():
            async with anyio.create_task_group() as tg:
                manager = PluginManager(tg, "distmqtt.test.plugins", context=None)
                await fire_event(manager)
                plugin = manager.get_plugin("event_plugin")
                self.assertTrue(plugin.object.test_flag)

        anyio.run(coro)

    def test_fire_event_wait(self):
        async def fire_event(manager):
            await manager.fire_event("test", wait=True)
            await manager.close()

        async def coro():
            async with anyio.create_task_group() as tg:
                manager = PluginManager(tg, "distmqtt.test.plugins", context=None)
                await fire_event(manager)
                plugin = manager.get_plugin("event_plugin")
                self.assertTrue(plugin.object.test_flag)

        anyio.run(coro)

    def test_map_coro(self):
        async def call_coro(manager):
            await manager.map_plugin_coro("test_coro")

        async def coro():
            async with anyio.create_task_group() as tg:
                manager = PluginManager(tg, "distmqtt.test.plugins", context=None)
                await call_coro(manager)
                plugin = manager.get_plugin("event_plugin")
                self.assertTrue(plugin.object.test_coro)

        anyio.run(coro)

    def test_map_coro_return(self):
        async def call_coro(manager):
            return await manager.map_plugin_coro("ret_coro")

        async def coro():
            async with anyio.create_task_group() as tg:
                manager = PluginManager(tg, "distmqtt.test.plugins", context=None)
                ret = await call_coro(manager)
                plugin = manager.get_plugin("event_plugin")
                self.assertEqual(ret[plugin], "TEST")

        anyio.run(coro)

    def test_map_coro_filter(self):
        """
        Run plugin coro but expect no return as an empty filter is given
        :return:
        """

        async def call_coro(manager):
            return await manager.map_plugin_coro("ret_coro", filter_plugins=[])

        async def coro():
            async with anyio.create_task_group() as tg:
                manager = PluginManager(tg, "distmqtt.test.plugins", context=None)
                ret = await call_coro(manager)
                self.assertTrue(len(ret) == 0)

        anyio.run(coro)
