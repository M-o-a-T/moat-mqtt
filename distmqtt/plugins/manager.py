# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.

__all__ = ["BaseContext", "PluginManager"]

import pkg_resources
import logging
import anyio
import copy

from collections import namedtuple

try:
    from contextlib import asynccontextmanager
except ImportError:
    from async_generator import asynccontextmanager
from functools import partial


Plugin = namedtuple("Plugin", ["name", "ep", "object"])


class BaseContext:
    def __init__(self):
        self.config = {}  # compat
        self.logger = None


class PluginManager:
    """
    Wraps setuptools Entry point mechanism to provide a basic plugin system.
    Plugins are loaded for a given namespace (group).
    This plugin manager uses coroutines to run plugin call asynchronously in an event queue
    """

    def __init__(self, tg: anyio.abc.TaskGroup, namespace, context):
        self._tg = tg

        self.logger = logging.getLogger(namespace)
        if context is None:
            self.context = BaseContext()
        else:
            self.context = context
        self._plugins = []
        self._load_plugins(namespace)

    @property
    def app_context(self):
        return self.context

    def _load_plugins(self, namespace):
        self.logger.debug("Loading plugins for namespace %s", namespace)
        plugs = self.context.config.get("plugins", None)
        for ep in pkg_resources.iter_entry_points(group=namespace):
            if plugs is not None and ep.name not in plugs:
                continue
            plugin = self._load_plugin(ep)
            self._plugins.append(plugin)
            self.logger.debug(" Plugin %s ready", plugin.ep.name)

    def _load_plugin(self, ep: pkg_resources.EntryPoint):
        try:
            self.logger.debug(" Loading plugin %s", ep)
            plugin = ep.load(require=True)
            self.logger.debug(" Initializing plugin %s", ep)
            plugin_context = copy.copy(self.app_context)
            plugin_context.logger = self.logger.getChild(ep.name)
            obj = plugin(plugin_context)
            return Plugin(ep.name, ep, obj)
        except ImportError as ie:
            self.logger.warning("Plugin %r import failed: %s", ep, ie)
        except pkg_resources.DistributionNotFound as ue:
            self.logger.warning("Plugin %r dependencies resolution failed: %s", ep, ue)
        except pkg_resources.UnknownExtra as ue:
            self.logger.warning("Plugin %r dependencies resolution failed: %s", ep, ue)

    def get_plugin(self, name):
        """
        Get a plugin by its name from the plugins loaded for the current namespace
        :param name:
        :return:
        """
        for p in self._plugins:
            if p.name == name:
                return p
        return None

    async def close(self):
        """
        Free PluginManager resources and cancel pending event methods
        This method call a close() coroutine for each plugin, allowing plugins to close and free resources
        :return:
        """
        await self.map_plugin_coro("close")

    @property
    def plugins(self):
        """
        Get the loaded plugins list
        :return:
        """
        return self._plugins

    async def fire_event(
        self, event_name, wait=False, *args, **kwargs
    ):  # pylint: disable=W1113
        """
        Fire an event to plugins.
        PluginManager schedule async calls for each plugin on method called "on_" + event_name
        For example, on_connect will be called on event 'connect'
        Method calls are schedule in the async loop. wait parameter must be set to true to wait until all
        mehtods are completed.
        :param event_name:
        :param args:
        :param kwargs:
        :param wait: indicates if fire_event should wait for plugin calls completion (True), or not
        :return:
        """

        @asynccontextmanager
        async def _event_tg(wait):
            if wait:
                async with anyio.create_task_group() as tg:
                    yield tg
            else:
                yield self._tg

        async def _schedule_coro(tg, coro):
            if kwargs:
                coro = partial(coro, **kwargs)
            await tg.spawn(coro, *args)

        async with _event_tg(wait) as tg:
            event_method_name = "on_" + event_name
            for plugin in self._plugins:
                event_method = getattr(plugin.object, event_method_name, None)
                if event_method:
                    try:
                        await _schedule_coro(tg, event_method)

                    except TypeError:
                        self.logger.error(
                            "Method '%s' on plugin '%s' is not a coroutine",
                            event_method_name,
                            plugin.name,
                        )

    async def map(self, coro, *args, **kwargs):
        """
        Schedule a given coroutine call for each plugin.
        The coro called get the Plugin instance as first argument of its method call
        :param coro: coro to call on each plugin
        :param filter_plugins: list of plugin names to filter (only plugin whose name is in filter are called).
        None will call all plugins. [] will call None.
        :param args: arguments to pass to coro
        :param kwargs: arguments to pass to coro
        :return: dict containing return from coro call for each plugin
        """

        async def worker(plugin):
            res = await coro(plugin, *args, **kwargs)
            ret_dict[plugin] = res

        p_list = kwargs.pop("filter_plugins", None)
        if p_list is None:
            p_list = [p.name for p in self.plugins]
        ret_dict = {}
        async with anyio.create_task_group() as tg:
            for plugin in self._plugins:
                if plugin.name in p_list:
                    try:
                        await tg.spawn(worker, plugin)
                    except TypeError:
                        self.logger.error(
                            "Method '%r' on plugin '%s' is not a coroutine",
                            coro,
                            plugin.name,
                        )
        return ret_dict

    @staticmethod
    async def _call_coro(plugin, coro_name, *args, **kwargs):
        try:
            coro = getattr(plugin.object, coro_name, None)(*args, **kwargs)
        except TypeError:
            # Plugin doesn't implement coro_name
            return None
        else:
            return await coro

    async def map_plugin_coro(self, coro_name, *args, **kwargs):
        """
        Call a plugin declared by plugin by its name
        :param coro_name:
        :param args:
        :param kwargs:
        :return:
        """
        return await self.map(self._call_coro, coro_name, *args, **kwargs)
