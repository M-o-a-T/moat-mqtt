# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.

import unittest
import logging
import os
import anyio
from distmqtt.plugins.manager import BaseContext
from distmqtt.plugins.authentication import AnonymousAuthPlugin, FileAuthPlugin
from distmqtt.session import Session

formatter = (
    "[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
)
logging.basicConfig(level=logging.DEBUG, format=formatter)


class TestAnonymousAuthPlugin(unittest.TestCase):
    def test_allow_anonymous(self):
        context = BaseContext()
        context.logger = logging.getLogger(__name__)
        context.config = {"auth": {"allow-anonymous": True}}

        async def coro():
            s = Session(None)
            s.username = ""
            auth_plugin = AnonymousAuthPlugin(context)
            ret = await auth_plugin.authenticate(session=s)
            self.assertTrue(ret)

        anyio.run(coro)

    def test_disallow_anonymous(self):
        context = BaseContext()
        context.logger = logging.getLogger(__name__)
        context.config = {"auth": {"allow-anonymous": False}}

        async def coro():
            s = Session(None)
            s.username = ""
            auth_plugin = AnonymousAuthPlugin(context)
            ret = await auth_plugin.authenticate(session=s)
            self.assertFalse(ret)

        anyio.run(coro)

    def test_allow_nonanonymous(self):
        context = BaseContext()
        context.logger = logging.getLogger(__name__)
        context.config = {"auth": {"allow-anonymous": False}}

        async def coro():
            s = Session(None)
            s.username = "test"
            auth_plugin = AnonymousAuthPlugin(context)
            ret = await auth_plugin.authenticate(session=s)
            self.assertTrue(ret)

        anyio.run(coro)


class TestFileAuthPlugin(unittest.TestCase):
    def test_allow(self):
        context = BaseContext()
        context.logger = logging.getLogger(__name__)
        context.config = {
            "auth": {
                "password-file": os.path.join(
                    os.path.dirname(os.path.realpath(__file__)), "passwd"
                )
            }
        }

        async def coro():
            s = Session(None)
            s.username = "user"
            s.password = "test"
            auth_plugin = FileAuthPlugin(context)
            ret = await auth_plugin.authenticate(session=s)
            self.assertTrue(ret)

        anyio.run(coro)

    def test_wrong_password(self):
        context = BaseContext()
        context.logger = logging.getLogger(__name__)
        context.config = {
            "auth": {
                "password-file": os.path.join(
                    os.path.dirname(os.path.realpath(__file__)), "passwd"
                )
            }
        }

        async def coro():
            s = Session(None)
            s.username = "user"
            s.password = "wrong password"
            auth_plugin = FileAuthPlugin(context)
            ret = await auth_plugin.authenticate(session=s)
            self.assertFalse(ret)

        anyio.run(coro)

    def test_unknown_password(self):
        context = BaseContext()
        context.logger = logging.getLogger(__name__)
        context.config = {
            "auth": {
                "password-file": os.path.join(
                    os.path.dirname(os.path.realpath(__file__)), "passwd"
                )
            }
        }

        async def coro():
            s = Session(None)
            s.username = "some user"
            s.password = "some password"
            auth_plugin = FileAuthPlugin(context)
            ret = await auth_plugin.authenticate(session=s)
            self.assertFalse(ret)

        anyio.run(coro)
