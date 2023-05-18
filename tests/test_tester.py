import pytest
import trio  # noqa: F401
from asyncscope import main_scope
from moat.util import P

from moat.mqtt.test import client, server


@pytest.mark.trio
async def test_basic(autojump_clock):  # pylint: disable=unused-argument
    async with client() as c:
        await c.set(P("test.a.b.c"), value=123)
        res = await c.get(P("test.a.b.c"))
        assert res.value == 123


@pytest.mark.trio
async def test_split(autojump_clock):  # pylint: disable=unused-argument
    async with main_scope(), server() as s:
        c1 = await s.test_client_scope(name="c_A")
        c2 = await s.test_client_scope(name="c_B")
        await c1.set(P("test.a.b.c"), value=123)
        res = await c2.get(P("test.a.b.c"))
        assert res.value == 123
