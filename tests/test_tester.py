import pytest
import trio  # noqa: F401

from distmqtt.test import test_server, test_client

try:
    from distkv.util import P
except ImportError:
    pytestmark = pytest.mark.skip


@pytest.mark.trio
async def test_basic(autojump_clock):  # pylint: disable=unused-argument
    async with test_client() as c:
        await c.set(P("test.a.b.c"), value=123)
        res = await c.get(P("test.a.b.c"))
        assert res.value == 123


@pytest.mark.trio
async def test_split(autojump_clock):  # pylint: disable=unused-argument
    async with test_server() as s:
        async with s.test_client() as c1, s.test_client() as c2:
            await c1.set(P("test.a.b.c"), value=123)
            res = await c2.get(P("test.a.b.c"))
            assert res.value == 123
