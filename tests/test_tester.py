import pytest
import trio

from distmqtt.test import test_server,test_client

@pytest.mark.trio
async def test_basic(autojump_clock):
    async with test_client() as c:
        await c.set("test","a","b","c", value=123)
        res = await c.get("test","a","b","c")
        assert res.value == 123
