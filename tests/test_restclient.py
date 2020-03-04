import pytest
from dmwmclient import RESTClient


@pytest.mark.asyncio
async def test_basic():
    client = RESTClient()

    res = await client.getjson("http://httpbin.org/headers")
    print(res)
