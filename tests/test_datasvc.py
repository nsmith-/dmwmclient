import pytest
from dmwmclient import Client


@pytest.mark.asyncio
async def test_ping():
    client = Client()

    res = await client.datasvc.jsonmethod("bounce", asdf="hi there")
    assert res['phedex']['bounce'] == {'asdf': 'hi there'}
