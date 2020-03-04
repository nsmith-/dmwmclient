import pytest
from dmwmclient import Client


@pytest.mark.asyncio
async def test_cycle():
    client = Client()
    dynamo = client.dynamo

    cycle = await dynamo.latest_cycle()
    assert type(cycle) is dict
    assert set(cycle.keys()) == {'cycle', 'partition_id', 'timestamp', 'comment'}


@pytest.mark.asyncio
async def test_detail():
    client = Client()
    dynamo = client.dynamo

    df = await dynamo.site_detail('T2_PK_NCP', 34069)
    assert set(df.columns) == {'condition', 'condition_id', 'decision', 'name', 'site', 'size'}
    assert df.sum()['size'] == 99787.18272119202
