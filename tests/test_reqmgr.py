import pytest
from dmwmclient import Client


@pytest.mark.asyncio
async def test_transitions():
    client = Client()

    df = await client.reqmgr.transitions(outputdataset="/QCD_HT700to1000_TuneCP5_13TeV-madgraph-pythia8/RunIIFall17NanoAODv6-PU2017_12Apr2018_Nano25Oct2019_new_pmx_102X_mc2017_realistic_v7-v1/NANOAODSIM")
    assert set(df.columns) == {'DN', 'Status', 'UpdateTime', 'current', 'mask', 'outputdataset', 'requestname'}
    assert df['current'].sum() == 1
