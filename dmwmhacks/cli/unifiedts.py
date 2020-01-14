import asyncio
from ..asyncutil import completed
import pandas


class UnifiedTransferStatus:
    @classmethod
    def register(cls, subparsers):
        parser = subparsers.add_parser(
            'unifiedts',
            help='Check unified transfer status against phedex blockarrive'
        )
        parser.set_defaults(command=cls)
        return parser

    def __init__(self, *, unified=None, datasvc=None, **_):
        self.datasvc = datasvc
        self.unified = unified
        asyncio.run(self.go())

    async def addinfo(self, reqid, datasets):
        info = await self.datasvc.jsonmethod('requestlist', request=reqid)
        info = info['phedex']['request'][0]
        return (datasets, info)

    def findincomplete(self, datasets, info):
        print("Looking for incompletes in req#", info['id'], "created at", pandas.to_datetime(info['time_create'], unit='s'))
        for dataset, sites in datasets.items():
            for site, completion in sites.items():
                if completion > 100.:
                    print("Overcomplete dataset:", dataset, site)
                elif completion == 100.:
                    continue
                yield info['time_create'], dataset, site

    async def investigate(self, after_time, dataset, site):
        ba = await self.datasvc.jsonmethod('blockarrive', dataset=dataset, to_node=site)
        return (dataset, site, ba)

    async def go(self):
        pendingreq = await self.unified.transfer_statuses()
        addinfos = (self.addinfo(reqid, ds) for reqid, ds in pendingreq.items())
        async for datasets, info in completed(addinfos):
            blockarrives = (self.investigate(*tup) for tup in self.findincomplete(datasets, info))
            async for dataset, site, ba in completed(blockarrives):
                if len(ba) == 0:
                    print("No blockarrives for incomplete dataset:", dataset, site)
