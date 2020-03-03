import asyncio
from ..asyncutil import completed
from ..datasvc import BLOCKARRIVE_BASISCODE
import pandas


class UnifiedTransferStatus:
    @classmethod
    def register(cls, subparsers):
        parser = subparsers.add_parser(
            'unifiedtransferstatus',
            help='Check unified transfer status against phedex blockarrive'
        )
        parser.add_argument(
            '-s',
            '--showstatus',
            action='store_true',
            help='Show status of ongoing requests (blockarrive basis codes)',
        )
        parser.set_defaults(command=cls)
        return parser

    def __init__(self, *, unified=None, datasvc=None, args=None, **_):
        self.datasvc = datasvc
        self.unified = unified
        self.args = args
        asyncio.run(self.go())

    async def addinfo(self, reqid, datasets):
        info = await self.datasvc.jsonmethod('requestlist', request=reqid)
        info = info['phedex']['request'][0]
        return (datasets, info)

    def findincomplete(self, datasets, info):
        for dataset, sites in datasets.items():
            for site, completion in sites.items():
                if completion > 100.:
                    print("Overcomplete dataset:", dataset, site)
                elif completion == 100.:
                    continue
                yield info['time_create'], dataset, site

    async def investigate(self, after_time, dataset, site):
        ba = await self.datasvc.jsonmethod('blockarrive', dataset=dataset, to_node=site)
        ba = ba['phedex']['block']
        return (dataset, site, ba)

    async def go(self):
        pendingreq = await self.unified.transfer_statuses()
        addinfos = (self.addinfo(reqid, ds) for reqid, ds in pendingreq.items())
        async for datasets, info in completed(addinfos):
            blockarrives = (self.investigate(*tup) for tup in self.findincomplete(datasets, info))
            async for dataset, site, ba in completed(blockarrives):
                if len(ba) == 0:
                    print("No blockarrives for (request, dataset, site):", info['id'], dataset, site)
                else:
                    basis = pandas.Series(b['destination'][0]['basis'] for b in ba).map(BLOCKARRIVE_BASISCODE)
                    if self.args.showstatus:
                        print("Request", info['id'], "dataset", dataset, "to site", site, "has basiscodes:")
                        print(basis.value_counts())
