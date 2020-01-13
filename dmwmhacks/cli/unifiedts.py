import asyncio


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

    async def go(self):
        pendingreq = await self.unified.transfer_statuses()

        reqinfos = []
        for reqid in pendingreq:
            coro = self.datasvc.jsonmethod('transferrequests', request=reqid)
            reqinfos.append(coro)
        reqinfos = await asyncio.gather(*reqinfos)

        incomplete = []
        for ds_site, reqinfo in zip(pendingreq.values(), reqinfos):
            reqinfo = reqinfo['phedex']['request'][0]
            print(reqinfo['id'], reqinfo['time_start'])
            for dataset, sites in ds_site.items():
                for site, completion in sites.items():
                    if completion > 100.:
                        print("Overcomplete?", dataset, site)
                    elif completion == 100.:
                        continue
                    coro = self.datasvc.jsonmethod(
                        'blockarrive',
                        dataset=dataset,
                        to_node=site,
                    )
                    incomplete.append(coro)
        incomplete = await asyncio.gather(*incomplete)

        for ba in incomplete:
            ba = ba['phedex']['block']
            print(ba)
