import logging
import asyncio
from functools import reduce
from operator import add
from ..datasvc import BLOCKARRIVE_BASISCODE
import pandas


logger = logging.getLogger(__name__)


class UnifiedTransferStatus:
    @classmethod
    def register(cls, subparsers):
        parser = subparsers.add_parser(
            "unifiedtransferstatus",
            help="Check unified transfer status against phedex blockarrive",
        )
        parser.set_defaults(command=cls)
        return parser

    def __init__(self, client):
        self.datasvc = client.datasvc
        self.unified = client.unified
        print(asyncio.run(self.go()))

    async def investigate(self, dataset, site, info):
        ba = await self.datasvc.jsonmethod("blockarrive", dataset=dataset, to_node=site)
        ba = ba["phedex"]["block"]
        if len(ba) == 0:
            logger.warning(
                f"No blockarrives for request ID {info['id']}, dataset {dataset} at {site}"
            )
            return []
        basis = pandas.Series(b["destination"][0]["basis"] for b in ba).map(
            BLOCKARRIVE_BASISCODE
        )
        codes = []
        for code, count in basis.value_counts().items():
            codes.append(
                {
                    "request_id": info["id"],
                    "dataset": dataset,
                    "site": site,
                    "basiscode": code,
                    "count": count,
                }
            )
        return codes

    async def find_incomplete(self, reqid, datasets):
        info = await self.datasvc.jsonmethod("requestlist", request=reqid)
        info = info["phedex"]["request"][0]
        tasks = []
        for dataset, sites in datasets.items():
            for site, completion in sites.items():
                if completion > 100.0:
                    logger.warning(f"Overcomplete dataset: {dataset} at {site}")
                elif completion == 100.0:
                    continue
                tasks.append(self.investigate(dataset, site, info))
        return reduce(add, await asyncio.gather(*tasks), [])

    async def go(self):
        pendingrequests = await self.unified.transfer_statuses()
        tasks = []
        for reqid, datasets in pendingrequests.items():
            tasks.append(self.find_incomplete(reqid, datasets))
        return reduce(add, await asyncio.gather(*tasks), [])
