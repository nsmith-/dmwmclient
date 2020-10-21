import logging
import asyncio
from itertools import chain
from dmwmclient.asyncutil import gather


logger = logging.getLogger(__name__)


class RucioSummary:
    @classmethod
    def register(cls, subparsers):
        parser = subparsers.add_parser(
            "ruciosummary",
            help="Build summary statistics for rucio and output",
        )
        parser.add_argument(
            "-o",
            "--out",
            default=".",
            type=str,
            help="Output directory (default: %(default)s)",
        )
        parser.set_defaults(command=cls)
        return parser

    def __init__(self, client, args):
        self.client = client
        self.out = args.out
        asyncio.get_event_loop().run_until_complete(self.go())

    async def go(self):
        import pandas as pd
        import matplotlib.pyplot as plt

        self.client.rucio.account = "transfer_ops"

        ddm_rses = await self.client.rucio.getjson(
            "rses/", params={"expression": "(rse_type=DISK)&(ddm_quota>0)"}
        )
        ddm_rses = sorted(item["rse"] for item in ddm_rses)

        async def get_usage(rse):
            usage = await self.client.rucio.getjson(f"rses/{rse}/usage")
            for item in usage:
                del item["updated_at"]
                del item["rse_id"]
                del item["files"]
            return usage

        ddm_rse_usage = (
            pd.json_normalize(
                chain.from_iterable(await gather(map(get_usage, ddm_rses), 5))
            )
            .set_index(["rse", "source"])
            .unstack()
            .fillna(0)
        )

        async def get_quota(rse):
            info = await self.client.rucio.getjson(f"rses/{rse}/attr/")
            return info[0]["ddm_quota"]

        async def get_sync_usage(rse):
            account = "sync_" + rse.lower()
            usage = await self.client.rucio.getjson(
                f"accounts/{account}/usage/local/{rse}"
            )
            return usage[0]["bytes"]

        ddm_usage = pd.DataFrame(
            {
                "rse": ddm_rses,
                "ddm_quota": await gather(map(get_quota, ddm_rses), 5),
                "sync": await gather(map(get_sync_usage, ddm_rses), 5),
            }
        ).set_index("rse")

        async def account_usage(account):
            usage = await self.client.rucio.getjson(f"accounts/{account}/usage/local")
            usage = pd.json_normalize(usage)
            usage["account"] = account
            del usage["rse_id"]
            del usage["bytes_remaining"]
            return usage

        accounts = [
            "transfer_ops",
            "wma_prod",
            "wmcore_transferor",
            "wmcore_output",
        ]
        usage = pd.concat(await gather(map(account_usage, accounts), 1))
        usage = usage.set_index(["rse", "account"]).unstack()
        usage = usage.loc[ddm_rses]
        usage.loc[:, ("bytes", "sync")] = ddm_usage["sync"]
        usage.loc[:, ("bytes_limit", "ddm_quota")] = ddm_usage["ddm_quota"]
        usage = pd.concat([usage, ddm_rse_usage], axis=1).astype(float)

        fig, ax = plt.subplots(figsize=(15, 5))
        usagepb = usage.fillna(0) / 1e15
        usagepb.plot.bar(
            y=("used", "rucio"),
            ax=ax,
            color="black",
            alpha=0.3,
            width=0.8,
            label="Total",
        )
        usagepb.plot.bar(y="bytes", ax=ax, width=0.8)
        ax.set_xlabel("RSE")
        ax.set_ylabel("Petabytes")
        fig.savefig(f"{self.out}/rucio_summary_absolute.pdf", bbox_inches="tight")

        fig, ax = plt.subplots(figsize=(15, 5))
        usagerel = usage.divide(usage["bytes_limit", "ddm_quota"], axis=0)
        usagerel.plot.bar(
            y=("used", "rucio"),
            ax=ax,
            color="black",
            alpha=0.3,
            width=0.8,
            label="Total",
        )
        usagerel.plot.bar(y="bytes", ax=ax, width=0.8)
        ax.set_xlabel("RSE")
        ax.set_ylabel("Usage relative to DDM quota")
        ax.axhline(1, linestyle="dotted", color="black")
        ax.axhline(1 / 0.8, linestyle="dotted", color="orange")
        fig.savefig(f"{self.out}/rucio_summary_relative.pdf", bbox_inches="tight")

        fig, ax = plt.subplots(figsize=(15, 5))
        mstransferor = (
            usage["bytes", "wmcore_transferor"]
            / usage["bytes_limit", "wmcore_transferor"]
        )
        mstransferor.plot.bar(ax=ax)
        ax.set_xlabel("RSE")
        ax.set_ylabel("MSTransferor usage relative to quota")
        ax.axhline(1, linestyle="dotted", color="black")
        fig.savefig(f"{self.out}/rucio_summary_mstransferor.pdf", bbox_inches="tight")

        fig, ax = plt.subplots(figsize=(15, 5))
        wmaprod = usage["bytes", "wma_prod"] / 1e15
        wmaprod.plot.bar(ax=ax)
        ax.set_xlabel("RSE")
        ax.set_ylabel("WMAgent usage [PB]")
        fig.savefig(f"{self.out}/rucio_summary_wma_prod.pdf", bbox_inches="tight")

        usage.to_pickle(f"{self.out}/rucio_summary.pkl.gz")
