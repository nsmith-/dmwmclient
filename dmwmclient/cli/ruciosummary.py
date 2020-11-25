import logging
import asyncio
from itertools import chain
from dmwmclient.asyncutil import gather
from matplotlib.ticker import EngFormatter


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

        async def get_sync_usage(rse):
            account = "sync_" + rse.lower()
            usage = await self.client.rucio.getjson(
                f"accounts/{account}/usage/local/{rse}"
            )
            usage = usage[0]
            return {
                "files": usage["files"],
                "used": usage["bytes"],
                "rse": usage["rse"],
                "free": usage["bytes_remaining"],
                "total": usage["bytes_limit"],
                "source": "sync",
            }

        async def get_rse_usage(rse):
            usage = await self.client.rucio.getjson(f"rses/{rse}/usage")
            for item in usage:
                del item["updated_at"]
                del item["rse_id"]
            attr = await self.client.rucio.getjson(f"rses/{rse}/attr/")
            attr = attr[0]
            limits = await self.client.rucio.getjson(f"rses/{rse}/limits")
            limits = limits[0]
            reaper_info = {
                "source": "reaper",
                "rse": rse,
                # this is pretty hacky
                "free": limits.get("MinFreeSpace", None),
            }
            for item in usage:
                if item["source"] == attr.get("source_for_used_space", "storage"):
                    reaper_info["used"] = item["used"]
                if item["source"] == attr.get("source_for_total_space", "storage"):
                    reaper_info["total"] = item["total"]
            reaper_info.setdefault("total", float(attr["ddm_quota"]))
            usage.append(reaper_info)
            usage.append(await get_sync_usage(rse))
            return usage

        rse_usage = (
            pd.json_normalize(
                chain.from_iterable(await gather(map(get_rse_usage, ddm_rses), 5))
            )
            .set_index(["rse", "source"])
            .unstack()
        )

        async def get_account_usage(account):
            usage = await self.client.rucio.getjson(f"accounts/{account}/usage/local")
            usage = pd.json_normalize(usage)
            return pd.DataFrame(
                {
                    "files": usage["files"],
                    "used": usage["bytes"],
                    "rse": usage["rse"],
                    "free": usage["bytes_remaining"],
                    "total": usage["bytes_limit"],
                    "source": account,
                }
            )

        accounts = [
            "transfer_ops",
            "wma_prod",
            "wmcore_transferor",
            "wmcore_output",
        ]
        account_usage = (
            pd.concat(await gather(map(get_account_usage, accounts), 1))
            .set_index(["rse", "source"])
            .unstack()
            .loc[ddm_rses]
        )
        usage = pd.concat([rse_usage, account_usage], axis=1)
        usage.loc["Total"] = usage.sum()
        usage.to_pickle(f"{self.out}/rucio_summary.pkl.gz")

        volume = pd.DataFrame(
            {
                "Locked": usage["used", "rucio"] - usage["used", "expired"].fillna(0),
                "Dynamic": usage["used", "expired"] - usage["used", "obsolete"],
                "Obsolete": usage["used", "obsolete"],
            }
        ).fillna(0)
        volume_colors = {
            "Locked": "lightblue",
            "Dynamic": "lightgreen",
            "Obsolete": "tomato",
        }

        account_colors = {
            "transfer_ops": "orange",
            "wma_prod": "red",
            "wmcore_transferor": "green",
            "wmcore_output": "blue",
            "sync": "grey",
        }
        rule_volume = usage["used"].filter(account_colors, axis=1).fillna(0)

        formatter = EngFormatter(unit="B")

        fig, ax = plt.subplots(figsize=(15, 5))
        ax.yaxis.set_major_formatter(formatter)
        volume.plot.bar(ax=ax, stacked=True, color=volume_colors, width=0.9)
        rule_volume.plot.bar(ax=ax, color=account_colors, width=0.9)
        ax.set_xlabel("RSE")
        ax.set_ylabel("Used volume")
        ax.legend(title="Source")
        fig.savefig(f"{self.out}/rucio_summary_absolute.pdf", bbox_inches="tight")

        fig, ax = plt.subplots(figsize=(15, 5))
        limit = usage["total", "reaper"]
        target = 1 - usage["free", "reaper"] / usage["total", "reaper"]
        volume.divide(limit, axis=0).plot.bar(
            ax=ax, stacked=True, color=volume_colors, width=0.9
        )
        ax.plot(
            target, marker=5, color="black", linestyle="none", label="Target occupancy"
        )
        rule_volume.divide(limit, axis=0).plot.bar(
            ax=ax, color=account_colors, width=0.9
        )
        ax.set_xlabel("RSE")
        ax.set_ylabel("Usage relative to reaper limit")
        ax.axhline(1, linestyle="dotted", color="black")
        ax.legend(title="Source")
        fig.savefig(f"{self.out}/rucio_summary_relative.pdf", bbox_inches="tight")

        fig, ax = plt.subplots(figsize=(15, 5))
        mstransferor = (
            account_usage["used", "wmcore_transferor"]
            / account_usage["total", "wmcore_transferor"]
        )
        mstransferor.plot.bar(ax=ax)
        ax.set_xlabel("RSE")
        ax.set_ylabel("MSTransferor usage relative to quota")
        ax.axhline(1, linestyle="dotted", color="black")
        fig.savefig(f"{self.out}/rucio_summary_mstransferor.pdf", bbox_inches="tight")

        fig, ax = plt.subplots(figsize=(15, 5))
        wmaprod = account_usage["used", "wma_prod"] / 1e15
        wmaprod.plot.bar(ax=ax)
        ax.set_xlabel("RSE")
        ax.set_ylabel("WMAgent usage [PB]")
        fig.savefig(f"{self.out}/rucio_summary_wma_prod.pdf", bbox_inches="tight")
