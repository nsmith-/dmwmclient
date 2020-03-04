import httpx
import pandas
import datetime


class Dynamo:
    """Dynamo client"""

    defaults = {
        "dynamo_base": "http://dynamo.mit.edu/data/",
    }

    def __init__(self, client, dynamo_base=None):
        if dynamo_base is None:
            dynamo_base = Dynamo.defaults["dynamo_base"]
        self.client = client
        self.baseurl = httpx.URL(dynamo_base)

    async def latest_cycle(self, partition_id=10):
        """Get the latest cycle information"""
        result = await self.client.getjson(self.baseurl.join("detox/cycles"))
        for cycle in result["data"][::-1]:
            if cycle["partition_id"] == partition_id:
                cycle["timestamp"] = datetime.datetime.fromtimestamp(cycle["timestamp"])
                return cycle

    async def site_detail(self, site, cycle):
        """Get a dataframe of site usage from detox"""
        params = {"site": site, "cycle": cycle}
        result = await self.client.getjson(
            self.baseurl.join("detox/sitedetail"), params=params
        )
        out = pandas.io.json.json_normalize(result["data"]["content"]["datasets"])
        out["condition"] = out["condition_id"].map(
            {int(k): v for k, v in result["data"]["conditions"].items()}
        )
        out["site"] = site
        return out
