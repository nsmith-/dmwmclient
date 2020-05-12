import httpx
import pandas
from .util import format_dates


class MSMgr:
    """MSManager client"""
    defaults = {
        "msmgr_base": "https://cmsweb.cern.ch/ms-transferor/data/",
    }

    def __init__(self, client, msmgr_base=None):
        if msmgr_base is None:
            msmgr_base = MSMgr.defaults["msmgr_base"]
        self.client = client
        self.baseurl = httpx.URL(msmgr_base)

    async def transfer_ids(self, workflowName=None):
        """ Request stuck transfer IDs

        Specify workflow name

        Returns a list of all stuck transfer requests
        """
        if workflowName is not None:
            params = {"request": workflowName}
        transfers = []
        result = await self.client.getjson(self.baseurl.join("info"), params=params)
        for row in result["result"]:
            for i, item in row.items():
                if i == "transferDoc":
                    input_data = {
                        "InputDataset": item["transfers"][-1]["dataset"],
                        "TransferIDs": item["transfers"][-1]["transferIDs"],
                        "LastUpdate": item["lastUpdate"],
                    }
                    transfers.append(input_data)
        df = pandas.json_normalize(transfers)
        format_dates(df, ["LastUpdate"])
        return df
