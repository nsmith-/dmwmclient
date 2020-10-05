import httpx
import pandas
from .util import format_dates
import datetime


class ReqMgr:
    """ReqMgr client

    Server API documentation at https://github.com/dmwm/WMCore/wiki/reqmgr2-apis
    """

    defaults = {
        "reqmgr_base": "https://cmsweb.cern.ch/reqmgr2/data/",
    }

    def __init__(self, client, reqmgr_base=None):
        if reqmgr_base is None:
            reqmgr_base = ReqMgr.defaults["reqmgr_base"]
        self.client = client
        self.baseurl = httpx.URL(reqmgr_base)

    async def transitions(
        self, inputdataset=None, outputdataset=None, mc_pileup=None, status=None
    ):
        """Request transitions

        Specify either input or output dataset.

        Returns a list of all request transitions that involve the specified dataset
        """
        params = {
            "mask": "RequestTransition",
        }
        if inputdataset is not None:
            params["inputdataset"] = inputdataset
        if outputdataset is not None:
            params["outputdataset"] = outputdataset
        if mc_pileup is not None:
            params["mc_pileup"] = mc_pileup
        if status is not None:
            params["status"] = status
        result = await self.client.getjson(self.baseurl.join("request"), params=params)

        flat = []
        for row in result["result"]:
            for requestname, item in row.items():
                for i, transition in enumerate(item["RequestTransition"]):
                    flatrow = {
                        "requestname": requestname,
                        "current": (i + 1) == len(item["RequestTransition"]),
                    }
                    flatrow.update(params)
                    flatrow.update(transition)
                    flat.append(flatrow)

        df = pandas.json_normalize(flat)
        format_dates(df, ["UpdateTime"])
        return df

    async def stuck_transfers(self, timedelta=14):
        """Request stuck input datasets

        Default time delta is 14.

        Returns a list of all stuck input datasets that are stuck more than timedelta days
        """
        params = {"status": "staging"}
        stuck_data = []
        result = await self.client.getjson(self.baseurl.join("request"), params=params)
        for row in result["result"]:
            for requestname, item in row.items():
                tstart = datetime.datetime.fromtimestamp(
                    item["RequestTransition"][-1]["UpdateTime"]
                )
                current_time = datetime.datetime.now()
                past_time = datetime.timedelta(days=timedelta)
                if tstart < current_time - past_time:
                    if item.get("InputDataset") is not None:
                        input_data = {
                            "InputDataset": item.get("InputDataset"),
                            "UpdateTime": item["RequestTransition"][-1]["UpdateTime"],
                        }
                        stuck_data.append(input_data)
        df = pandas.json_normalize(stuck_data)
        format_dates(df, ["UpdateTime"])
        return df

    async def active_request_datasets(self):
        params = {
            "status": "ACTIVE",
            "mask": [
                "InputDataset",
                "OutputDatasets",
                "IncludeParents",
                "MCPileup",
                "Team",
            ],
        }
        result = await self.client.getjson(self.baseurl.join("request"), params=params)
        result = result["result"][0]
        for name, request in result.items():
            request["requestname"] = name
            # normalize schema a bit
            if isinstance(request["InputDataset"], str):
                request["InputDataset"] = [request["InputDataset"]]
        return list(result.values())
