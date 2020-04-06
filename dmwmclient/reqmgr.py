import httpx
import pandas
from .util import format_dates


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

    async def transitions(self, inputdataset=None, outputdataset=None):
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
