import httpx
import pandas


class DBS:
    """DBS3 client

    API reference: https://cms-http-group.web.cern.ch/cms-http-group/apidoc/dbs3/current/dbs.web.html#module-dbs.web.DBSReaderModel
    """

    defaults = {
        # DBS REST endpoint URL with trailing slash
        "dbs_base": "https://cmsweb.cern.ch/dbs/prod/global/DBSReader/",
    }

    def __init__(self, client, dbs_base=None):
        if dbs_base is None:
            dbs_base = DBS.defaults["dbs_base"]
        self.client = client
        self.baseurl = httpx.URL(dbs_base)

    async def jsonmethod(self, method, **params):
        return await self.client.getjson(url=self.baseurl.join(method), params=params)

    async def pandasmethod(self, method, **params):
        req = self.client.build_request(
            method="GET", url=self.baseurl.join(method), params=params,
        )
        res = await self.client.send(req, timeout=30)
        return pandas.read_json(res.content)
