import httpx
import pandas


class DBS:
    """DBS3 client

    """

    defaults = {
        "dbs_base": "https://cmsweb.cern.ch/dbs/prod/global/DBSReader/",
    }

    @classmethod
    def add_args(cls, parser):
        group = parser.add_argument_group("DBS3 API config")
        group.add_argument(
            "--dbs_base",
            default=cls.defaults["dbs_base"],
            help="DBS REST endpoint URL with trailing slash (default: %(default)s)",
        )
        return group

    @classmethod
    def from_cli(cls, client, args):
        return cls(client, dbs_base=args.dbs_base)

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
