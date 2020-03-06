import httpx


class McM:
    """McM client

    """

    defaults = {
        # McM REST endpoint URL with trailing slash
        "mcm_base": "https://cms-pdmv.cern.ch/mcm/",
    }

    def __init__(self, client, mcm_base=None):
        if mcm_base is None:
            mcm_base = McM.defaults["mcm_base"]
        self.client = client
        self.baseurl = httpx.URL(mcm_base)

    async def search(self, **params):
        return await self.client.getjson(url=self.baseurl.join("search"), params=params)
