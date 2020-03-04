import httpx


class Unified:
    """Unified REST API

    A random assortment of URLs
    """

    defaults = {
        # Unified base URL with trailing slash
        "unified_base": "https://cms-unified.web.cern.ch/cms-unified/",
    }

    def __init__(self, client, unified_base=None):
        if unified_base is None:
            unified_base = Unified.defaults["unified_base"]
        self.client = client
        self.baseurl = httpx.URL(unified_base)

    async def transfer_statuses(self):
        res = await self.client.getjson(
            url=self.baseurl.join("transfer_statuses.json"),
        )
        return res
