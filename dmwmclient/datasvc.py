import httpx
import pandas
from .util import format_dates


BLOCKARRIVE_BASISCODE = {
    -6: "no_source",
    -5: "no_link",
    -4: "auto_suspend",
    -3: "no_download_link",
    -2: "manual_suspend",
    -1: "block_open",
    0: "routed",
    1: "queue_full",
    2: "rerouting",
}


class DataSvc:
    """PhEDEx datasvc REST API

    Full documentation at https://cmsweb.cern.ch/phedex/datasvc/doc
    """

    defaults = {
        # PhEDEx datasvc base URL with trailing slash
        "datasvc_base": "https://cmsweb.cern.ch/phedex/datasvc/",
        # Options: prod, dev, debug
        "phedex_instance": "prod",
    }

    def __init__(self, client, datasvc_base=None, phedex_instance=None):
        if datasvc_base is None:
            datasvc_base = DataSvc.defaults["datasvc_base"]
        if phedex_instance is None:
            phedex_instance = DataSvc.defaults["phedex_instance"]
        self.client = client
        self.baseurl = httpx.URL(datasvc_base)
        self.jsonurl = self.baseurl.join("json/%s/" % phedex_instance)
        self.xmlurl = self.baseurl.join("xml/%s/" % phedex_instance)

    async def jsonmethod(self, method, **params):
        return await self.client.getjson(url=self.jsonurl.join(method), params=params)

    async def blockreplicas(self, **params):
        """Get block replicas as a pandas dataframe

        Parameters
        ----------
        block          block name, can be multiple (*)
        dataset        dataset name, can be multiple (*)
        node           node name, can be multiple (*)
        se             storage element name, can be multiple (*)
        update_since   unix timestamp, only return replicas whose record was
                        updated since this time
        create_since   unix timestamp, only return replicas whose record was
                        created since this time. When no "dataset", "block"
                        or "node" are given, create_since is default to 24 hours ago
        complete       y or n, whether or not to require complete or incomplete
                        blocks. Open blocks cannot be complete.  Default is to
                        return either.
        dist_complete  y or n, "distributed complete".  If y, then returns
                        only block replicas for which at least one node has
                        all files in the block.  If n, then returns block
                        replicas for which no node has all the files in the
                        block.  Open blocks cannot be dist_complete.  Default is
                        to return either kind of block replica.
        subscribed     y or n, filter for subscription. default is to return either.
        custodial      y or n. filter for custodial responsibility.  default is
                        to return either.
        group          group name.  default is to return replicas for any group.
        show_dataset   y or n, default n. If y, show dataset information with
                        the blocks; if n, only show blocks
        """
        resjson = await self.jsonmethod("blockreplicas", **params)
        df = pandas.io.json.json_normalize(
            resjson["phedex"]["block"],
            record_path="replica",
            record_prefix="replica.",
            meta=["bytes", "files", "name", "id", "is_open"],
        )
        format_dates(df, ["replica.time_create", "replica.time_update"])
        return df
