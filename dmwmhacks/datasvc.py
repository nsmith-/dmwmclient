import httpx
import pandas


BLOCKARRIVE_BASISCODE = {
    -6: 'no_source',
    -5: 'no_link',
    -4: 'auto_suspend',
    -3: 'no_download_link',
    -2: 'manual_suspend',
    -1: 'block_open',
    0: 'routed',
    1: 'queue_full',
    2: 'rerouting',
}


class DataSvc:
    '''PhEDEx datasvc REST API

    Full documentation at https://cmsweb.cern.ch/phedex/datasvc/doc
    '''
    defaults = {
        'datasvc_base': 'https://cmsweb.cern.ch/phedex/datasvc/',
        'phedex_instance': 'prod',
        'datasvc_timeout': 30,
    }

    @classmethod
    def add_args(cls, parser):
        group = parser.add_argument_group('PhEDEx datasvc config')
        group.add_argument(
            '--datasvc_base',
            default=cls.defaults['datasvc_base'],
            help='PhEDEx datasvc base URL with trailing slash (default: %(default)s)',
        )
        group.add_argument(
            '--phedex_instance',
            default=cls.defaults['phedex_instance'],
            help='PhEDEx instance (default: %(default)s)',
            choices=['prod', 'dev', 'debug'],
        )
        group.add_argument(
            '--datasvc_timeout',
            default=cls.defaults['datasvc_timeout'],
            help='REST query timeout in seconds (default: %(default)s)',
            type=int,
        )
        return group

    @classmethod
    def from_cli(cls, client, args):
        return cls(
            client,
            datasvc_base=args.datasvc_base,
            phedex_instance=args.phedex_instance,
            datasvc_timeout=args.datasvc_timeout,
        )

    def __init__(self, client, **kwargs):
        args = dict(DataSvc.defaults)
        args.update(kwargs)
        self.client = client
        self.baseurl = httpx.URL(args['datasvc_base'])
        self.jsonurl = self.baseurl.join('json/%s/' % args['phedex_instance'])
        self.xmlurl = self.baseurl.join('xml/%s/' % args['phedex_instance'])
        self.timeout = args['datasvc_timeout']

    async def jsonmethod(self, method, **params):
        # TODO: respect timeout
        return await self.client.getjson(
            url=self.jsonurl.join(method),
            params=params,
        )

    def _format_dates(self, df, datecols):
        if df.size > 0:
            df[datecols] = df[datecols].apply(lambda v: pandas.to_datetime(v, unit='s'))
        return df

    async def blockreplicas(self, **params):
        '''Get block replicas as a pandas dataframe

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
        '''
        resjson = await self.jsonmethod('blockreplicas', **params)
        df = pandas.io.json.json_normalize(
            resjson['phedex']['block'],
            record_path='replica',
            record_prefix='replica.',
            meta=['bytes', 'files', 'name', 'id', 'is_open'],
        )
        self._format_dates(df, ['replica.time_create', 'replica.time_update'])
        return df
