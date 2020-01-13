import httpx


class Unified:
    '''Unified REST API

    A random assortment of URLs
    '''
    defaults = {
        'unified_base': 'https://cms-unified.web.cern.ch/cms-unified/',
        'unified_timeout': 30,
    }

    @classmethod
    def add_args(cls, parser):
        group = parser.add_argument_group('Unified API config')
        group.add_argument(
            '--unified_base',
            default=cls.defaults['unified_base'],
            help='Unified base URL with trailing slash (default: %(default)s)',
        )
        group.add_argument(
            '--unified_timeout',
            default=cls.defaults['unified_timeout'],
            help='REST query timeout in seconds (default: %(default)s)',
            type=int,
        )
        return group

    @classmethod
    def from_cli(cls, client, args):
        return cls(
            client,
            unified_base=args.unified_base,
            unified_timeout=args.unified_timeout,
        )

    def __init__(self, client, **kwargs):
        kwargs.update(Unified.defaults)
        self.client = client
        self.baseurl = httpx.URL(kwargs['unified_base'])
        self.timeout = kwargs['unified_timeout']

    async def transfer_statuses(self):
        # TODO: respect timeout
        res = await self.client.getjson(
            url=self.baseurl.join('transfer_statuses.json'),
        )
        return res
