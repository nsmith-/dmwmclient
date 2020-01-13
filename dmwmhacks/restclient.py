import os
import json
import httpx


class RESTClient:
    defaults = {
        'usercert': '~/.globus/usercert.pem',
        'userkey': '~/.globus/userkey.pem',
        'certdir': os.getenv('X509_CERT_DIR', '/etc/grid-security/certificates'),
    }

    @classmethod
    def add_args(cls, parser):
        group = parser.add_argument_group('REST Client config')
        group.add_argument(
            '--usercert',
            default=cls.defaults['usercert'],
            help='Location of user x509 certificate (default: %(default)s)',
        )
        group.add_argument(
            '--userkey',
            default=cls.defaults['userkey'],
            help='Location of user x509 key (default: %(default)s)',
        )
        group.add_argument(
            '--certdir',
            default=cls.defaults['certdir'],
            help='Location of trusted x509 certificates (default: $X509_CERT_DIR if set, else /etc/grid-security/certificates)',
        )
        return group

    @classmethod
    def from_cli(cls, args):
        return cls(
            usercert=args.usercert,
            userkey=args.userkey,
            certdir=args.certdir,
        )

    def __init__(self, **kwargs):
        kwargs.update(RESTClient.defaults)
        usercert = os.path.expanduser(kwargs['usercert'])
        userkey = os.path.expanduser(kwargs['userkey'])
        certdir = os.path.expanduser(kwargs['certdir'])
        self.client = httpx.AsyncClient(
            backend='asyncio',
            cert=(usercert, userkey),
            verify=certdir,
            timeout=httpx.Timeout(
                10.,
                read_timeout=30.,
            ),
        )

    async def send(self, request):
        return await self.client.send(request)

    async def getjson(self, url, params=None):
        req = httpx.Request(
            method='GET',
            url=url,
            params=params,
        )
        res = await self.send(req)
        if res.status_code != 200:
            raise IOError("Error while executing request %r" % req)
        try:
            resjson = res.json()
        except json.JSONDecodeError:
            raise IOError("Failed to decode json for request %r. Content start:", (req, res.content[:160]))
        return resjson
