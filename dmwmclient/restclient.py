import os
import logging
import json
import httpx
import asyncio
from lxml import etree
from . import __version__


logger = logging.getLogger(__name__)


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

    def __init__(self, usercert=None, userkey=None, certdir=None):
        if usercert is None:
            usercert = RESTClient.defaults['usercert']
        if userkey is None:
            userkey = RESTClient.defaults['userkey']
        if certdir is None:
            certdir = RESTClient.defaults['certdir']
        usercert = os.path.expanduser(usercert)
        userkey = os.path.expanduser(userkey)
        certdir = os.path.expanduser(certdir)
        self._ssoevents = {}
        self._client = httpx.AsyncClient(
            backend='asyncio',
            cert=(usercert, userkey),
            verify=certdir,
            timeout=httpx.Timeout(
                10.,
                read_timeout=30.,
            ),
            headers=httpx.Headers({'User-Agent': f'python-dmwmclient/{__version__}'}),
        )

    async def cern_sso_check(self, host):
        '''Check if this host already has an SSO action in progress, and wait for it'''
        try:
            await self._ssoevents[host].wait()
            return True
        except KeyError:
            pass
        return False

    async def cern_sso_follow(self, result, host):
        '''Follow CERN SSO redirect, returning the result of the original request'''
        html = etree.HTML(result.content)
        link = [l for l in html.xpath('//a') if l.text == 'Sign in using your CERN Certificate']
        if len(link) == 1:
            logger.debug('Running first-time CERN SSO sign-in routine')
            self._ssoevents[host] = asyncio.Event()
            url = result.url.join(link[0].attrib['href'])
            result = await self._client.get(url)
            html = etree.HTML(result.content)
            url = result.url.join(html.xpath('body/form')[0].attrib['action'])
            data = {el.attrib['name']: el.attrib['value'] for el in html.xpath('body/form/input')}
            result = await self._client.post(url, data=data)
            logger.debug('Received SSO cookie for %s: %r' % (host, dict(result.history[0].cookies)))
            self._ssoevents[host].set()
            del self._ssoevents[host]
            return result
        form = html.xpath('body/form')
        if len(form) == 1:
            logger.debug('Following CERN SSO redirect')
            url = result.url.join(form[0].attrib['action'])
            data = {el.attrib['name']: el.attrib['value'] for el in html.xpath('body/form/input')}
            result = await self._client.post(url, data=data)
            logger.debug('Received SSO cookie for %s: %r' % (host, dict(result.history[0].cookies)))
            return result
        logger.debug('Invalid SSO login page content:\n' + result.content.decode('ascii'))
        raise RuntimeError('Could not parse CERN SSO login page (no sign-in link or auto-redirect found)')

    def build_request(self, **params):
        return self._client.build_request(**params)

    async def send(self, request, timeout=None, retries=1):
        await self.cern_sso_check(request.url.host)
        # Looking forward to https://github.com/encode/httpx/pull/784
        while retries > 0:
            try:
                result = await self._client.send(request, timeout=timeout)
                if result.status_code == 200 and result.url.host == 'login.cern.ch':
                    if await self.cern_sso_check(request.url.host):
                        self._client.cookies.set_cookie_header(request)
                        continue
                    result = await self.cern_sso_follow(result, request.url.host)
                if result.status_code != 200:
                    logging.warning('HTTP status code %d received while executing request %r' % (result.status_code, request))
                return result
            except httpx.TimeoutException:
                logging.warning('Timeout encountered while executing request %r' % request)
            retries -= 1
        raise IOError('Exhausted %d retries while executing request %r' % (retries, request))

    async def getjson(self, url, params=None, timeout=None, retries=1):
        request = self.build_request(method='GET', url=url, params=params)
        result = await self.send(request, timeout=timeout, retries=retries)
        try:
            return result.json()
        except json.JSONDecodeError:
            raise IOError('Failed to decode json for request %r.\nContent start:', (request, result.content[:160]))
