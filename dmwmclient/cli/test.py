import logging
import asyncio


logger = logging.getLogger(__name__)


class Test:
    @classmethod
    def register(cls, subparsers):
        parser = subparsers.add_parser("test", help="Test connections")
        parser.set_defaults(command=cls)
        return parser

    def __init__(self, client, args):
        self.client = client
        asyncio.get_event_loop().run_until_complete(self.go())

    async def go(self):
        res = await self.client.datasvc.jsonmethod("bounce", asdf="hi there")
        logging.info("DataSvc bounce test: %r" % res)
        logging.debug("Debug message")
