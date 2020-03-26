import argparse
import logging
from .. import Client
from ..restclient import locate_proxycert
from .shell import Shell
from .test import Test
from .unified import UnifiedTransferStatus


def cli():
    parser = argparse.ArgumentParser(
        description="CLI to run various scripts",
        formatter_class=lambda prog: argparse.RawTextHelpFormatter(prog, width=88),
    )
    parser.add_argument(
        "-v", "--verbose", action="count", default=0, help="Verbosity",
    )
    parser.add_argument(
        "--proxy",
        action="store_true",
        help="Prefer using a user proxy over user certificate",
    )
    subparsers = parser.add_subparsers(help="sub-command help")
    Shell.register(subparsers)
    Test.register(subparsers)
    UnifiedTransferStatus.register(subparsers)

    args = parser.parse_args()

    loglevel = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}
    logging.basicConfig(level=loglevel[min(2, args.verbose)])

    client = Client(usercert=locate_proxycert() if args.proxy else None)

    if hasattr(args, "command"):
        args.command(client=client)
    else:
        parser.parse_args(["-h"])
