import argparse
import logging
from dmwmclient import Client
from dmwmclient.restclient import locate_proxycert
from dmwmclient.cli.shell import Shell
from dmwmclient.cli.test import Test
from dmwmclient.cli.ruciosummary import RucioSummary


def cli():
    parser = argparse.ArgumentParser(
        description="CLI to run various scripts",
        formatter_class=lambda prog: argparse.RawTextHelpFormatter(prog, width=88),
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Verbosity",
    )
    parser.add_argument(
        "--proxy",
        action="store_true",
        help="Prefer using a user proxy over user certificate",
    )
    subparsers = parser.add_subparsers(help="sub-command help")
    Shell.register(subparsers)
    Test.register(subparsers)
    RucioSummary.register(subparsers)

    args = parser.parse_args()

    loglevel = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}
    logging.basicConfig(level=loglevel[min(2, args.verbose)])

    client = Client(usercert=locate_proxycert() if args.proxy else None)

    if hasattr(args, "command"):
        args.command(client=client, args=args)
    else:
        parser.parse_args(["-h"])
