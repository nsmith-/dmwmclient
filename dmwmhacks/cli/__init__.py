import argparse
from ..restclient import RESTClient
from ..datasvc import DataSvc
from ..unified import Unified
from .shell import Shell
from .test import Test
from .unified import UnifiedTransferStatus


def cli():
    parser = argparse.ArgumentParser(
        description='CLI to run various scripts',
        formatter_class=lambda prog: argparse.RawTextHelpFormatter(prog, width=160),
    )
    RESTClient.add_args(parser)
    DataSvc.add_args(parser)
    Unified.add_args(parser)

    subparsers = parser.add_subparsers(
        help='sub-command help'
    )
    Shell.register(subparsers)
    Test.register(subparsers)
    UnifiedTransferStatus.register(subparsers)

    args = parser.parse_args()
    client = RESTClient.from_cli(args)
    datasvc = DataSvc.from_cli(client, args)
    unified = Unified.from_cli(client, args)

    if hasattr(args, 'command'):
        args.command(
            client=client,
            datasvc=datasvc,
            unified=unified,
            args=args,
        )
    else:
        parser.parse_args(['-h'])
