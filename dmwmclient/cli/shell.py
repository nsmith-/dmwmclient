from IPython import embed


class Shell:
    @classmethod
    def register(cls, subparsers):
        parser = subparsers.add_parser("shell", help="IPython interactive shell")
        parser.set_defaults(command=cls)
        return parser

    def __init__(self, client, args):
        embed(
            header="Local variables: client (%r)" % client,
            using="asyncio",
            user_ns={"client": client},
        )
