from IPython import embed


class Shell:
    @classmethod
    def register(cls, subparsers):
        parser = subparsers.add_parser("shell", help="IPython interactive shell")
        parser.set_defaults(command=cls)
        return parser

    def __init__(self, **kwargs):
        embed(
            header="Local variables:\n"
            + ",\n".join("% 15s: %r" % (k, v) for k, v in kwargs.items()),
            using="asyncio",
            user_ns=kwargs,
        )
