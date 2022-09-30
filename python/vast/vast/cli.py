import asyncio

import vast.utils.logging

logger = vast.utils.logging.get(__name__)


class CLI:
    """A command-line wrapper for the VAST executable."""

    @staticmethod
    def arguments(**kwargs):
        result = []
        for k, v in kwargs.items():
            if v is True:
                result.append(f"--{k.replace('_','-')}")
            else:
                result.append(f"--{k.replace('_','-')}={v}")
        return result

    def __init__(self, **kwargs):
        self.args = CLI.arguments(**kwargs)

    async def exec(self, stdin=False):
        async def run(*args, stdin):
            return await asyncio.create_subprocess_exec(
                "vast",
                *args,
                stdin=asyncio.subprocess.PIPE if stdin else None,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

        logger.debug(f"> vast {' '.join(self.args)}")
        if not stdin:
            return await run(*self.args, stdin=False)
        proc = await run(*self.args, stdin=True)
        # TODO: for large inputs, the buffering may be excessive here. We may
        # have to switch to a streaming approach.
        proc.stdin.write(str(stdin).encode())
        await proc.stdin.drain()
        proc.stdin.close()
        return proc

    def __getattr__(self, name, **kwargs):
        """Chains every unknown method call to the internal call stack."""
        if name.endswith("_"):
            # trim trailing underscores to overcome the 'import' keyword
            name = name[:-1]
        if not name == "__iter_":
            self.args.append(name.replace("_", "-"))

        def command(*args, **kwargs):
            if kwargs:
                self.args.extend(CLI.arguments(**kwargs))
            if args:
                self.args.extend(args)
            return self

        return command
