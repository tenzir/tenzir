import asyncio
import copy
import os
import subprocess
from pathlib import Path

import tenzir_common.logging as core_logging

logger = core_logging.get(__name__)


from typing import Optional


def _packaged_tenzir_ctl() -> Optional[str]:
    # Resolve packaged binary path if present, else None
    p = Path(__file__).resolve().parents[1] / "bin" / "tenzir-ctl"
    if p.is_file() and os.access(p, os.X_OK):
        return p.as_posix()
    return None


class CLI:
    """A command-line wrapper for the tenzir-ctl executable."""

    @staticmethod
    def arguments(**kwargs) -> list[str]:
        result = []
        for k, v in kwargs.items():
            if v is True:
                result.append(f"--{k.replace('_','-')}")
            else:
                result.append(f"--{k.replace('_','-')}={v}")
        return result

    def __init__(self, **kwargs):
        self.args = CLI.arguments(**kwargs)

    async def exec(self, stdin=False) -> asyncio.subprocess.Process:
        async def run(*args, stdin) -> asyncio.subprocess.Process:
            return await asyncio.create_subprocess_exec(
                _packaged_tenzir_ctl() or "tenzir-ctl",
                *args,
                stdin=asyncio.subprocess.PIPE if stdin else None,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

        logger.debug(f"> {['tenzir-ctl', *self.args]}")
        if not stdin:
            return await run(*self.args, stdin=False)
        proc = await run(*self.args, stdin=True)
        # TODO: for large inputs, the buffering may be excessive here. We may
        # have to switch to a streaming approach.
        proc.stdin.write(str(stdin).encode())
        await proc.stdin.drain()
        proc.stdin.close()
        return proc

    def sync_exec(self, stdin=False) -> subprocess.Popen:
        def run(*args, stdin) -> subprocess.Popen:
            return subprocess.Popen(
                [_packaged_tenzir_ctl() or "tenzir-ctl", *args],
                stdin=subprocess.PIPE if stdin else None,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

        logger.debug(f"> {['tenzir-ctl', *self.args]}")
        if not stdin:
            return run(*self.args, stdin=False)
        proc = run(*self.args, stdin=True)
        # TODO: for large inputs, the buffering may be excessive here. We may
        # have to switch to a streaming approach.
        proc.stdin.write(str(stdin).encode())
        proc.stdin.flush()
        proc.stdin.close()
        return proc

    def __getattr__(self, name, **kwargs):
        """Chains every unknown method call to the internal call stack."""
        # Don't break `__special__` attributes.
        if name.startswith("__"):
            raise AttributeError(name)
        new = copy.deepcopy(self)
        name = name.removesuffix("_")
        if not name == "__iter_":
            new.args.append(name.replace("_", "-"))

        def command(*args, **kwargs):
            if kwargs:
                new.args.extend(CLI.arguments(**kwargs))
            if args:
                new.args.extend(args)
            return new

        return command
