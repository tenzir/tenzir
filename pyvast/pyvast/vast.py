#!/usr/bin/env python3

"""python vast module

Disclaimer: `await` does not work in the python3.7 repl,
use either python3.8 or ipython.

Example:

    Create a connector to a VAST server:
    > from pyvast import VAST
    > vast = VAST(binary="/opt/tenzir/bin/vast")
    Test if the connector works:
    > await vast.test_connection()
    Extract some Data:
    > data = await vast.export(max_events=10).json(":addr == 192.168.1.104").exec()

"""

import asyncio
import logging


class VAST:
    """A VAST node handle"""

    def __init__(self, binary="vast", endpoint="localhost:42000"):
        self.logger = logging.getLogger("vast")
        self.binary = binary
        self.endpoint = endpoint
        self.call_stack = []
        self.logger.debug(f"VAST client configured to use endpoint {self.endpoint}")

    async def __spawn(self, *args, stdin=None):
        """Spawns a process asynchronously."""
        return await asyncio.create_subprocess_exec(
            self.binary,
            "-e",
            self.endpoint,
            *args,
            stdin=stdin,
            stdout=asyncio.subprocess.PIPE,
        )

    async def test_connection(self):
        """Checks if the configured endpoint is reachable"""
        proc = await self.__spawn("status")
        stdout, stderr = await proc.communicate()
        return proc.returncode == 0

    async def exec(self, stdin=None):
        self.logger.debug(f"Executing call stack: {self.call_stack}")
        if not stdin:
            proc = await self.__spawn(*self.call_stack)
        else:
            self.logger.debug(f"Forwarding stdin {stdin}")
            proc = await self.__spawn(*self.call_stack, stdin=asyncio.subprocess.PIPE)
            proc.stdin.write(str(stdin).encode())
            await proc.stdin.drain()
            proc.stdin.close()
        self.call_stack = []
        return await proc.communicate()

    def __getattr__(self, name, **kwargs):
        """Chains every unknown method call to the internal call stack."""
        if name.endswith("_"):
            # trim trailing underscores to overcome the 'import' keyword
            name = name[:-1]
        self.call_stack.append(name)

        def method(*args, **kwargs):
            if kwargs:
                for k, v in kwargs.items():
                    if v is True:
                        self.call_stack.append(f"--{k.replace('_','-')}")
                    else:
                        self.call_stack.append(f"--{k.replace('_','-')}={v}")
            if args:
                self.call_stack.extend(args)
            return self

        return method
