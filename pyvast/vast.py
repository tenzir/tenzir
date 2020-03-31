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
import pyarrow


class VAST:
    """A VAST node handle"""

    def __init__(self, binary="vast", endpoint="localhost:42000"):
        self.logger = logging.getLogger("vast")
        self.binary = binary
        self.endpoint = endpoint
        self.call_stack = []
        self.logger.debug(f"VAST client configured to use endpoint {self.endpoint}")

    async def __spawn(self, *args):
        """Spawns a process asynchronously."""
        return await asyncio.create_subprocess_exec(
            self.binary, "-e", self.endpoint, *args, stdout=asyncio.subprocess.PIPE
        )

    async def test_connection(self):
        """Checks if the configured endpoint is reachable"""
        proc = await self.__spawn("status")
        stdout, stderr = await proc.communicate()
        return proc.returncode == 0

    async def exec(self):
        print(f"Call stack: {self.call_stack}")
        result = await self.__spawn(*self.call_stack)
        self.call_stack = []
        return result

    def __getattr__(self, name, **kwargs):
        """Chains every unknown method call to the internal call stack."""

        def method(*args, **kwargs):
            self.call_stack.append(name)
            if kwargs:
                for k, v in kwargs.items():
                    self.call_stack.append(f"--{k.replace('_','-')}={v}")
            if args:
                self.call_stack.extend(args)
            return self

        return method
