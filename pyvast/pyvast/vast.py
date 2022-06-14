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
from distutils.spawn import find_executable


class VAST:
    """A VAST node handle"""

    def __init__(
        self,
        binary="vast",
        endpoint=None,
        container={"runtime": "docker", "name": "vast"},
        logger=None,
    ):
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger("vast")
            self.logger.setLevel(logging.DEBUG)
            ch = logging.StreamHandler()
            ch.setFormatter(
                logging.Formatter(
                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                )
            )
            self.logger.addHandler(ch)
        self.container = container
        self.binary = binary
        self.endpoint = endpoint
        self.call_stack = []
        self.logger.debug(
            f"PyVAST: VAST client configured to use endpoint {self.endpoint}"
        )

    async def __spawn(self, *args, stdin=None):
        """Spawns a process asynchronously."""
        if self.endpoint is not None:
            args = ("-e", self.endpoint) + args

        if find_executable(self.binary) is not None:
            command = self.binary
        else:
            args = ("exec", self.container.get("name"), "vast") + args
            command = self.container.get("runtime")

        return await asyncio.create_subprocess_exec(
            command,
            *args,
            stdin=stdin,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

    async def test_connection(self):
        """Checks if the configured endpoint is reachable"""
        proc = await self.__spawn("status")
        await proc.wait()
        return proc.returncode == 0

    async def exec(self, stdin=None):
        self.logger.debug(f"PyVAST: Executing call stack: {self.call_stack}")
        if not stdin:
            proc = await self.__spawn(*self.call_stack)
        else:
            self.logger.debug(f"PyVAST: Forwarding stdin {stdin}")
            proc = await self.__spawn(*self.call_stack, stdin=asyncio.subprocess.PIPE)
            proc.stdin.write(str(stdin).encode())
            await proc.stdin.drain()
            proc.stdin.close()
        self.call_stack = []
        return proc

    def __getattr__(self, name, **kwargs):
        """Chains every unknown method call to the internal call stack."""
        if name.endswith("_"):
            # trim trailing underscores to overcome the 'import' keyword
            name = name[:-1]

        if not name == "__iter_":
            self.call_stack.append(name.replace("_", "-"))

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
