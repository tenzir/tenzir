"""python vast module

Example:
    Disclaimer: `await` does not work in the python3.7 repl,
    use either python3.8 or ipython.

    Create a connector to a VAST server:
    > from pyvast import VAST
    > vast = VAST(app="/opt/tenzir/bin/vast")
    Test if the connector works:
    > await vast.connect()
    Extract some Data:
    > data = await vast.query(":addr == 192.168.1.104")

"""

import asyncio
import logging
import pyarrow


async def spawn(*args):
    """Spawns a process asynchronously."""
    proc = await asyncio.create_subprocess_exec(*args, stdout=asyncio.subprocess.PIPE)
    return proc


class VAST:
    """A VAST node handle"""

    def __init__(self, app="vast", endpoint="localhost:42000"):
        self.logger = logging.getLogger("vast")
        self.app = app
        self.endpoint = endpoint
        self.logger.debug("connecting to vast on %s", self.endpoint)

    async def query(self, expression):
        """Extracts data from VAST according to a query"""
        self.logger.debug("running query %s", expression)
        proc = await spawn(self.app, "export", "arrow", expression)
        # This cannot be avoided, but the reader does not create a second copy,
        # so we can only do better with memory mapping, i.e. Plasma.
        output = (await proc.communicate())[0]
        reader = pyarrow.ipc.open_stream(output)
        table = reader.read_all()
        return table

    async def connect(self):
        """Checks if the endpoint can be connected to"""
        proc = await spawn(self.app, "status")
        await proc.communicate()
        result = False
        if proc.returncode == 0:
            result = True
        return result
