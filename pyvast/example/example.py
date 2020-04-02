#!/usr/bin/env python3

import asyncio
import json
import pyarrow

from pyvast import VAST

"""
Example script to showcase the VAST CLI wrapper.

Prerequisites: VAST must be running.
    0. Install VAST
    1. Go to another terminal
    2. `vast start`
"""

vast = VAST(binary="/opt/tenzir/bin/vast")
asyncio.run(vast.test_connection())

stdout, stderr = asyncio.run(
    vast.export(max_events=2).json(":addr == 192.168.1.104").exec()
)
print(stdout)

stdout, stderr = asyncio.run(
    vast.export(max_events=2).arrow(":addr == 192.168.1.104").exec()
)
reader = pyarrow.ipc.open_stream(stdout)
table = reader.read_all()
print(table.to_pydict())
