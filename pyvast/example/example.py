#!/usr/bin/env python3

import asyncio
import json
import pyarrow

from pyvast import VAST

"""
Example script to showcase the VAST CLI wrapper.

Follow the instructions in the README.md to setup a local vast node and ingest
some demo data before running this example.
"""


async def continuous_query_example():
    """
        This example demonstrates how continuous queries of VAST can be wrapped
        with the Python bindings.

        To spawn continuous events in VAST, first run this code and then, on a
        different terminal, ingest new Zeek logs with VAST. That should update
        the console that is running this example code.
    """
    vast = VAST(binary="/opt/tenzir/bin/vast")
    proc = await vast.export(continuous=True).json('#type == "zeek.conn"').exec()
    print("Waiting for VAST to ingest data...")
    for _ in range(10):  # print 10 updates
        data = await proc.stdout.readline()
        print("Ingested new data:")
        print(data.decode("ascii").rstrip(), "\n")


async def example():
    print("normal query")
    vast = VAST(binary="/opt/tenzir/bin/vast")
    await vast.test_connection()
    proc = await vast.export(max_events=2).json(":addr == 192.168.1.104").exec()
    stdout, stderr = await proc.communicate()  # wait until all pipes reached EOF
    print(stdout)

    print("query with apache arrow export")
    proc = await vast.export(max_events=2).arrow(":addr == 192.168.1.104").exec()
    stdout, stderr = await proc.communicate()  # wait until all pipes reached EOF
    reader = pyarrow.ipc.open_stream(stdout)
    table = reader.read_all()
    print(table.to_pydict())


if __name__ == "__main__":
    asyncio.run(continuous_query_example())
    # asyncio.run(example())
