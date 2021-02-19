import aiounittest
import unittest

import asyncio
from pyvast import VAST
import json


class TestConnection(aiounittest.AsyncTestCase):
    def setUp(self):
        self.vast = VAST(binary="/opt/tenzir/bin/vast")  # default port
        self.vast_disconnected = VAST(
            binary="/opt/tenzir/bin/vast", endpoint="localhost:44883"
        )  # closed / unbound port

    async def test_connection(self):
        self.assertTrue(await self.vast.test_connection())

    async def test_custom_endpoint_parameterization(self):
        self.assertFalse(await self.vast_disconnected.test_connection())

    async def test_connection(self):
        self.assertTrue(await self.vast.test_connection())


class TestCallStackCreation(unittest.TestCase):
    def setUp(self):
        self.vast = VAST(binary="/opt/tenzir/bin/vast")

    def test_call_chaining(self):
        self.assertEqual(self.vast.call_stack, [])
        query = ":timestamp < 1 hour ago"
        self.vast.export().arrow(query)
        self.assertEqual(self.vast.call_stack, ["export", "arrow", query])

    def test_boolean_flag_handling(self):
        self.assertEqual(self.vast.call_stack, [])
        query = ":addr == 192.168.1.104 && :timestamp < 1 hour ago"
        self.vast.export(continuous=True).json(query)
        self.assertEqual(
            self.vast.call_stack, ["export", "--continuous", "json", query]
        )

    def test_import_keyword(self):
        self.assertEqual(self.vast.call_stack, [])
        path = "/some/file"
        self.vast.import_().pcap(read=path)
        self.assertEqual(self.vast.call_stack, ["import", "pcap", f"--read={path}"])

    def test_underscore_replacement_in_parameters(self):
        self.assertEqual(self.vast.call_stack, [])
        self.vast.export(max_events=10).json("192.168.1.104")
        self.assertEqual(
            self.vast.call_stack, ["export", "--max-events=10", "json", "192.168.1.104"]
        )

    def test_underscore_replacement_in_subcommands(self):
        self.assertEqual(self.vast.call_stack, [])
        self.vast.matcher().ioc_remove(name="foo", ioc="bar")
        self.assertEqual(
            self.vast.call_stack, ["matcher", "ioc-remove", "--name=foo", "--ioc=bar"]
        )
