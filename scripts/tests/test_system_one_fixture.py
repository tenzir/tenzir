"""Unit tests for request accounting, without HTTP servers or a Tenzir process.

Run with:
    python -m unittest discover -s scripts/tests -p test_system_one_fixture.py
"""

import importlib.util
import io
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch


def load_fixture():
    api = types.ModuleType("tenzir_test")
    api.FixtureHandle = object
    api.fixture = lambda **_kwargs: lambda function: function
    spec = importlib.util.spec_from_file_location(
        "system_one_fixture_unit_test",
        Path(__file__).resolve().parents[2] / "test/fixtures/system_one.py",
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {"tenzir_test": api, spec.name: module}):
        spec.loader.exec_module(module)
    return module


class RequestAccountingTest(unittest.TestCase):
    def setUp(self):
        self.fixture = load_fixture()
        # One other request remains in flight throughout each test.
        self.state = {"active": 1, "arrivals": 1, "max_active": 1}
        self.handler_type = self.fixture._make_handler([], [], self.state)

    def handler(self):
        handler = object.__new__(self.handler_type)
        handler.send_response = lambda _status: None
        handler.send_header = lambda _name, _value: None
        handler.end_headers = lambda: None
        handler.wfile = io.BytesIO()
        return handler

    def test_response_completes_before_handler_cleanup(self):
        first = self.handler()
        successor = self.handler()

        def reply(_arrival, _overlapping):
            successor._reply(200, {})

        successor._handle = reply

        def reply_and_submit_successor(_arrival, _overlapping):
            first._reply(200, {})
            # Model a client submitting its next request after receiving the
            # response, while this handler has not yet reached its finally.
            successor.do_POST()

        first._handle = reply_and_submit_successor
        first.do_POST()
        self.assertEqual(self.state["max_active"], 2)
        self.assertEqual(self.state["active"], 1)
        self.assertEqual(self.state["arrivals"], 3)

    def test_request_stays_active_during_response_write(self):
        handler = self.handler()

        def write(_body):
            self.assertEqual(self.state["active"], 2)

        handler.wfile.write = write
        handler._handle = lambda _arrival, _overlapping: handler._reply(200, {})
        handler.do_POST()
        self.assertEqual(self.state["active"], 1)

    def test_exception_cleanup_counts_each_request_once(self):
        for phase in ("before_reply", "during_write", "after_reply"):
            with self.subTest(phase=phase):
                handler = self.handler()

                def fail(*_args):
                    raise OSError("injected failure")

                def handle(_arrival, _overlapping):
                    if phase == "before_reply":
                        fail()
                    handler._reply(200, {})
                    fail()

                handler._handle = handle
                if phase == "during_write":
                    handler.wfile.write = fail
                with self.assertRaisesRegex(OSError, "injected failure"):
                    handler.do_POST()
                self.assertEqual(self.state["active"], 1)


if __name__ == "__main__":
    unittest.main()
