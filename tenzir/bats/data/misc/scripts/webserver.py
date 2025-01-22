#!/usr/bin/env python3
#
# This script is inspired by https://github.com/nickjj/webserver/ in that it is
# a minimalistic web server. However, we need to perform numerous adaptations to
# fit into the Tenzir integration test suite, e.g., make the output more
# deterministic.

from http.server import HTTPServer, BaseHTTPRequestHandler
from email.message import Message
import argparse

REFLECT = False


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        print(format % args)

    def do_GET(self):
        self.write_response(b"")

    def do_POST(self):
        content_length = int(self.headers.get("content-length", 0))
        body = self.rfile.read(content_length)
        self.write_response(body)

    def do_PUT(self):
        content_length = int(self.headers.get("content-length", 0))
        body = self.rfile.read(content_length)
        self.write_response(body)

    def write_response(self, content):
        self.send_response(200)
        self.end_headers()
        # Ensure determinism.
        headers = Message()
        for key, value in self.headers.items():
            if key == "User-Agent":
                if value.startswith("Tenzir/"):
                    value = "Tenzir/*.*.*"
            elif key == "Accept-Encoding":
                value = "*"
            headers[key] = value
        result = f"\n{headers}{content.decode('utf-8')}"
        if REFLECT:
            self.wfile.write(result.encode("utf-8"))
        else:
            print(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--reflect', action='store_true')
    parser.add_argument('--host', type=str, default='localhost')
    parser.add_argument('--port', type=int, default=8888)
    args = parser.parse_args()
    REFLECT = args.reflect

    with HTTPServer((args.host, args.port), Handler) as server:
        # Handle a single request then exit
        server.handle_request()
