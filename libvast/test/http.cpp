/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/concept/parseable/vast/http.hpp"
#include "vast/concept/parseable/vast/uri.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/http.hpp"
#include "vast/concept/printable/vast/uri.hpp"

#define SUITE http
#include "vast/test/test.hpp"

using namespace vast;
using namespace std::string_literals;

TEST(HTTP response) {
  http::response r;
  r.status_code = 200;
  r.status_text = "OK";
  r.protocol = "HTTP";
  r.version = 1.1;
  r.headers.push_back({"Content-Type", "text/plain"});
  r.headers.push_back({"Connection", "keep-alive"});
  r.body = "foo";
  auto ok = "HTTP/1.1 200 OK\r\n"s;
  ok += "Content-Type: text/plain\r\nConnection: keep-alive\r\n\r\nfoo";
  CHECK_EQUAL(to_string(r), ok);
}

TEST(URI) {
  uri u;
  u.scheme = "http";
  u.host = "foo.bar";
  u.port = 80;
  u.path.emplace_back("foo");
  u.path.emplace_back("bar");
  u.path.emplace_back("baz");
  u.query["opt1"] = "val 1";
  u.query["opt2"] = "val2";
  u.fragment = "frag 1";
  auto ok = "http://foo.bar:80/foo/bar/baz?opt1=val%201&opt2=val2#frag%201"s;
  CHECK_EQUAL(to_string(u), ok);
}

TEST(HTTP header) {
  auto p = make_parser<http::header>();
  auto str = "foo: bar"s;
  auto f = str.begin();
  auto l = str.end();
  http::header hdr;
  CHECK(p(f, l, hdr));
  CHECK(hdr.name == "FOO");
  CHECK(hdr.value == "bar");
  CHECK(f == l);

  str = "Content-Type:application/pdf";
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, hdr));
  CHECK(hdr.name == "CONTENT-TYPE");
  CHECK(hdr.value == "application/pdf");
  CHECK(f == l);
}

TEST(HTTP request) {
  auto p = make_parser<http::request>();
  auto str = "GET /foo/bar%20baz/ HTTP/1.1\r\n"
             "Content-Type:text/html\r\n"
             "Content-Length:1234\r\n"
             "\r\n"
             "Body "s;
  auto f = str.begin();
  auto l = str.end();
  http::request req;
  CHECK(p(f, l, req));
  CHECK(req.method == "GET");
  CHECK(req.uri.path[0] == "foo");
  CHECK(req.uri.path[1] == "bar baz");
  CHECK(req.protocol == "HTTP");
  CHECK(req.version == 1.1);
  auto hdr = req.header("content-type");
  REQUIRE(hdr);
  CHECK(hdr->name == "CONTENT-TYPE");
  CHECK(hdr->value == "text/html");
  hdr = req.header("content-length");
  REQUIRE(hdr);
  CHECK(hdr->name == "CONTENT-LENGTH");
  CHECK(hdr->value == "1234");
  CHECK(f == l);
}

TEST(URI with HTTP URL) {
  auto p = make_parser<uri>();
  auto str = "http://foo.bar:80/foo/bar?opt1=val1&opt2=x+y#frag1"s;
  auto f = str.begin();
  auto l = str.end();
  uri u;
  CHECK(p(f, l, u));
  CHECK(u.scheme == "http");
  CHECK(u.host == "foo.bar");
  CHECK(u.port == 80);
  CHECK(u.path[0] == "foo");
  CHECK(u.path[1] == "bar");
  CHECK(u.query["opt1"] == "val1");
  CHECK(u.query["opt2"] == "x y");
  CHECK(u.fragment == "frag1");
  CHECK(f == l);
}

TEST(URI with path only) {
  auto p = make_parser<uri>();
  auto str = "/foo/bar?opt1=val1&opt2=val2"s;
  auto f = str.begin();
  auto l = str.end();
  uri u;
  CHECK(p(f, l, u));
  CHECK(u.scheme == "");
  CHECK(u.host == "");
  CHECK(u.port == 0);
  CHECK(u.path[0] == "foo");
  CHECK(u.path[1] == "bar");
  CHECK(u.query["opt1"] == "val1");
  CHECK(u.query["opt2"] == "val2");
  CHECK(u.fragment == "");
  CHECK(f == l);
}

