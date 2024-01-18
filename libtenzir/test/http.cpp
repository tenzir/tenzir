//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/concept/parseable/tenzir/http.hpp"

#include "tenzir/concept/parseable/tenzir/uri.hpp"
#include "tenzir/concept/printable/tenzir/http.hpp"
#include "tenzir/concept/printable/tenzir/uri.hpp"
#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/test/test.hpp"

using namespace tenzir;
using namespace std::string_literals;

namespace {

auto make_item = [](std::string_view str) {
  auto result = http::request_item::parse(str);
  REQUIRE_NOT_EQUAL(result, std::nullopt);
  return *result;
};

} // namespace

TEST(parse HTTP request item) {
  auto separators = std::array{":=@", ":=", "==", "=@", "@", "=", ":"};
  auto types = std::array{
    http::request_item::file_data_json, http::request_item::data_json,
    http::request_item::url_param,      http::request_item::file_data,
    http::request_item::file_form,      http::request_item::data,
    http::request_item::header,
  };
  static_assert(separators.size() == types.size());
  for (auto i = 0u; i < types.size(); ++i) {
    auto sep = separators[i];
    auto str = fmt::format("foo{}bar", sep);
    auto item = http::request_item::parse(str);
    REQUIRE(item);
    CHECK_EQUAL(item->key, "foo");
    CHECK_EQUAL(item->value, "bar");
    CHECK_EQUAL(item->type, types[i]);
    str = fmt::format("foo{}bar\\{}", sep, sep);
    item = http::request_item::parse(str);
    REQUIRE(item);
    CHECK_EQUAL(item->key, "foo");
    auto value = fmt::format("bar\\{}", sep);
    CHECK_EQUAL(item->value, value);
  }
}

TEST(HTTP request items - JSON) {
  auto request = http::request{};
  auto items = std::vector<http::request_item>{
    make_item("Content-Type:application/json"),
    make_item("foo:=42"),
  };
  auto err = apply(items, request);
  REQUIRE_EQUAL(err, caf::none);
  // If we have a Content-Type header, apply also adds an Accept header. So here
  // we have 1 explicit header from the request item, plus one implicit Accept
  // header.
  CHECK_EQUAL(request.headers.size(), 1ull + 1);
  const auto* header = request.header("Accept");
  REQUIRE(header);
  CHECK_EQUAL(header->value, "application/json, */*");
  // Adding an item with (JSON) data makes the method POST.
  CHECK_EQUAL(request.method, "POST");
  CHECK_EQUAL(request.body, "{\"foo\": 42}");
}

TEST(HTTP request items - JSON without content type) {
  auto request = http::request{};
  auto items = std::vector<http::request_item>{
    make_item("foo:=42"),
  };
  auto err = apply(items, request);
  REQUIRE_EQUAL(err, caf::none);
  CHECK_EQUAL(request.headers.size(), 2ull);
  const auto* header = request.header("Accept");
  REQUIRE(header);
  CHECK_EQUAL(header->value, "application/json, */*");
  header = request.header("Content-Type");
  REQUIRE(header);
  CHECK_EQUAL(header->value, "application/json");
  CHECK_EQUAL(request.method, "POST");
  CHECK_EQUAL(request.body, "{\"foo\": 42}");
}

TEST(HTTP request items - urlencoded) {
  auto request = http::request{};
  auto items = std::vector<http::request_item>{
    make_item("Content-Type:application/x-www-form-urlencoded"),
    make_item("foo:=42"),
    make_item("bar:=true"),
  };
  auto err = apply(items, request);
  CHECK_EQUAL(request.headers.size(), 1ull + 1);
  const auto* header = request.header("Accept");
  REQUIRE(header);
  CHECK_EQUAL(header->value, "*/*");
  REQUIRE_EQUAL(err, caf::none);
  CHECK_EQUAL(request.method, "POST");
  CHECK_EQUAL(request.body, "foo=42&bar=true");
}

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
