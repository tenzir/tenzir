//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/concept/parseable/tenzir/http.hpp"

#include "tenzir/concept/printable/tenzir/http.hpp"
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

TEST(HTTP request items - URL param) {
  auto request = http::request{};
  auto items = std::vector<http::request_item>{
    make_item("foo==42"),
    make_item("bar==true"),
  };
  request.uri = "https://example.org/";
  auto err = apply(items, request);
  CHECK_EQUAL(err, caf::none);
  CHECK_EQUAL(request.uri, "https://example.org/?foo=42&bar=true");
  request.uri = "https://example.org/?";
  err = apply(items, request);
  CHECK_EQUAL(err, caf::none);
  CHECK_EQUAL(request.uri, "https://example.org/?foo=42&bar=true");
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
