//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/curl.hpp"

#include "tenzir/collect.hpp"
#include "tenzir/test/test.hpp"

using namespace tenzir;

TEST("overwriting HTTP headers") {
  auto easy = curl::easy{};
  easy.set_http_header("Foo", "42");
  auto headers = collect(easy.headers());
  CHECK_EQUAL(headers.size(), 1ull);
  easy.set_http_header("Foo", "Bar");
  headers = collect(easy.headers());
  REQUIRE_EQUAL(headers.size(), 1ull);
  auto [name, value] = headers[0];
  CHECK_EQUAL(name, "Foo");
  CHECK_EQUAL(value, "Bar");
}

TEST("valid URL") {
  auto url = curl::url{};
  // Set URL.
  auto code = url.set(curl::url::part::url, "http://localhost");
  CHECK_EQUAL(code, curl::url::code::ok);
  // Get host.
  auto [host_code, host] = url.get(curl::url::part::host);
  CHECK_EQUAL(host_code, curl::url::code::ok);
  REQUIRE(host);
  CHECK_EQUAL(*host, "localhost");
  // Get full URL.
  auto [full_url_code, full_url] = url.get(curl::url::part::url);
  CHECK_EQUAL(full_url_code, curl::url::code::ok);
  REQUIRE(full_url);
  CHECK_EQUAL(*full_url, "http://localhost/");
}

TEST("invalid URL") {
  auto url = curl::url{};
  auto code = url.set(curl::url::part::url, "localhost");
  CHECK_EQUAL(code, curl::url::code::bad_scheme);
}

TEST("default scheme") {
  auto url = curl::url{};
  auto code = url.set(curl::url::part::url, "localhost",
                      curl::url::flags::default_scheme);
  CHECK_EQUAL(code, curl::url::code::ok);
  auto [full_url_code, full_url] = url.get(curl::url::part::url);
  CHECK_EQUAL(full_url_code, curl::url::code::ok);
  REQUIRE(full_url);
  CHECK_EQUAL(*full_url, "https://localhost/");
}
