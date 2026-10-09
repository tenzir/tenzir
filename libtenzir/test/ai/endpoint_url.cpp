//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ai.hpp"
#include "tenzir/test/test.hpp"

using namespace tenzir;

TEST("endpoint URL appends the resource") {
  auto url = ai::make_endpoint_url("http://localhost:11434/v1", "responses");
  REQUIRE(url);
  CHECK_EQUAL(url.unwrap(), "http://localhost:11434/v1/responses");
  url = ai::make_endpoint_url("http://localhost:8000", "systemone");
  REQUIRE(url);
  CHECK_EQUAL(url.unwrap(), "http://localhost:8000/systemone");
}

TEST("endpoint URL keeps an existing resource") {
  auto url
    = ai::make_endpoint_url("https://api.openai.com/v1/responses", "responses");
  REQUIRE(url);
  CHECK_EQUAL(url.unwrap(), "https://api.openai.com/v1/responses");
  url = ai::make_endpoint_url("https://api.example.com/v1/systemone/",
                              "systemone");
  REQUIRE(url);
  CHECK_EQUAL(url.unwrap(), "https://api.example.com/v1/systemone");
}

TEST("endpoint URL rejects invalid endpoints without revealing them") {
  auto error = [](std::string endpoint) {
    auto url = ai::make_endpoint_url(endpoint, "responses");
    REQUIRE(url.is_err());
    auto message = std::move(url).unwrap_err();
    CHECK(not message.contains("secret"));
    return message;
  };
  CHECK_EQUAL(error("http://"), "endpoint must include a host");
  CHECK_EQUAL(error("ftp://example.com/secret"),
              "endpoint must use HTTP or HTTPS");
  CHECK_EQUAL(error("http://127.0.0.1:999999/secret"),
              "endpoint must use a port between 1 and 65535");
}
