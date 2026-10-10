//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ai.hpp"
#include "tenzir/ai/system_one.hpp"
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

TEST("endpoint URL keeps a complete route") {
  auto complete = ai::system_one::is_complete_route;
  auto keep = [&](std::string endpoint) {
    auto url
      = ai::make_endpoint_url(std::move(endpoint), "systemone", complete);
    REQUIRE(url);
    return std::move(url).unwrap();
  };
  CHECK_EQUAL(keep("https://api.cloudflare.com/client/v4/accounts/abc/ai/run/"
                   "@cf/cloudflare/clef"),
              "https://api.cloudflare.com/client/v4/accounts/abc/ai/run/@cf/"
              "cloudflare/clef");
  CHECK_EQUAL(keep("https://api.cloudflare.com/client/v4/accounts/abc/ai/run/"
                   "@cf/cloudflare/clef/"),
              "https://api.cloudflare.com/client/v4/accounts/abc/ai/run/@cf/"
              "cloudflare/clef");
  CHECK_EQUAL(keep("https://api.openai.com/v1/decisions"),
              "https://api.openai.com/v1/decisions");
  CHECK_EQUAL(keep("https://api.openai.com/v1/decisions/"),
              "https://api.openai.com/v1/decisions");
  CHECK_EQUAL(keep("https://api.openai.com/v1"),
              "https://api.openai.com/v1/systemone");
  // Without the predicate, the resource follows any path.
  auto url
    = ai::make_endpoint_url("https://api.openai.com/v1/decisions", "responses");
  REQUIRE(url);
  CHECK_EQUAL(url.unwrap(), "https://api.openai.com/v1/decisions/responses");
}

TEST("request format follows the path") {
  using ai::system_one::Dialect;
  CHECK(ai::system_one::dialect_of("/v1/decisions")
        == Dialect::openai_decisions);
  CHECK(ai::system_one::dialect_of("/v1/systemone") == Dialect::system_one);
  CHECK(
    ai::system_one::dialect_of("/client/v4/accounts/a/ai/run/@cf/cloudflare/"
                               "clef")
    == Dialect::system_one);
}

TEST("provider limits follow the path") {
  auto openai = ai::system_one::limits_of("/v1/decisions");
  REQUIRE(openai);
  CHECK_EQUAL(openai->max_questions, size_t{200});
  CHECK_EQUAL(openai->max_options, Option<size_t>{255});
  CHECK_EQUAL(openai->max_levels, size_t{10});
  CHECK(not openai->restricted_ids);
  auto workers_ai
    = ai::system_one::limits_of("/client/v4/accounts/a/ai/run/@cf/cloudflare/"
                                "clef");
  REQUIRE(workers_ai);
  CHECK_EQUAL(workers_ai->max_questions, size_t{64});
  CHECK_EQUAL(workers_ai->max_options, Option<size_t>{});
  CHECK_EQUAL(workers_ai->max_levels, size_t{10});
  CHECK(workers_ai->restricted_ids);
  CHECK(not ai::system_one::limits_of("/v1/systemone"));
}

TEST("Workers AI question IDs") {
  using ai::system_one::is_restricted_id;
  CHECK(is_restricted_id("risk"));
  CHECK(is_restricted_id("Risk_2.next-step"));
  CHECK(is_restricted_id(std::string(100, 'a')));
  CHECK(not is_restricted_id(""));
  CHECK(not is_restricted_id(std::string(101, 'a')));
  CHECK(not is_restricted_id("risk level"));
  CHECK(not is_restricted_id("risiko\xc3\xa4"));
  CHECK(not is_restricted_id("a/b"));
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
