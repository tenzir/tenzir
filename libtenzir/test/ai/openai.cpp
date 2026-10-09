//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ai/openai.hpp"

#include "tenzir/test/test.hpp"

using namespace tenzir;
using namespace std::chrono_literals;

TEST("responses request body") {
  auto body = ai::openai::make_responses_body(ai::openai::ResponsesRequest{
    .model = "test-model",
    .instructions = "keep it short",
    .input = R"({"message":"hello"})",
    .temperature = 0.25,
    .max_output_tokens = uint64_t{42},
  });
  REQUIRE(body);
  CHECK(body.unwrap().contains(R"("model":"test-model")"));
  CHECK(body.unwrap().contains(R"("input":"{\"message\":\"hello\"}")"));
  CHECK(body.unwrap().contains(R"("stream":false)"));
  CHECK(body.unwrap().contains(R"("store":false)"));
  CHECK(body.unwrap().contains(R"("temperature":0.25)"));
  CHECK(body.unwrap().contains(R"("instructions":"keep it short")"));
  CHECK(body.unwrap().contains(R"("max_output_tokens":42)"));
}

TEST("responses request body omits unset temperature") {
  auto body = ai::openai::make_responses_body(ai::openai::ResponsesRequest{
    .model = "test-model",
    .input = "hello",
  });
  REQUIRE(body);
  CHECK(not body.unwrap().contains("temperature"));
  body = ai::openai::make_responses_body(ai::openai::ResponsesRequest{
    .model = "test-model",
    .input = "hello",
    .temperature = 0.0,
  });
  REQUIRE(body);
  CHECK(body.unwrap().contains(R"("temperature":0)"));
}

TEST("responses body parser") {
  auto response = R"({
    "id": "resp_test",
    "object": "response",
    "status": "completed",
    "model": "test-model",
    "output": [
      {
        "type": "message",
        "role": "assistant",
        "content": [
          {"type": "output_text", "text": "{\"answer\":42}"},
          {"type": "refusal", "refusal": null}
        ]
      }
    ],
    "usage": {
      "input_tokens": 11,
      "output_tokens": 7,
      "total_tokens": 18
    }
  })";
  auto parsed = ai::openai::parse_responses_body(
    response, std::chrono::duration_cast<duration>(123ms));
  REQUIRE(parsed);
  auto result = std::move(parsed).unwrap();
  CHECK_EQUAL(result.text, R"({"answer":42})");
  REQUIRE(result.model);
  CHECK_EQUAL(*result.model, "test-model");
  REQUIRE(result.usage);
  REQUIRE(result.usage->input_tokens);
  CHECK_EQUAL(*result.usage->input_tokens, uint64_t{11});
  REQUIRE(result.usage->output_tokens);
  CHECK_EQUAL(*result.usage->output_tokens, uint64_t{7});
  REQUIRE(result.usage->total_tokens);
  CHECK_EQUAL(*result.usage->total_tokens, uint64_t{18});
  CHECK_EQUAL(result.latency, std::chrono::duration_cast<duration>(123ms));
}

TEST("responses body parser accepts empty output text") {
  auto parsed = ai::openai::parse_responses_body(
    R"({"status":"completed","output":[{"content":[{"type":"output_text","text":""}]}]})",
    duration::zero());
  REQUIRE(parsed);
  CHECK_EQUAL(parsed.unwrap().text, "");
}

TEST("responses body parser rejects missing text") {
  auto parsed = ai::openai::parse_responses_body(
    R"({"status":"completed","output":[]})", duration::zero());
  REQUIRE(parsed.is_err());
  CHECK_EQUAL(parsed.unwrap_err(), "response did not contain output text");
}
