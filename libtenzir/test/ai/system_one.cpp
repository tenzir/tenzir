//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ai/system_one.hpp"

#include "tenzir/test/test.hpp"

using namespace tenzir;
using namespace std::chrono_literals;

namespace {

auto make_questions() -> ai::system_one::QuestionSet {
  return {
    .questions = {
      {.id = "pii", .type = ai::system_one::QuestionType::noul},
      {
        .id = "team",
        .type = ai::system_one::QuestionType::choice,
        .options = {"soc", "it"},
      },
      {.id = "risk", .type = ai::system_one::QuestionType::score, .levels = 3},
    },
    .json = R"({"pii":{}})",
  };
}

} // namespace

TEST("system one request body") {
  auto body = ai::system_one::make_body("jev \"latest\"", make_questions(),
                                        R"({"cmd":"ls"})");
  CHECK_EQUAL(
    body,
    R"({"model":"jev \"latest\"","questions":{"pii":{}},"state":{"cmd":"ls"}})");
}

TEST("system one response") {
  auto response = R"({
    "model": "jev-1.13.0",
    "answers": {
      "risk": {
        "type": "score",
        "score": 1.5,
        "legend": {"0": "low", "1": "mid", "2": "high"},
        "probabilities": {"0": 0.1, "1": 0.3, "2": 0.6},
        "confidence": 0.8
      },
      "team": {
        "type": "choice",
        "choice": "it",
        "probabilities": {"it": 0.9, "soc": 0.1},
        "confidence": 0.81
      },
      "pii": {"type": "noul", "noul": 0.95}
    },
    "usage": {
      "input_tokens": 296,
      "output_tokens": 0,
      "truncated": true,
      "truncated_questions": ["risk"]
    }
  })";
  auto parsed = ai::system_one::parse_body(
    response, make_questions(), std::chrono::duration_cast<duration>(5ms));
  REQUIRE(parsed);
  auto decision = std::move(parsed).unwrap();
  REQUIRE_EQUAL(decision.answers.size(), size_t{3});
  auto const* pii = try_as<ai::system_one::NoulAnswer>(decision.answers[0]);
  REQUIRE(pii);
  CHECK_EQUAL(pii->probability, 0.95);
  auto const* team = try_as<ai::system_one::ChoiceAnswer>(decision.answers[1]);
  REQUIRE(team);
  CHECK_EQUAL(team->choice, "it");
  REQUIRE_EQUAL(team->probabilities.size(), size_t{2});
  CHECK_EQUAL(team->probabilities[0], Option<double>{0.1});
  CHECK_EQUAL(team->probabilities[1], Option<double>{0.9});
  CHECK_EQUAL(team->confidence, Option<double>{0.81});
  auto const* risk = try_as<ai::system_one::ScoreAnswer>(decision.answers[2]);
  REQUIRE(risk);
  CHECK_EQUAL(risk->score, 1.5);
  REQUIRE_EQUAL(risk->probabilities.size(), size_t{3});
  CHECK_EQUAL(risk->probabilities[2], Option<double>{0.6});
  CHECK_EQUAL(decision.model, Option<std::string>{"jev-1.13.0"});
  REQUIRE(decision.usage);
  CHECK_EQUAL(decision.usage->input_tokens, Option<uint64_t>{296});
  CHECK_EQUAL(decision.usage->truncated, Option<bool>{true});
  REQUIRE(decision.usage->truncated_questions);
  CHECK_EQUAL(decision.usage->truncated_questions->size(), size_t{1});
  CHECK_EQUAL(decision.latency, std::chrono::duration_cast<duration>(5ms));
}

TEST("system one response with probability alias and missing metadata") {
  auto response = R"({
    "answers": {
      "pii": {"type": "noul", "probability": 0.1},
      "team": {"choice": "soc"},
      "risk": {"score": 0, "probabilities": [0.9, 0.1]}
    }
  })";
  auto parsed
    = ai::system_one::parse_body(response, make_questions(), duration::zero());
  REQUIRE(parsed);
  auto decision = std::move(parsed).unwrap();
  auto const* pii = try_as<ai::system_one::NoulAnswer>(decision.answers[0]);
  REQUIRE(pii);
  CHECK_EQUAL(pii->probability, 0.1);
  auto const* team = try_as<ai::system_one::ChoiceAnswer>(decision.answers[1]);
  REQUIRE(team);
  CHECK_EQUAL(team->probabilities[0], Option<double>{});
  CHECK_EQUAL(team->confidence, Option<double>{});
  auto const* risk = try_as<ai::system_one::ScoreAnswer>(decision.answers[2]);
  REQUIRE(risk);
  CHECK_EQUAL(risk->score, 0.0);
  CHECK_EQUAL(risk->probabilities[1], Option<double>{0.1});
  CHECK_EQUAL(risk->probabilities[2], Option<double>{});
  CHECK(not decision.model);
  CHECK(not decision.usage);
}

TEST("system one response tolerates rounding") {
  auto parsed = ai::system_one::parse_body(
    R"({"answers":{"pii":{"noul":1.0000000001},"team":{"choice":"it"},)"
    R"("risk":{"score":-0.0000001}}})",
    make_questions(), duration::zero());
  REQUIRE(parsed);
  auto decision = std::move(parsed).unwrap();
  auto const* pii = try_as<ai::system_one::NoulAnswer>(decision.answers[0]);
  REQUIRE(pii);
  CHECK_EQUAL(pii->probability, 1.0);
  auto const* risk = try_as<ai::system_one::ScoreAnswer>(decision.answers[2]);
  REQUIRE(risk);
  CHECK_EQUAL(risk->score, 0.0);
}

TEST("system one response errors") {
  auto parse = [](std::string_view body) {
    auto parsed
      = ai::system_one::parse_body(body, make_questions(), duration::zero());
    REQUIRE(parsed.is_err());
    return std::move(parsed).unwrap_err();
  };
  CHECK_EQUAL(parse("[]"), "expected response JSON object");
  CHECK_EQUAL(parse("{}"), "response has no `answers` object");
  CHECK_EQUAL(parse(R"({"answers":{}})"),
              "response has no answer for question `pii`");
  CHECK_EQUAL(parse(R"({"answers":{"pii":{"type":"choice","noul":0.5}}})"),
              "answer for question `pii` has type `choice`, expected `noul`");
  CHECK_EQUAL(parse(R"({"answers":{"pii":{"type":42,"noul":0.5}}})"),
              "answer for question `pii` has a non-string type");
  CHECK_EQUAL(parse(R"({"answers":{"pii":{"noul":-9}}})"),
              "answer for question `pii` has a probability of -9 outside of "
              "[0, 1]");
  CHECK_EQUAL(parse(R"({"answers":{"pii":{"noul":"high"}}})"),
              "answer for question `pii` has a non-numeric probability");
  CHECK_EQUAL(
    parse(R"({"answers":{"pii":{"noul":0.5},"team":{"choice":"hr"}}})"),
    "answer for question `team` has unknown choice `hr`");
  CHECK_EQUAL(parse(R"({"answers":{"pii":{"noul":0.5},"team":{"choice":"it",)"
                    R"("probabilities":{"it":1.5}}}})"),
              "answer for question `team` has a probability of 1.5 outside "
              "of [0, 1]");
  CHECK_EQUAL(parse(R"({"answers":{"pii":{"noul":0.5},"team":{"choice":"it",)"
                    R"("confidence":7}}})"),
              "answer for question `team` has a confidence of 7 outside of "
              "[0, 1]");
  CHECK_EQUAL(parse(R"({"answers":{"pii":{"noul":0.5},"team":{"choice":"it"},)"
                    R"("risk":{"score":999}}})"),
              "answer for question `risk` has a score of 999 outside of "
              "[0, 2]");
  CHECK_EQUAL(parse(R"({"answers":{"pii":{"noul":0.5},"team":{"choice":"it"},)"
                    R"("risk":{"score":1,"probabilities":[-5,6]}}})"),
              "answer for question `risk` has a probability of -5 outside of "
              "[0, 1]");
  CHECK_EQUAL(
    parse(
      R"({"answers":{"pii":{"noul":0.5},"team":{"choice":"it"},"risk":{}}})"),
    "answer for question `risk` has no score");
}
