//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ai/system_one.hpp"

#include "tenzir/data.hpp"
#include "tenzir/test/test.hpp"

using namespace tenzir;
using namespace std::chrono_literals;

namespace {

auto make_wire() -> record {
  return record{
    {"pii", record{{"type", "noul"}, {"instructions", "Personal data?"}}},
    {"team",
     record{{"type", "choice"},
            {"instructions", "Which team?"},
            {"criteria", record{{"soc", caf::none}, {"it", caf::none}}}}},
    {"risk", record{{"type", "score"},
                    {"instructions", "How risky?"},
                    {"criteria", list{"low", "mid", "high"}}}},
  };
}

auto make_questions() -> ai::system_one::QuestionSet {
  return ai::system_one::make_question_set(
    {
      {.id = "pii", .type = ai::system_one::QuestionType::noul},
      {
        .id = "team",
        .type = ai::system_one::QuestionType::choice,
        .options = {"soc", "it"},
      },
      {.id = "risk", .type = ai::system_one::QuestionType::score, .levels = 3},
    },
    make_wire());
}

/// Returns the OpenAI translation of a single question named `q`.
auto translate(record question) -> std::string {
  return ai::system_one::make_question_set({},
                                           record{{"q", std::move(question)}})
    .openai_json;
}

} // namespace

TEST("system one request body") {
  auto body = ai::system_one::make_body("jev \"latest\"", make_questions(),
                                        R"({"cmd":"ls"})");
  CHECK_EQUAL(
    body, R"({"model":"jev \"latest\"","questions":{"pii":{"type":"noul",)"
          R"("instructions":"Personal data?"},"team":{"type":"choice",)"
          R"("instructions":"Which team?","criteria":{"soc":null,"it":null}},)"
          R"("risk":{"type":"score","instructions":"How risky?",)"
          R"("criteria":["low","mid","high"]}},"state":{"cmd":"ls"}})");
}

TEST("openai question translation") {
  // Structured instructions and descriptions travel as their JSON text.
  CHECK_EQUAL(
    translate(record{
      {"type", "choice"},
      {"instructions", record{{"task", "Pick a team"}}},
      {"criteria", record{{"soc", list{"alerts", "triage"}}, {"it", "IT"}}},
    }),
    R"([{"name":"q","type":"choice","instructions":"{\"task\":\"Pick a team\"}",)"
    R"("choices":[{"value":"soc","description":"[\"alerts\",\"triage\"]"},)"
    R"({"value":"it","description":"IT"}]}])");
  // A `noul` question with one outcome only describes that outcome.
  CHECK_EQUAL(translate(record{
                {"type", "noul"},
                {"instructions", "Is it PII?"},
                {"criteria", record{{"false", "No personal data."}}},
              }),
              R"([{"name":"q","type":"predicate",)"
              R"("instructions":"Is it PII?\nFalse: No personal data."}])");
  // A record level without a `label` becomes its JSON text, and a level
  // description that is no content is dropped.
  CHECK_EQUAL(translate(record{
                {"type", "score"},
                {"instructions", "How risky?"},
                {"criteria",
                 list{
                   record{{"name", "low"}},
                   record{{"label", "high"}, {"description", int64_t{42}}},
                 }},
              }),
              R"([{"name":"q","type":"score","instructions":"How risky?",)"
              R"("levels":[{"label":"{\"name\":\"low\"}"},)"
              R"({"label":"high"}]}])");
}

TEST("openai decisions request body") {
  using ai::system_one::Dialect;
  auto structured
    = ai::system_one::make_body("gpt-6-luna", make_questions(),
                                R"({"cmd":"ls"})", Dialect::openai_decisions);
  CHECK_EQUAL(
    structured,
    R"({"model":"gpt-6-luna","input":"{\"cmd\":\"ls\"}","questions":)"
    R"([{"name":"pii","type":"predicate","instructions":"Personal data?"},)"
    R"({"name":"team","type":"choice","instructions":"Which team?",)"
    R"("choices":[{"value":"soc"},{"value":"it"}]},)"
    R"({"name":"risk","type":"score","instructions":"How risky?",)"
    R"("levels":[{"label":"low"},{"label":"mid"},{"label":"high"}]}]})");
  auto text = ai::system_one::make_body("gpt-6-luna", make_questions(),
                                        R"("ls")", Dialect::openai_decisions);
  CHECK(text.starts_with(R"({"model":"gpt-6-luna","input":"ls","questions":)"));
}

TEST("openai decisions response recorded from gpt-6-luna") {
  // The response of `POST /v1/decisions` for a predicate, a choice, and a score
  // question, as recorded on 2026-10-10.
  auto questions = ai::system_one::QuestionSet{
    .questions = {
      {.id = "risky", .type = ai::system_one::QuestionType::noul},
      {
        .id = "tactic",
        .type = ai::system_one::QuestionType::choice,
        .options = {"discovery", "execution", "persistence"},
      },
      {
        .id = "severity",
        .type = ai::system_one::QuestionType::score,
        .levels = 3,
      },
    },
    .json = "{}",
  };
  auto parsed = ai::system_one::parse_body(
    R"({"model":"gpt-6-luna","answers":[)"
    R"({"type":"predicate","name":"risky","probability":1.0},)"
    R"({"type":"choice","name":"tactic","choice":"execution","probabilities":)"
    R"([{"value":"discovery","probability":0.28},)"
    R"({"value":"execution","probability":0.69},)"
    R"({"value":"persistence","probability":0.03}],"confidence":0.53},)"
    R"({"type":"score","name":"severity","score":1.87,"probabilities":)"
    R"([{"value":0,"label":"Routine administration","probability":0.03},)"
    R"({"value":1,"label":"Unusual but plausible","probability":0.07},)"
    R"({"value":2,"label":"Likely malicious","probability":0.9}],)"
    R"("confidence":0.81}],"usage":{"input_tokens":410,)"
    R"("input_tokens_details":{"cached_tokens":0,"cache_write_tokens":0},)"
    R"("output_tokens":0,"total_tokens":410}})",
    questions, duration::zero());
  REQUIRE(parsed);
  auto decision = std::move(parsed).unwrap();
  REQUIRE_EQUAL(decision.answers.size(), size_t{3});
  auto const* risky = try_as<ai::system_one::NoulAnswer>(decision.answers[0]);
  REQUIRE(risky);
  CHECK_EQUAL(risky->probability, 1.0);
  auto const* tactic
    = try_as<ai::system_one::ChoiceAnswer>(decision.answers[1]);
  REQUIRE(tactic);
  CHECK_EQUAL(tactic->choice, "execution");
  REQUIRE_EQUAL(tactic->probabilities.size(), size_t{3});
  CHECK_EQUAL(tactic->probabilities[0], Option<double>{0.28});
  CHECK_EQUAL(tactic->probabilities[1], Option<double>{0.69});
  CHECK_EQUAL(tactic->confidence, Option<double>{0.53});
  auto const* severity
    = try_as<ai::system_one::ScoreAnswer>(decision.answers[2]);
  REQUIRE(severity);
  CHECK_EQUAL(severity->score, 1.87);
  REQUIRE_EQUAL(severity->probabilities.size(), size_t{3});
  CHECK_EQUAL(severity->probabilities[2], Option<double>{0.9});
  CHECK_EQUAL(severity->confidence, Option<double>{0.81});
  CHECK_EQUAL(decision.model, Option<std::string>{"gpt-6-luna"});
  REQUIRE(decision.usage);
  CHECK_EQUAL(decision.usage->input_tokens, Option<uint64_t>{410});
  CHECK_EQUAL(decision.usage->output_tokens, Option<uint64_t>{0});
}

TEST("openai decisions response with a refusal recorded from gpt-6-luna") {
  // The response of `POST /v1/decisions` for the input `ls`, as recorded on
  // 2026-10-10. OpenAI refuses the vague question and answers the others.
  auto questions = ai::system_one::QuestionSet{
    .questions = {
      {.id = "a", .type = ai::system_one::QuestionType::noul},
      {.id = "b", .type = ai::system_one::QuestionType::noul},
      {
        .id = "c",
        .type = ai::system_one::QuestionType::choice,
        .options = {"discovery", "execution"},
      },
    },
    .json = "{}",
  };
  auto parsed = ai::system_one::parse_body(
    R"({"model":"gpt-6-luna","answers":[{"type":"refusal","name":"a"},)"
    R"({"type":"predicate","name":"b","probability":0.39},)"
    R"({"type":"choice","name":"c","choice":"discovery","probabilities":)"
    R"([{"value":"discovery","probability":0.91},)"
    R"({"value":"execution","probability":0.09}],"confidence":0.82}],)"
    R"("usage":{"input_tokens":19,"output_tokens":0,"total_tokens":19}})",
    questions, duration::zero());
  REQUIRE(parsed);
  auto decision = std::move(parsed).unwrap();
  REQUIRE_EQUAL(decision.answers.size(), size_t{3});
  CHECK(is<ai::system_one::Refusal>(decision.answers[0]));
  auto const* b = try_as<ai::system_one::NoulAnswer>(decision.answers[1]);
  REQUIRE(b);
  CHECK_EQUAL(b->probability, 0.39);
  auto const* c = try_as<ai::system_one::ChoiceAnswer>(decision.answers[2]);
  REQUIRE(c);
  CHECK_EQUAL(c->choice, "discovery");
}

TEST("openai decisions response errors") {
  auto parse = [](std::string_view body) {
    auto parsed
      = ai::system_one::parse_body(body, make_questions(), duration::zero());
    REQUIRE(parsed.is_err());
    return std::move(parsed).unwrap_err();
  };
  CHECK_EQUAL(parse(R"({"answers":[]})"),
              "response has no answer for question `pii`");
  CHECK_EQUAL(
    parse(R"({"answers":[{"type":"choice","name":"pii","probability":0.5}]})"),
    "answer for question `pii` has type `choice`, expected `predicate`");
  CHECK_EQUAL(parse(R"({"answers":[{"type":1,"name":"pii"}]})"),
              "answer for question `pii` has a non-string type");
  CHECK_EQUAL(parse(R"({"answers":[{"type":"predicate","name":"pii"}]})"),
              "answer for question `pii` has no probability");
  CHECK_EQUAL(parse(R"({"answers":[{"name":"pii","probability":-9}]})"),
              "answer for question `pii` has a probability of -9 outside of "
              "[0, 1]");
  CHECK_EQUAL(parse(R"({"answers":[{"name":"pii","probability":0.5},)"
                    R"({"name":"team","choice":"hr"}]})"),
              "answer for question `team` has unknown choice `hr`");
}

TEST("openai decisions probabilities skip entries without a value") {
  auto parsed = ai::system_one::parse_body(
    R"({"answers":[{"name":"pii","probability":0.5},)"
    R"({"name":"team","choice":"it","probabilities":)"
    R"([{"probability":0.4},{"value":"it","probability":0.6}]},)"
    R"({"name":"risk","score":0.8,"probabilities":)"
    R"([{"probability":0.8},{"value":null,"probability":0.1},)"
    R"({"value":1,"probability":0.2}]}]})",
    make_questions(), duration::zero());
  REQUIRE(parsed);
  auto decision = std::move(parsed).unwrap();
  auto const* team = try_as<ai::system_one::ChoiceAnswer>(decision.answers[1]);
  REQUIRE(team);
  CHECK_EQUAL(team->probabilities[0], Option<double>{});
  CHECK_EQUAL(team->probabilities[1], Option<double>{0.6});
  auto const* risk = try_as<ai::system_one::ScoreAnswer>(decision.answers[2]);
  REQUIRE(risk);
  CHECK_EQUAL(risk->probabilities[0], Option<double>{});
  CHECK_EQUAL(risk->probabilities[1], Option<double>{0.2});
  CHECK_EQUAL(risk->probabilities[2], Option<double>{});
}

TEST("openai decisions score levels accept integral numbers") {
  auto parsed = ai::system_one::parse_body(
    R"({"answers":[{"name":"pii","probability":0.5},)"
    R"({"name":"team","choice":"it"},)"
    R"({"name":"risk","score":1.2,"probabilities":)"
    R"([{"value":0.0,"probability":0.1},{"value":1e0,"probability":0.6},)"
    R"({"value":2,"probability":0.3},{"value":1.5,"probability":0.9},)"
    R"({"value":3,"probability":0.9},{"value":-1,"probability":0.9}]}]})",
    make_questions(), duration::zero());
  REQUIRE(parsed);
  auto decision = std::move(parsed).unwrap();
  auto const* risk = try_as<ai::system_one::ScoreAnswer>(decision.answers[2]);
  REQUIRE(risk);
  CHECK_EQUAL(risk->probabilities[0], Option<double>{0.1});
  CHECK_EQUAL(risk->probabilities[1], Option<double>{0.6});
  CHECK_EQUAL(risk->probabilities[2], Option<double>{0.3});
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

TEST("system one response in a Workers AI envelope") {
  auto parsed = ai::system_one::parse_body(
    R"({"result":{"model":"clef","answers":{"pii":{"type":"noul",)"
    R"("noul":0.9813},"team":{"choice":"it"},"risk":{"type":"score",)"
    R"("score":1.9206}},"usage":{"input_tokens":254,"output_tokens":0}},)"
    R"("success":true,"errors":[],"messages":[]})",
    make_questions(), duration::zero());
  REQUIRE(parsed);
  auto decision = std::move(parsed).unwrap();
  REQUIRE_EQUAL(decision.answers.size(), size_t{3});
  auto const* pii = try_as<ai::system_one::NoulAnswer>(decision.answers[0]);
  REQUIRE(pii);
  CHECK_EQUAL(pii->probability, 0.9813);
  auto const* risk = try_as<ai::system_one::ScoreAnswer>(decision.answers[2]);
  REQUIRE(risk);
  CHECK_EQUAL(risk->score, 1.9206);
  CHECK_EQUAL(decision.model, Option<std::string>{"clef"});
  REQUIRE(decision.usage);
  CHECK_EQUAL(decision.usage->input_tokens, Option<uint64_t>{254});
}

TEST("system one response recorded from Cloudflare Workers AI") {
  // The response of `POST /ai/run/@cf/cloudflare/clef` for a `noul` and a
  // `score` question, as recorded on 2026-10-09.
  auto questions = ai::system_one::QuestionSet{
    .questions = {
      {.id = "answer", .type = ai::system_one::QuestionType::noul},
      {.id = "risk", .type = ai::system_one::QuestionType::score, .levels = 3},
    },
    .json = R"({"answer":{},"risk":{}})",
  };
  auto parsed = ai::system_one::parse_body(
    R"({"result":{"model":"clef","answers":{"answer":{"type":"noul",)"
    R"("noul":0.9813},"risk":{"type":"score","score":1.9206,"legend":)"
    R"({"0":"Routine administration","1":"Unusual but plausible",)"
    R"("2":"Likely malicious"},"probabilities":{"0":0.0191,"1":0.0411,)"
    R"("2":0.9398},"confidence":0.8278}},"usage":{"input_tokens":254,)"
    R"("output_tokens":0}},"success":true,"errors":[],"messages":[]})",
    questions, duration::zero());
  REQUIRE(parsed);
  auto decision = std::move(parsed).unwrap();
  auto const* risk = try_as<ai::system_one::ScoreAnswer>(decision.answers[1]);
  REQUIRE(risk);
  CHECK_EQUAL(risk->score, 1.9206);
  CHECK_EQUAL(risk->probabilities[2], Option<double>{0.9398});
  CHECK_EQUAL(risk->confidence, Option<double>{0.8278});
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
  CHECK_EQUAL(parse(R"({"result":{},"success":true})"),
              "response has no `answers` object");
  CHECK_EQUAL(parse(R"({"result":[],"success":false})"),
              "response has no `answers` object");
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
