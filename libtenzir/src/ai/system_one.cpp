//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/ai/system_one.hpp>
#include <tenzir/concept/printable/tenzir/json_printer_options.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/http.hpp>

#include <fmt/format.h>

#include <algorithm>
#include <chrono>
#include <simdjson.h>
#include <string_view>

namespace tenzir::ai::system_one {
namespace {

auto optional_string(simdjson::dom::object object, std::string_view field)
  -> Option<std::string> {
  auto text = std::string_view{};
  if (object[field].get_string().get(text) != simdjson::SUCCESS) {
    return None{};
  }
  return std::string{text};
}

auto optional_uint64(simdjson::dom::object object, std::string_view field)
  -> Option<uint64_t> {
  auto value = simdjson::dom::element{};
  if (object[field].get(value) != simdjson::SUCCESS) {
    return None{};
  }
  auto unsigned_value = uint64_t{};
  if (value.get_uint64().get(unsigned_value) == simdjson::SUCCESS) {
    return unsigned_value;
  }
  auto signed_value = int64_t{};
  if (value.get_int64().get(signed_value) == simdjson::SUCCESS
      and signed_value >= 0) {
    return detail::narrow<uint64_t>(signed_value);
  }
  return None{};
}

/// Reads a number in `[0, max]`. Absent and `null` values yield none, values
/// of other types and numbers out of range are errors. A small tolerance
/// absorbs rounding.
auto read_number(simdjson::dom::element element, std::string_view name,
                 double max) -> Result<Option<double>, std::string> {
  if (element.is_null()) {
    return None{};
  }
  auto value = double{};
  if (element.get_double().get(value) != simdjson::SUCCESS) {
    return Err{fmt::format("a non-numeric {}", name)};
  }
  constexpr auto tolerance = 1e-6;
  if (not(value >= -tolerance and value <= max + tolerance)) {
    return Err{fmt::format("a {} of {} outside of [0, {}]", name, value, max)};
  }
  return std::clamp(value, 0.0, max);
}

auto read_number(simdjson::dom::object object, std::string_view field,
                 std::string_view name, double max)
  -> Result<Option<double>, std::string> {
  auto element = simdjson::dom::element{};
  if (object[field].get(element) != simdjson::SUCCESS) {
    return None{};
  }
  return read_number(element, name, max);
}

/// Reads probabilities keyed by option name or level index. Servers send a
/// JSON object; a JSON array is accepted for levels as well. Missing entries
/// yield none.
auto read_probabilities(simdjson::dom::object answer,
                        std::span<std::string const> keys)
  -> Result<std::vector<Option<double>>, std::string> {
  auto result = std::vector<Option<double>>(keys.size());
  auto element = simdjson::dom::element{};
  if (answer["probabilities"].get(element) != simdjson::SUCCESS
      or element.is_null()) {
    return result;
  }
  auto object = simdjson::dom::object{};
  if (element.get_object().get(object) == simdjson::SUCCESS) {
    for (auto i = size_t{}; i < keys.size(); ++i) {
      TRY(result[i], read_number(object, keys[i], "probability", 1.0));
    }
    return result;
  }
  auto array = simdjson::dom::array{};
  if (element.get_array().get(array) != simdjson::SUCCESS) {
    return Err{std::string{"probabilities that are neither a record nor a "
                           "list"}};
  }
  auto i = size_t{};
  for (auto value : array) {
    if (i == keys.size()) {
      break;
    }
    TRY(result[i++], read_number(value, "probability", 1.0));
  }
  return result;
}

auto level_keys(size_t levels) -> std::vector<std::string> {
  auto result = std::vector<std::string>{};
  result.reserve(levels);
  for (auto i = size_t{}; i < levels; ++i) {
    result.push_back(fmt::to_string(i));
  }
  return result;
}

/// Reads the answer to one question. Errors describe what the answer has, so
/// that the caller can prefix them with the question.
auto read_answer(simdjson::dom::object answer, Question const& question)
  -> Result<Answer, std::string> {
  auto expected = to_string(question.type);
  auto type_element = simdjson::dom::element{};
  if (answer["type"].get(type_element) == simdjson::SUCCESS) {
    auto type = std::string_view{};
    if (type_element.get_string().get(type) != simdjson::SUCCESS) {
      return Err{std::string{"a non-string type"}};
    }
    if (type != expected) {
      return Err{fmt::format("type `{}`, expected `{}`", type, expected)};
    }
  }
  switch (question.type) {
    case QuestionType::noul: {
      // Jev and Laya send `noul`; other implementations call it `probability`.
      TRY(auto probability, read_number(answer, "noul", "probability", 1.0));
      if (not probability) {
        TRY(probability,
            read_number(answer, "probability", "probability", 1.0));
      }
      if (not probability) {
        return Err{std::string{"no probability"}};
      }
      return NoulAnswer{.probability = *probability};
    }
    case QuestionType::choice: {
      auto choice = optional_string(answer, "choice");
      if (not choice) {
        return Err{std::string{"no choice"}};
      }
      if (std::ranges::find(question.options, *choice)
          == question.options.end()) {
        return Err{fmt::format("unknown choice `{}`", *choice)};
      }
      TRY(auto probabilities, read_probabilities(answer, question.options));
      TRY(auto confidence,
          read_number(answer, "confidence", "confidence", 1.0));
      return ChoiceAnswer{
        .choice = std::move(*choice),
        .probabilities = std::move(probabilities),
        .confidence = confidence,
      };
    }
    case QuestionType::score: {
      auto max_score = static_cast<double>(question.levels - 1);
      TRY(auto score, read_number(answer, "score", "score", max_score));
      if (not score) {
        return Err{std::string{"no score"}};
      }
      TRY(auto probabilities,
          read_probabilities(answer, level_keys(question.levels)));
      TRY(auto confidence,
          read_number(answer, "confidence", "confidence", 1.0));
      return ScoreAnswer{
        .score = *score,
        .probabilities = std::move(probabilities),
        .confidence = confidence,
      };
    }
  }
  TENZIR_UNREACHABLE();
}

auto parse_answer(simdjson::dom::object answers, Question const& question)
  -> Result<Answer, std::string> {
  auto answer = simdjson::dom::object{};
  if (answers[question.id].get_object().get(answer) != simdjson::SUCCESS) {
    return Err{
      fmt::format("response has no answer for question `{}`", question.id)};
  }
  auto result = read_answer(answer, question);
  if (result.is_err()) {
    return Err{fmt::format("answer for question `{}` has {}", question.id,
                           std::move(result).unwrap_err())};
  }
  return result;
}

auto parse_usage(simdjson::dom::object usage) -> Usage {
  auto result = Usage{
    .input_tokens = optional_uint64(usage, "input_tokens"),
    .output_tokens = optional_uint64(usage, "output_tokens"),
  };
  auto truncated = false;
  if (usage["truncated"].get_bool().get(truncated) == simdjson::SUCCESS) {
    result.truncated = truncated;
  }
  auto ids = simdjson::dom::array{};
  if (usage["truncated_questions"].get_array().get(ids) == simdjson::SUCCESS) {
    auto& list = result.truncated_questions.emplace();
    for (auto id : ids) {
      auto text = std::string_view{};
      if (id.get_string().get(text) == simdjson::SUCCESS) {
        list.emplace_back(text);
      }
    }
  }
  return result;
}

} // namespace

auto to_string(QuestionType type) -> std::string_view {
  switch (type) {
    case QuestionType::noul:
      return "noul";
    case QuestionType::choice:
      return "choice";
    case QuestionType::score:
      return "score";
  }
  TENZIR_UNREACHABLE();
}

auto parse_question_type(std::string_view name) -> Option<QuestionType> {
  for (auto type :
       {QuestionType::noul, QuestionType::choice, QuestionType::score}) {
    if (name == to_string(type)) {
      return type;
    }
  }
  return None{};
}

auto make_body(std::string_view model, QuestionSet const& questions,
               std::string_view state) -> std::string {
  auto quoted_model
    = to_json(data{std::string{model}}, json_printer_options{.oneline = true});
  TENZIR_ASSERT(quoted_model);
  return fmt::format(R"({{"model":{},"questions":{},"state":{}}})",
                     *quoted_model, questions.json, state);
}

auto parse_body(std::string_view body, QuestionSet const& questions,
                duration latency) -> Result<Decision, std::string> {
  auto padded = simdjson::padded_string{body};
  auto parser = simdjson::dom::parser{};
  auto doc = simdjson::dom::element{};
  if (auto error = parser.parse(padded).get(doc); error != simdjson::SUCCESS) {
    return Err{fmt::format("failed to parse response JSON: {}",
                           simdjson::error_message(error))};
  }
  auto object = simdjson::dom::object{};
  if (doc.get_object().get(object) != simdjson::SUCCESS) {
    return Err{std::string{"expected response JSON object"}};
  }
  auto answers = simdjson::dom::object{};
  if (object["answers"].get_object().get(answers) != simdjson::SUCCESS) {
    return Err{std::string{"response has no `answers` object"}};
  }
  auto result = Decision{};
  result.latency = latency;
  result.model = optional_string(object, "model");
  result.answers.reserve(questions.questions.size());
  for (auto const& question : questions.questions) {
    auto answer = parse_answer(answers, question);
    if (answer.is_err()) {
      return Err{std::move(answer).unwrap_err()};
    }
    result.answers.push_back(std::move(answer).unwrap());
  }
  auto usage = simdjson::dom::object{};
  if (object["usage"].get_object().get(usage) == simdjson::SUCCESS) {
    result.usage = parse_usage(usage);
  }
  return result;
}

Client::Client(Box<HttpPool> pool, std::vector<http::Header> headers,
               std::string model, QuestionSet questions)
  : pool_{std::move(pool)},
    headers_{std::move(headers)},
    model_{std::move(model)},
    questions_{std::move(questions)} {
}

auto Client::questions() const -> QuestionSet const& {
  return questions_;
}

auto Client::decide(std::string_view state)
  -> Task<Result<Decision, std::string>> {
  auto body = make_body(model_, questions_, state);
  auto headers = headers_;
  http::set(headers, "Content-Type", "application/json");
  http::set(headers, "Accept", "application/json");
  http::set(headers, "Content-Length", fmt::to_string(body.size()));
  auto start = std::chrono::steady_clock::now();
  auto response = co_await pool_->post(std::move(body), std::move(headers));
  auto stop = std::chrono::steady_clock::now();
  auto latency = std::chrono::duration_cast<duration>(stop - start);
  if (response.is_err()) {
    co_return Err{
      fmt::format("HTTP request failed: {}", std::move(response).unwrap_err())};
  }
  auto http_response = std::move(response).unwrap();
  if (not http_response.is_status_success()) {
    constexpr auto max_body_size = size_t{1024};
    auto excerpt = std::string_view{http_response.body};
    if (excerpt.size() > max_body_size) {
      excerpt = excerpt.substr(0, max_body_size);
    }
    co_return Err{fmt::format("HTTP request returned status {}: {}",
                              http_response.status_code, excerpt)};
  }
  co_return parse_body(http_response.body, questions_, latency);
}

} // namespace tenzir::ai::system_one
