//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/ai/system_one.hpp>
#include <tenzir/concept/printable/tenzir/json.hpp>
#include <tenzir/concept/printable/tenzir/json_printer_options.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/http.hpp>

#include <fmt/format.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <simdjson.h>
#include <span>
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

/// How a response lays out its answers. System One servers send a record keyed
/// by question ID, and OpenAI sends a list of named answers.
enum class AnswerLayout {
  keyed,
  named,
};

/// Reads probabilities keyed by option name or level index. Servers send a
/// JSON object; a JSON array is accepted for levels as well. Missing entries
/// yield none.
auto read_keyed_probabilities(simdjson::dom::object answer,
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

/// Returns the level that an integral number in `[0, levels)` denotes, so that
/// `2`, `2.0`, and `2e0` all name the third level.
auto level_index(simdjson::dom::element value, size_t levels)
  -> Option<size_t> {
  auto number = double{};
  if (value.get_double().get(number) != simdjson::SUCCESS) {
    return None{};
  }
  if (not(number >= 0.0) or number >= static_cast<double>(levels)
      or number != std::floor(number)) {
    return None{};
  }
  return static_cast<size_t>(number);
}

/// Reads the probabilities of a named answer, a list of objects with the
/// `value` of an option or level and its `probability`. Entries without a
/// known `value` are skipped, and missing entries yield none.
auto read_named_probabilities(simdjson::dom::object answer,
                              Question const& question, size_t size)
  -> Result<std::vector<Option<double>>, std::string> {
  auto result = std::vector<Option<double>>(size);
  auto list = simdjson::dom::array{};
  if (answer["probabilities"].get_array().get(list) != simdjson::SUCCESS) {
    return result;
  }
  auto position_of = [&](simdjson::dom::element value) -> Option<size_t> {
    if (question.type != QuestionType::choice) {
      return level_index(value, size);
    }
    auto text = std::string_view{};
    if (value.get_string().get(text) != simdjson::SUCCESS) {
      return None{};
    }
    auto it = std::ranges::find(question.options, text);
    if (it == question.options.end()) {
      return None{};
    }
    return static_cast<size_t>(it - question.options.begin());
  };
  for (auto element : list) {
    auto entry = simdjson::dom::object{};
    if (element.get_object().get(entry) != simdjson::SUCCESS) {
      continue;
    }
    auto value = simdjson::dom::element{};
    if (entry["value"].get(value) != simdjson::SUCCESS) {
      continue;
    }
    auto position = position_of(value);
    if (not position) {
      continue;
    }
    TRY(result[*position],
        read_number(entry, "probability", "probability", 1.0));
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

/// Reads the probabilities of a `choice` or `score` answer in either layout.
auto read_probabilities(simdjson::dom::object answer, Question const& question,
                        AnswerLayout layout)
  -> Result<std::vector<Option<double>>, std::string> {
  auto keys = question.type == QuestionType::choice
                ? question.options
                : level_keys(question.levels);
  if (layout == AnswerLayout::keyed) {
    return read_keyed_probabilities(answer, keys);
  }
  return read_named_probabilities(answer, question, keys.size());
}

/// Reads the answer to one question. Errors describe what the answer has, so
/// that the caller can prefix them with the question. OpenAI calls a `noul`
/// question a `predicate`.
auto read_answer(simdjson::dom::object answer, Question const& question,
                 AnswerLayout layout) -> Result<Answer, std::string> {
  auto expected
    = layout == AnswerLayout::named and question.type == QuestionType::noul
        ? std::string_view{"predicate"}
        : to_string(question.type);
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
      TRY(auto probabilities, read_probabilities(answer, question, layout));
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
      TRY(auto probabilities, read_probabilities(answer, question, layout));
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

/// Finds the answer whose key is `id` in a record of answers.
auto find_keyed(simdjson::dom::object answers, std::string_view id)
  -> Option<simdjson::dom::object> {
  auto answer = simdjson::dom::object{};
  if (answers[id].get_object().get(answer) != simdjson::SUCCESS) {
    return None{};
  }
  return answer;
}

/// Finds the object in `list` whose `name` is `name`.
auto find_named(simdjson::dom::array list, std::string_view name)
  -> Option<simdjson::dom::object> {
  for (auto element : list) {
    auto object = simdjson::dom::object{};
    if (element.get_object().get(object) != simdjson::SUCCESS) {
      continue;
    }
    if (optional_string(object, "name") == Option<std::string>{name}) {
      return object;
    }
  }
  return None{};
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

auto to_json_text(data const& value) -> std::string {
  auto json = to_json(value, json_printer_options{.oneline = true});
  TENZIR_ASSERT(json);
  return std::move(*json);
}

/// Returns whether `value` can describe a question or criterion.
auto is_content(data const& value) -> bool {
  return is<std::string>(value) or is<record>(value) or is<list>(value);
}

/// Returns a string as is and any other value as its JSON text, because OpenAI
/// takes the descriptions of questions, options, and levels as strings.
auto content_text(data const& value) -> std::string {
  if (auto const* text = try_as<std::string>(value)) {
    return *text;
  }
  return to_json_text(value);
}

/// Joins serialized JSON values into a JSON array. The values are serialized
/// one by one, because a list of records with different fields would unify
/// into one schema and gain `null` fields.
auto json_array(std::vector<std::string> const& items) -> std::string {
  return fmt::format("[{}]", fmt::join(items, ","));
}

/// Translates a validated System One question into an OpenAI Decisions
/// question: `noul` becomes `predicate`, the criteria of a `choice` become
/// `choices`, and the criteria of a `score` become `levels`. A level can be a
/// record with a `label` and a `description`. A `noul` question has no
/// criteria in OpenAI, so their descriptions join the instructions.
auto to_openai_question(std::string const& id, record const& question)
  -> std::string {
  auto field = [&](std::string_view name) -> data const* {
    auto it = question.find(name);
    return it == question.end() ? nullptr : &it->second;
  };
  auto type = std::string{};
  if (auto const* value = field("type")) {
    if (auto const* text = try_as<std::string>(*value)) {
      type = *text;
    }
  }
  auto instructions = std::string{};
  if (auto const* value = field("instructions")) {
    instructions = content_text(*value);
  }
  auto const* criteria = field("criteria");
  auto extra = std::string{};
  if (type == "choice") {
    auto choices = std::vector<std::string>{};
    auto const* options = criteria ? try_as<record>(*criteria) : nullptr;
    for (auto const& [value, description] : options ? *options : record{}) {
      auto choice = record{};
      choice.emplace("value", value);
      if (not is<caf::none_t>(description)) {
        choice.emplace("description", content_text(description));
      }
      choices.push_back(to_json_text(choice));
    }
    extra = fmt::format(R"(,"choices":{})", json_array(choices));
  } else if (type == "score") {
    auto levels = std::vector<std::string>{};
    auto const* described = criteria ? try_as<list>(*criteria) : nullptr;
    for (auto const& level : described ? *described : list{}) {
      auto entry = record{};
      auto const* fields = try_as<record>(level);
      auto label = fields ? fields->find("label") : record::const_iterator{};
      if (fields and label != fields->end()
          and is<std::string>(label->second)) {
        entry.emplace("label", label->second);
        if (auto description = fields->find("description");
            description != fields->end() and is_content(description->second)) {
          entry.emplace("description", content_text(description->second));
        }
      } else {
        entry.emplace("label", content_text(level));
      }
      levels.push_back(to_json_text(entry));
    }
    extra = fmt::format(R"(,"levels":{})", json_array(levels));
  } else if (auto const* outcomes
             = criteria ? try_as<record>(*criteria) : nullptr) {
    for (auto const* outcome : {"true", "false"}) {
      if (auto it = outcomes->find(outcome); it != outcomes->end()) {
        instructions
          += fmt::format("\n{}: {}",
                         std::string_view{outcome} == "true" ? "True" : "False",
                         content_text(it->second));
      }
    }
  }
  return fmt::format(R"({{"name":{},"type":"{}","instructions":{}{}}})",
                     to_json_text(data{id}),
                     type == "noul" ? "predicate" : type,
                     to_json_text(data{instructions}), extra);
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

auto is_complete_route(std::string_view path) -> bool {
  return path.contains("/ai/run/") or path.ends_with("/decisions");
}

auto limits_of(std::string_view path) -> Option<Limits> {
  if (dialect_of(path) == Dialect::openai_decisions) {
    return Limits{
      .provider = "OpenAI",
      .max_questions = 200,
      .max_options = size_t{255},
      .max_levels = 10,
    };
  }
  if (path.contains("/ai/run/")) {
    return Limits{
      .provider = "Workers AI",
      .max_questions = 64,
      .max_options = None{},
      .max_levels = 10,
      .restricted_ids = true,
    };
  }
  return None{};
}

auto is_restricted_id(std::string_view id) -> bool {
  constexpr auto max_size = size_t{100};
  return not id.empty() and id.size() <= max_size
         and std::ranges::all_of(id, [](char c) {
               return (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z')
                      or (c >= '0' and c <= '9') or c == '_' or c == '.'
                      or c == '-';
             });
}

auto dialect_of(std::string_view path) -> Dialect {
  return path.ends_with("/decisions") ? Dialect::openai_decisions
                                      : Dialect::system_one;
}

auto make_question_set(std::vector<Question> questions, record const& wire)
  -> QuestionSet {
  auto openai = std::vector<std::string>{};
  openai.reserve(wire.size());
  for (auto const& [id, value] : wire) {
    if (auto const* question = try_as<record>(value)) {
      openai.push_back(to_openai_question(id, *question));
    }
  }
  return QuestionSet{
    .questions = std::move(questions),
    .json = to_json_text(wire),
    .openai_json = json_array(openai),
  };
}

auto make_body(std::string_view model, QuestionSet const& questions,
               std::string_view state, Dialect dialect) -> std::string {
  auto quoted_model = to_json_text(data{std::string{model}});
  if (dialect == Dialect::openai_decisions) {
    TENZIR_ASSERT(not questions.openai_json.empty(),
                  "build the question set with `make_question_set`");
    // The evidence is a string. A structured state travels as its JSON text.
    auto input = state.starts_with('"')
                   ? std::string{state}
                   : to_json_text(data{std::string{state}});
    return fmt::format(R"({{"model":{},"input":{},"questions":{}}})",
                       quoted_model, input, questions.openai_json);
  }
  TENZIR_ASSERT(not questions.json.empty(),
                "build the question set with `make_question_set`");
  return fmt::format(R"({{"model":{},"questions":{},"state":{}}})",
                     quoted_model, questions.json, state);
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
  // Cloudflare Workers AI wraps the response of a model in a `result` field
  // next to `success`, `errors`, and `messages`.
  auto result_object = simdjson::dom::object{};
  if (object["answers"].error() != simdjson::SUCCESS
      and object["result"].get_object().get(result_object)
            == simdjson::SUCCESS) {
    object = result_object;
  }
  auto result = Decision{};
  result.latency = latency;
  result.model = optional_string(object, "model");
  result.answers.reserve(questions.questions.size());
  auto named = simdjson::dom::array{};
  auto keyed = simdjson::dom::object{};
  auto layout = AnswerLayout::keyed;
  if (object["answers"].get_array().get(named) == simdjson::SUCCESS) {
    layout = AnswerLayout::named;
  } else if (object["answers"].get_object().get(keyed) != simdjson::SUCCESS) {
    return Err{std::string{"response has no `answers` object"}};
  }
  for (auto const& question : questions.questions) {
    auto answer = layout == AnswerLayout::keyed
                    ? find_keyed(keyed, question.id)
                    : find_named(named, question.id);
    if (not answer) {
      return Err{
        fmt::format("response has no answer for question `{}`", question.id)};
    }
    // OpenAI answers a question that it refuses with a `refusal`, and still
    // answers the other questions of the request.
    if (layout == AnswerLayout::named
        and optional_string(*answer, "type")
              == Option<std::string>{"refusal"}) {
      result.answers.emplace_back(Refusal{});
      continue;
    }
    auto parsed = read_answer(*answer, question, layout);
    if (parsed.is_err()) {
      return Err{fmt::format("answer for question `{}` has {}", question.id,
                             std::move(parsed).unwrap_err())};
    }
    result.answers.push_back(std::move(parsed).unwrap());
  }
  auto usage = simdjson::dom::object{};
  if (object["usage"].get_object().get(usage) == simdjson::SUCCESS) {
    result.usage = parse_usage(usage);
  }
  return result;
}

Client::Client(Box<HttpPool> pool, std::vector<http::Header> headers,
               std::string model, QuestionSet questions, Dialect dialect)
  : pool_{std::move(pool)},
    headers_{std::move(headers)},
    model_{std::move(model)},
    questions_{std::move(questions)},
    dialect_{dialect} {
}

auto Client::questions() const -> QuestionSet const& {
  return questions_;
}

auto Client::decide(std::string_view state)
  -> Task<Result<Decision, std::string>> {
  auto body = make_body(model_, questions_, state, dialect_);
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
