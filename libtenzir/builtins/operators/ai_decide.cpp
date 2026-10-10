//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/ai.hpp>
#include <tenzir/ai/system_one.hpp>
#include <tenzir/async.hpp>
#include <tenzir/concept/printable/tenzir/json.hpp>
#include <tenzir/concept/printable/tenzir/json_printer_options.hpp>
#include <tenzir/data.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/secret.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/table_slice.hpp>

#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <string>
#include <utility>
#include <vector>

namespace tenzir::plugins::ai_decide {
namespace {

namespace system_one = ai::system_one;
using system_one::QuestionType;

/// The question ID of a question passed as positional argument. Its answer
/// lands in the `answer` field of the result.
constexpr auto single_question_id = std::string_view{"answer"};

struct DecideArgs {
  Option<located<std::string>> question;
  located<std::string> model;
  located<secret> endpoint;
  Option<located<secret>> api_key;
  Option<ast::expression> state;
  Option<located<data>> questions;
  Option<located<data>> choices;
  Option<located<data>> scale;
  ast::field_path into = ai::default_into("decide");
  located<duration> timeout{std::chrono::seconds{30}, location::unknown};
  located<uint64_t> concurrency{1, location::unknown};
  Option<located<data>> tls;
  location operator_location = location::unknown;
};

// -- question validation ------------------------------------------------------

/// Where a validated value came from, for diagnostics.
struct Source {
  /// The name of the value as the user wrote it, e.g., `scale` or
  /// `questions.risk.criteria`.
  std::string name;
  location loc;
  /// Whether the value belongs to a positional question.
  bool single;
  /// Whether to emit warnings. Validation can run more than once per
  /// operator, so only the run when the operator starts warns.
  bool warn = false;
};

/// Returns whether `value` can describe a question or criterion: a string,
/// record, or list.
auto is_content(data const& value) -> bool {
  return is<std::string>(value) or is<record>(value) or is<list>(value);
}

auto check_instructions(data const& value, Source const& source,
                        diagnostic_handler& dh) -> failure_or<void> {
  if (not is_content(value)) {
    diagnostic::error("`{}` must be a string, record, or list, but got `{}`",
                      source.name, type_kind_of_data(value))
      .primary(source.loc)
      .emit(dh);
    return failure::promise();
  }
  auto empty = match(
    value,
    [](std::string const& x) {
      return x.empty();
    },
    [](record const& x) {
      return x.empty();
    },
    [](list const& x) {
      return x.empty();
    },
    [](auto const&) {
      return false;
    });
  if (empty) {
    diagnostic::error("`{}` must not be empty", source.name)
      .primary(source.loc)
      .emit(dh);
    return failure::promise();
  }
  return {};
}

auto check_noul_criteria(data const& value, Source const& source,
                         diagnostic_handler& dh) -> failure_or<void> {
  auto const* criteria = try_as<record>(value);
  if (not criteria) {
    diagnostic::error("`{}` must be a record with `true` and `false` "
                      "descriptions, but got `{}`",
                      source.name, type_kind_of_data(value))
      .primary(source.loc)
      .hint("omit `criteria` if the question needs no descriptions")
      .emit(dh);
    return failure::promise();
  }
  if (criteria->empty()) {
    diagnostic::error("`{}` must describe `true`, `false`, or both",
                      source.name)
      .primary(source.loc)
      .emit(dh);
    return failure::promise();
  }
  for (auto const& [key, description] : *criteria) {
    if (key != "true" and key != "false") {
      diagnostic::error("`{}` has unknown outcome `{}`", source.name, key)
        .primary(source.loc)
        .hint("describe only the outcomes `true` and `false`")
        .emit(dh);
      return failure::promise();
    }
    if (not is_content(description)) {
      diagnostic::error("`{}.{}` must be a string, record, or list, but got "
                        "`{}`",
                        source.name, key, type_kind_of_data(description))
        .primary(source.loc)
        .emit(dh);
      return failure::promise();
    }
  }
  return {};
}

auto check_choice_criteria(data const& value, Source const& source,
                           diagnostic_handler& dh)
  -> failure_or<std::vector<std::string>> {
  auto const* criteria = try_as<record>(value);
  if (not criteria) {
    auto diag = diagnostic::error("`{}` must be a record that maps option "
                                  "names to descriptions, but got `{}`",
                                  source.name, type_kind_of_data(value))
                  .primary(source.loc);
    if (is<list>(value)) {
      diag = std::move(diag).hint(source.single
                                    ? "use `scale` for ordered levels"
                                    : "use a `score` question for ordered "
                                      "levels");
    }
    std::move(diag).emit(dh);
    return failure::promise();
  }
  if (criteria->size() < 2) {
    diagnostic::error("`{}` must have at least two options, but has {}",
                      source.name, criteria->size())
      .primary(source.loc)
      .emit(dh);
    return failure::promise();
  }
  auto options = std::vector<std::string>{};
  options.reserve(criteria->size());
  for (auto const& [key, description] : *criteria) {
    if (key.empty()) {
      diagnostic::error("option names in `{}` must not be empty", source.name)
        .primary(source.loc)
        .emit(dh);
      return failure::promise();
    }
    if (not is_content(description) and not is<caf::none_t>(description)) {
      diagnostic::error("option `{}` in `{}` must be described by a string, "
                        "record, list, or `null`, but got `{}`",
                        key, source.name, type_kind_of_data(description))
        .primary(source.loc)
        .emit(dh);
      return failure::promise();
    }
    options.push_back(key);
  }
  return options;
}

/// Returns the rank of a level description in a well-known ordered vocabulary,
/// together with the vocabulary it belongs to.
auto vocabulary_rank(data const& level) -> Option<std::pair<size_t, size_t>> {
  static constexpr auto vocabularies = std::array{
    std::array<std::string_view, 6>{"none", "informational", "low", "medium",
                                    "high", "critical"},
    std::array<std::string_view, 6>{"benign", "suspicious", "malicious", "", "",
                                    ""},
  };
  auto const* text = try_as<std::string>(level);
  if (not text) {
    return None{};
  }
  auto word = std::string{};
  for (auto c : *text) {
    word.push_back(
      static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
  }
  for (auto v = size_t{}; v < vocabularies.size(); ++v) {
    for (auto r = size_t{}; r < vocabularies[v].size(); ++r) {
      if (not vocabularies[v][r].empty() and word == vocabularies[v][r]) {
        return std::pair{v, r};
      }
    }
  }
  return None{};
}

/// Returns whether all levels are words of one well-known vocabulary that
/// appear from highest to lowest, such as `["high", "medium", "low"]`.
auto looks_descending(list const& levels) -> bool {
  auto previous = Option<std::pair<size_t, size_t>>{};
  for (auto const& level : levels) {
    auto rank = vocabulary_rank(level);
    if (not rank) {
      return false;
    }
    if (previous
        and (previous->first != rank->first
             or previous->second <= rank->second)) {
      return false;
    }
    previous = rank;
  }
  return true;
}

auto check_score_criteria(data const& value, Source const& source,
                          diagnostic_handler& dh) -> failure_or<size_t> {
  auto const* levels = try_as<list>(value);
  if (not levels) {
    auto diag = diagnostic::error("`{}` must be a list of levels ordered from "
                                  "lowest to highest, but got `{}`",
                                  source.name, type_kind_of_data(value))
                  .primary(source.loc);
    if (is<record>(value)) {
      diag = std::move(diag).hint(source.single
                                    ? "use `choices` for unordered options"
                                    : "use a `choice` question for unordered "
                                      "options");
    }
    std::move(diag).emit(dh);
    return failure::promise();
  }
  if (levels->size() < 2) {
    diagnostic::error("`{}` must have at least two levels, but has {}",
                      source.name, levels->size())
      .primary(source.loc)
      .emit(dh);
    return failure::promise();
  }
  for (auto i = size_t{}; i < levels->size(); ++i) {
    auto const& level = (*levels)[i];
    if (not is_content(level)) {
      diagnostic::error("level {} of `{}` must be a string, record, or list, "
                        "but got `{}`",
                        i, source.name, type_kind_of_data(level))
        .primary(source.loc)
        .emit(dh);
      return failure::promise();
    }
    for (auto j = size_t{}; j < i; ++j) {
      if ((*levels)[j] == level) {
        diagnostic::error("levels {} and {} of `{}` are identical", j, i,
                          source.name)
          .primary(source.loc)
          .hint("every level must describe a distinct step of the scale")
          .emit(dh);
        return failure::promise();
      }
    }
  }
  if (source.warn and looks_descending(*levels)) {
    diagnostic::warning("`{}` appears to be ordered from highest to lowest",
                        source.name)
      .primary(source.loc)
      .note("the score is the expected index into this list, so the first "
            "level scores 0")
      .hint("order the levels from lowest to highest")
      .emit(dh);
  }
  return levels->size();
}

auto check_question(std::string const& id, data const& value, location source,
                    bool warn, diagnostic_handler& dh)
  -> failure_or<system_one::Question> {
  auto name = fmt::format("questions.{}", id);
  if (id.empty()) {
    diagnostic::error("question IDs in `questions` must not be empty")
      .primary(source)
      .emit(dh);
    return failure::promise();
  }
  auto const* question = try_as<record>(value);
  if (not question) {
    diagnostic::error("`{}` must be a record, but got `{}`", name,
                      type_kind_of_data(value))
      .primary(source)
      .hint("describe a question with `type`, `instructions`, and `criteria`")
      .emit(dh);
    return failure::promise();
  }
  for (auto const& [key, _] : *question) {
    if (key != "type" and key != "instructions" and key != "criteria") {
      diagnostic::error("`{}` has unknown field `{}`", name, key)
        .primary(source)
        .hint("a question has only `type`, `instructions`, and `criteria`")
        .emit(dh);
      return failure::promise();
    }
  }
  auto type_value = question->find("type");
  if (type_value == question->end()) {
    diagnostic::error("`{}` has no `type`", name)
      .primary(source)
      .hint("set `type` to `noul`, `choice`, or `score`")
      .emit(dh);
    return failure::promise();
  }
  auto const* type_name = try_as<std::string>(type_value->second);
  if (not type_name) {
    diagnostic::error("`{}.type` must be a string, but got `{}`", name,
                      type_kind_of_data(type_value->second))
      .primary(source)
      .hint("set `type` to `noul`, `choice`, or `score`")
      .emit(dh);
    return failure::promise();
  }
  auto type = system_one::parse_question_type(*type_name);
  if (not type) {
    diagnostic::error("unknown question type `{}` in `{}.type`", *type_name,
                      name)
      .primary(source)
      .hint("use `noul` for yes/no questions, `choice` for picking one "
            "option, or `score` for rating on an ordered scale")
      .emit(dh);
    return failure::promise();
  }
  auto instructions = question->find("instructions");
  if (instructions == question->end()) {
    diagnostic::error("`{}` has no `instructions`", name)
      .primary(source)
      .emit(dh);
    return failure::promise();
  }
  TRY(check_instructions(
    instructions->second,
    Source{fmt::format("{}.instructions", name), source, false}, dh));
  auto result = system_one::Question{.id = id, .type = *type};
  auto criteria_source
    = Source{fmt::format("{}.criteria", name), source, false, warn};
  auto criteria = question->find("criteria");
  if (criteria == question->end()) {
    if (*type != QuestionType::noul) {
      diagnostic::error("`{}` has no `criteria`", name)
        .primary(source)
        .hint(*type == QuestionType::choice
                ? "add a record that maps option names to descriptions"
                : "add a list of levels ordered from lowest to highest")
        .emit(dh);
      return failure::promise();
    }
    return result;
  }
  switch (*type) {
    case QuestionType::noul: {
      TRY(check_noul_criteria(criteria->second, criteria_source, dh));
      break;
    }
    case QuestionType::choice: {
      TRY(result.options,
          check_choice_criteria(criteria->second, criteria_source, dh));
      break;
    }
    case QuestionType::score: {
      TRY(result.levels,
          check_score_criteria(criteria->second, criteria_source, dh));
      break;
    }
  }
  return result;
}

/// Checks which question arguments the user provided, independent of whether
/// their values are known yet.
auto check_question_arguments(Option<location> question,
                              Option<location> questions,
                              Option<location> choices, Option<location> scale,
                              location operator_location,
                              diagnostic_handler& dh) -> failure_or<void> {
  auto failed = false;
  if (question and questions) {
    diagnostic::error("cannot combine a positional question with `questions`")
      .primary(*question)
      .primary(*questions)
      .hint("add the question to `questions` instead")
      .emit(dh);
    failed = true;
  }
  if (not question and not questions) {
    diagnostic::error("expected a question")
      .primary(operator_location)
      .hint("pass a question, such as `ai_decide \"Is this event "
            "malicious?\"`, or several with `questions={...}`")
      .emit(dh);
    failed = true;
  }
  if (choices and scale) {
    diagnostic::error("cannot combine `choices` and `scale`")
      .primary(*choices)
      .primary(*scale)
      .hint("use `choices` for unordered options and `scale` for ordered "
            "levels")
      .emit(dh);
    failed = true;
  }
  for (auto [name, loc] :
       {std::pair{"choices", choices}, std::pair{"scale", scale}}) {
    if (loc and questions) {
      diagnostic::error("`{}` applies only to a positional question", name)
        .primary(*loc)
        .hint("set `criteria` within `questions` instead")
        .emit(dh);
      failed = true;
    }
  }
  if (failed) {
    return failure::promise();
  }
  return {};
}

/// Builds the question set from validated question arguments.
auto make_question_set(Option<located<std::string>> const& question,
                       Option<located<data>> const& questions,
                       Option<located<data>> const& choices,
                       Option<located<data>> const& scale, bool warn,
                       diagnostic_handler& dh)
  -> failure_or<system_one::QuestionSet> {
  if (questions) {
    auto const* fields = try_as<record>(questions->inner);
    if (not fields) {
      diagnostic::error("`questions` must be a record, but got `{}`",
                        type_kind_of_data(questions->inner))
        .primary(questions->source)
        .emit(dh);
      return failure::promise();
    }
    if (fields->empty()) {
      diagnostic::error("`questions` must not be empty")
        .primary(questions->source)
        .emit(dh);
      return failure::promise();
    }
    auto checked = std::vector<system_one::Question>{};
    for (auto const& [id, value] : *fields) {
      TRY(auto question,
          check_question(id, value, questions->source, warn, dh));
      checked.push_back(std::move(question));
    }
    return system_one::make_question_set(std::move(checked), *fields);
  }
  TENZIR_ASSERT(question);
  if (question->inner.empty()) {
    diagnostic::error("question must not be empty")
      .primary(question->source)
      .emit(dh);
    return failure::promise();
  }
  auto checked = system_one::Question{.id = std::string{single_question_id}};
  auto wire = record{};
  if (choices) {
    checked.type = QuestionType::choice;
    TRY(checked.options,
        check_choice_criteria(choices->inner,
                              Source{"choices", choices->source, true}, dh));
    wire.emplace("type", "choice");
    wire.emplace("instructions", question->inner);
    wire.emplace("criteria", choices->inner);
  } else if (scale) {
    checked.type = QuestionType::score;
    TRY(checked.levels,
        check_score_criteria(scale->inner,
                             Source{"scale", scale->source, true, warn}, dh));
    wire.emplace("type", "score");
    wire.emplace("instructions", question->inner);
    wire.emplace("criteria", scale->inner);
  } else {
    checked.type = QuestionType::noul;
    wire.emplace("type", "noul");
    wire.emplace("instructions", question->inner);
  }
  auto wire_questions = record{};
  wire_questions.emplace(std::string{single_question_id}, std::move(wire));
  auto checked_questions = std::vector<system_one::Question>{};
  checked_questions.push_back(std::move(checked));
  return system_one::make_question_set(std::move(checked_questions),
                                       wire_questions);
}

// -- execution ----------------------------------------------------------------

/// The request and outcome for one event.
struct Row {
  Option<std::string> state = None{};
  Option<system_one::Decision> decision = None{};
  Option<std::string> error = None{};
};

/// Turns the JSON text of a state value into a request state. Strings,
/// records, and lists pass through, other values become their JSON text as a
/// string, and `null` yields none.
auto make_state(std::string json) -> Option<std::string> {
  if (json.empty() or json == "null") {
    return None{};
  }
  auto first = json.front();
  if (first == '"' or first == '{' or first == '[') {
    return json;
  }
  // Numbers and booleans need no escaping.
  return fmt::format("\"{}\"", json);
}

/// Rejects questions that a hosted provider would reject in every request, so
/// that the pipeline fails once instead of warning per event.
auto check_limits(system_one::QuestionSet const& questions,
                  system_one::Limits const& limits, DecideArgs const& args,
                  diagnostic_handler& dh) -> failure_or<void> {
  if (questions.questions.size() > limits.max_questions) {
    TENZIR_ASSERT(args.questions);
    diagnostic::error("`questions` has {} questions, but {} takes at most {}",
                      questions.questions.size(), limits.provider,
                      limits.max_questions)
      .primary(args.questions->source)
      .hint("split the questions across several `ai_decide` calls")
      .emit(dh);
    return failure::promise();
  }
  for (auto const& question : questions.questions) {
    if (limits.restricted_ids
        and not system_one::is_restricted_id(question.id)) {
      TENZIR_ASSERT(args.questions);
      diagnostic::error("{} rejects the question ID `{}` in `questions`",
                        limits.provider, question.id)
        .primary(args.questions->source)
        .hint("use at most 100 letters, digits, `_`, `.`, and `-`")
        .emit(dh);
      return failure::promise();
    }
    auto count = size_t{};
    auto limit = Option<size_t>{};
    auto unit = std::string_view{};
    switch (question.type) {
      case QuestionType::noul:
        continue;
      case QuestionType::choice:
        count = question.options.size();
        limit = limits.max_options;
        unit = "options";
        break;
      case QuestionType::score:
        count = question.levels;
        limit = limits.max_levels;
        unit = "levels";
        break;
    }
    if (not limit or count <= *limit) {
      continue;
    }
    auto name = std::string{};
    auto source = args.operator_location;
    if (args.questions) {
      name = fmt::format("questions.{}.criteria", question.id);
      source = args.questions->source;
    } else if (args.choices) {
      name = "choices";
      source = args.choices->source;
    } else if (args.scale) {
      name = "scale";
      source = args.scale->source;
    }
    diagnostic::error("`{}` has {} {}, but {} takes at most {}", name, count,
                      unit, limits.provider, *limit)
      .primary(source)
      .hint("use at most {} {} with {}", *limit, unit, limits.provider)
      .emit(dh);
    return failure::promise();
  }
  return {};
}

auto connect(DecideArgs const& args, OpCtx& ctx)
  -> Task<Option<Box<system_one::Client>>> {
  auto questions = make_question_set(args.question, args.questions,
                                     args.choices, args.scale, true, ctx.dh());
  if (not questions) {
    co_return None{};
  }
  auto connection
    = co_await ai::connect(args.endpoint, args.api_key, "systemone",
                           args.timeout.inner, args.tls, ctx,
                           &system_one::is_complete_route);
  if (not connection) {
    co_return None{};
  }
  auto dialect = system_one::dialect_of(connection->path);
  if (auto limits = system_one::limits_of(connection->path);
      limits and check_limits(*questions, *limits, args, ctx.dh()).is_error()) {
    co_return None{};
  }
  co_return Box<system_one::Client>{
    std::in_place,
    std::move(connection->pool),
    std::move(connection->headers),
    args.model.inner,
    std::move(*questions),
    dialect,
  };
}

/// Sends one request per row with a state.
auto decide_all(system_one::Client& client, DecideArgs const& args,
                std::vector<Row>& rows) -> Task<void> {
  co_await ai::request_all(rows.size(), args.concurrency.inner,
                           [&](size_t i) -> Task<void> {
                             auto& row = rows[i];
                             if (not row.state) {
                               co_return;
                             }
                             auto response = co_await client.decide(*row.state);
                             if (response.is_err()) {
                               row.error = std::move(response).unwrap_err();
                               co_return;
                             }
                             row.decision = std::move(response).unwrap();
                           });
}

auto emit_warnings(std::vector<Row> const& rows, DecideArgs const& args,
                   system_one::QuestionSet const& questions,
                   diagnostic_handler& dh) -> void {
  auto null_states = std::ranges::count_if(rows, [](Row const& row) {
    return not row.state and not row.error;
  });
  if (null_states > 0) {
    auto source
      = args.state ? args.state->get_location() : args.operator_location;
    diagnostic::warning("skipped {} event{} whose `state` is `null`",
                        null_states, null_states == 1 ? "" : "s")
      .primary(source)
      .emit(dh);
  }
  for (auto const& row : rows) {
    if (row.error) {
      ai::request_failed(*row.error, args.operator_location).emit(dh);
    }
  }
  // Refusals are answers, not failures, so they warn once per question.
  auto const& ids = questions.questions;
  for (auto i = size_t{}; i < ids.size(); ++i) {
    auto refusals = std::ranges::count_if(rows, [&](Row const& row) {
      return row.decision and is<system_one::Refusal>(row.decision->answers[i]);
    });
    if (refusals > 0) {
      diagnostic::warning("the model refused to answer question `{}` for {} "
                          "event{}",
                          ids[i].id, refusals, refusals == 1 ? "" : "s")
        .primary(args.operator_location)
        .note("the answer to a refused question is `null`")
        .hint("a more specific question or more context in `state` can "
              "avoid refusals")
        .emit(dh);
    }
  }
}

template <class Field>
auto append_answer(Field field, system_one::Question const& question,
                   system_one::Answer const& answer) -> void {
  if (is<system_one::Refusal>(answer)) {
    field.null();
    return;
  }
  auto row = field.record();
  match(
    answer,
    [](system_one::Refusal const&) {
      TENZIR_UNREACHABLE();
    },
    [&](system_one::NoulAnswer const& x) {
      row.field("probability").data(x.probability);
    },
    [&](system_one::ChoiceAnswer const& x) {
      row.field("choice").data(std::string_view{x.choice});
      auto probabilities = row.field("probabilities").record();
      TENZIR_ASSERT(x.probabilities.size() == question.options.size());
      for (auto i = size_t{}; i < question.options.size(); ++i) {
        ai::set_optional(probabilities.field(question.options[i]),
                         x.probabilities[i]);
      }
      ai::set_optional(row.field("confidence"), x.confidence);
    },
    [&](system_one::ScoreAnswer const& x) {
      row.field("score").data(x.score);
      auto probabilities = row.field("probabilities").list();
      for (auto const& probability : x.probabilities) {
        if (probability) {
          probabilities.data(*probability);
        } else {
          probabilities.null();
        }
      }
      ai::set_optional(row.field("confidence"), x.confidence);
    });
}

/// Appends the result record of one decision. Works with the builders of both
/// event representations.
template <class Record>
auto append_decision(Record row, system_one::QuestionSet const& questions,
                     bool single, system_one::Decision const& decision)
  -> void {
  TENZIR_ASSERT(decision.answers.size() == questions.questions.size());
  if (single) {
    append_answer(row.field("answer"), questions.questions[0],
                  decision.answers[0]);
  } else {
    auto answers = row.field("answers").record();
    for (auto i = size_t{}; i < decision.answers.size(); ++i) {
      auto const& question = questions.questions[i];
      append_answer(answers.field(question.id), question, decision.answers[i]);
    }
  }
  ai::set_optional(row.field("model"), decision.model);
  if (not decision.usage) {
    row.field("usage").null();
  } else {
    auto const& usage = *decision.usage;
    auto record = row.field("usage").record();
    ai::set_optional(record.field("input_tokens"), usage.input_tokens);
    ai::set_optional(record.field("output_tokens"), usage.output_tokens);
    ai::set_optional(record.field("truncated"), usage.truncated);
    if (usage.truncated_questions) {
      auto ids = record.field("truncated_questions").list();
      for (auto const& id : *usage.truncated_questions) {
        ids.data(std::string_view{id});
      }
    } else {
      record.field("truncated_questions").null();
    }
  }
  row.field("latency").data(decision.latency);
}

class Decide final : public Operator<table_slice, table_slice> {
public:
  explicit Decide(DecideArgs args) : args_{std::move(args)} {
  }

  Decide(Decide const&) = delete;
  auto operator=(Decide const&) -> Decide& = delete;
  Decide(Decide&&) noexcept = default;
  auto operator=(Decide&&) noexcept -> Decide& = default;

  auto start(OpCtx& ctx) -> Task<void> override {
    client_ = co_await connect(args_, ctx);
    done_ = not client_;
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    if (done_) {
      co_return;
    }
    TENZIR_ASSERT(client_);
    auto rows = std::vector<Row>{};
    for (auto& json : ai::print_inputs(
           args_.state, args_.into, args_.operator_location, input, ctx.dh())) {
      auto& row = rows.emplace_back();
      if (json) {
        row.state = make_state(std::move(*json));
      } else {
        row.error = std::string{"failed to serialize state as JSON"};
      }
    }
    co_await decide_all(**client_, args_, rows);
    emit_warnings(rows, args_, (*client_)->questions(), ctx.dh());
    auto const& questions = (*client_)->questions();
    auto single = args_.question.has_value();
    auto results = series_builder{};
    for (auto const& row : rows) {
      if (row.decision) {
        append_decision(results.record(), questions, single, *row.decision);
      } else {
        results.null();
      }
    }
    for (auto& output :
         ai::assign_results(input, args_.into, std::move(results), ctx.dh())) {
      co_await push(std::move(output));
    }
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

private:
  DecideArgs args_;
  Option<Box<system_one::Client>> client_ = None{};
  bool done_ = false;
};

class DecideNova final : public Operator<nova::Events, nova::Events> {
public:
  explicit DecideNova(DecideArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto inputs = co_await ai::InputPrinter::make(args_.state, args_.into, ctx);
    if (not inputs) {
      co_return;
    }
    inputs_.emplace(std::move(*inputs));
    client_ = co_await connect(args_, ctx);
  }

  auto process(nova::Events input, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    if (not client_ or not inputs_ or not input.mask.any()) {
      co_return;
    }
    auto rows = std::vector<Row>{};
    for (auto& json : inputs_->print(input, ctx.dh())) {
      rows.push_back(Row{.state = make_state(std::move(json))});
    }
    co_await decide_all(**client_, args_, rows);
    emit_warnings(rows, args_, (*client_)->questions(), ctx.dh());
    auto const& questions = (*client_)->questions();
    auto single = args_.question.has_value();
    auto results = ai::build_results(
      input, [&](nova::ArrayBuilder<nova::Data>& builder, size_t i) {
        if (rows[i].decision) {
          append_decision(builder.record(), questions, single,
                          *rows[i].decision);
        } else {
          builder.null();
        }
      });
    ai::assign_results(input, args_.into, std::move(results), ctx.dh());
    co_await push(std::move(input));
  }

private:
  DecideArgs args_;
  Option<ai::InputPrinter> inputs_ = None{};
  Option<Box<system_one::Client>> client_ = None{};
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "ai_decide";
  }

  auto describe() const -> Description override {
    auto d = Describer<DecideArgs, Decide, DecideNova>{};
    auto question = d.positional("question", &DecideArgs::question);
    auto model = d.named("model", &DecideArgs::model);
    d.named("endpoint", &DecideArgs::endpoint, "string");
    d.named("api_key", &DecideArgs::api_key, "string");
    d.named("state", &DecideArgs::state, "any");
    auto questions = d.named("questions", &DecideArgs::questions, "record");
    auto choices = d.named("choices", &DecideArgs::choices, "record");
    auto scale = d.named("scale", &DecideArgs::scale, "list");
    d.named_optional("into", &DecideArgs::into);
    auto timeout = d.named_optional("timeout", &DecideArgs::timeout);
    auto concurrency
      = d.named_optional("concurrency", &DecideArgs::concurrency);
    d.named("tls", &DecideArgs::tls, "record");
    d.operator_location(&DecideArgs::operator_location);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      ai::check_arguments(ctx.get(model), ctx.get(concurrency),
                          ctx.get(timeout), ctx);
      auto structure = check_question_arguments(ctx.get_location(question),
                                                ctx.get_location(questions),
                                                ctx.get_location(choices),
                                                ctx.get_location(scale),
                                                ctx.operator_location(), ctx);
      if (not structure) {
        return {};
      }
      // Arguments that depend on unresolved bindings are checked later.
      auto complete = [&](auto arg) {
        return ctx.get_location(arg).has_value() == ctx.get(arg).has_value();
      };
      if (complete(question) and complete(questions) and complete(choices)
          and complete(scale)) {
        std::ignore
          = make_question_set(ctx.get(question), ctx.get(questions),
                              ctx.get(choices), ctx.get(scale), false, ctx);
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace
} // namespace tenzir::plugins::ai_decide

TENZIR_REGISTER_PLUGIN(tenzir::plugins::ai_decide::plugin)
