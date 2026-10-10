//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/box.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/http.hpp>
#include <tenzir/http_pool.hpp>
#include <tenzir/option.hpp>
#include <tenzir/result.hpp>
#include <tenzir/time.hpp>
#include <tenzir/variant.hpp>

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

/// A client for the System One API, which decision models such as Jev and
/// Laya serve at `POST <endpoint>/systemone`. It also speaks the variants that
/// Cloudflare Workers AI and the OpenAI Decisions API serve.
namespace tenzir::ai::system_one {

/// The request format of an endpoint. The response format follows from the
/// response itself.
enum class Dialect {
  /// `POST /systemone` with a `state` and a record of `questions`.
  system_one,
  /// `POST /v1/decisions` with an `input` and a list of named `questions`.
  openai_decisions,
};

/// Returns whether the path of an endpoint addresses a decision model, so that
/// no `/systemone` follows it. This holds for a Cloudflare Workers AI run URL
/// such as `/ai/run/@cf/cloudflare/clef`, and for the OpenAI `/v1/decisions`
/// endpoint.
auto is_complete_route(std::string_view path) -> bool;

/// Returns the request format that the path of an endpoint calls for.
auto dialect_of(std::string_view path) -> Dialect;

enum class QuestionType {
  noul,
  choice,
  score,
};

auto to_string(QuestionType type) -> std::string_view;

auto parse_question_type(std::string_view name) -> Option<QuestionType>;

/// A validated question, reduced to what reading its answer requires.
struct Question {
  std::string id;
  QuestionType type = QuestionType::noul;
  /// The option names of a `choice` question, in criteria order.
  std::vector<std::string> options = {};
  /// The number of levels of a `score` question.
  size_t levels = 0;
};

/// The questions of a request, together with their JSON serialization in
/// each dialect. Build it with `make_question_set`, so that both
/// serializations describe the same questions.
struct QuestionSet {
  std::vector<Question> questions;
  /// The questions as a System One record keyed by question ID.
  std::string json;
  /// The questions as the list of named questions that OpenAI expects.
  std::string openai_json = {};
};

/// Builds a question set from validated questions and their System One wire
/// form, a record that maps question IDs to records with `type`,
/// `instructions`, and optional `criteria`. Translates the wire form into the
/// OpenAI Decisions format: `noul` becomes `predicate`, the criteria of a
/// `choice` become `choices`, and the criteria of a `score` become `levels`.
/// Instructions and descriptions that are not strings travel as their JSON
/// text, and the descriptions of a `noul` question join its instructions.
auto make_question_set(std::vector<Question> questions, record const& wire)
  -> QuestionSet;

struct NoulAnswer {
  double probability = 0.0;
};

struct ChoiceAnswer {
  std::string choice;
  /// The probability of each option, in criteria order.
  std::vector<Option<double>> probabilities;
  Option<double> confidence = None{};
};

struct ScoreAnswer {
  double score = 0.0;
  /// The probability of each level, from lowest to highest.
  std::vector<Option<double>> probabilities;
  Option<double> confidence = None{};
};

/// The model declined to answer the question. OpenAI refuses single
/// questions, for example when the evidence is too short to judge, and still
/// answers the others.
struct Refusal {};

using Answer = variant<NoulAnswer, ChoiceAnswer, ScoreAnswer, Refusal>;

struct Usage {
  Option<uint64_t> input_tokens = None{};
  Option<uint64_t> output_tokens = None{};
  Option<bool> truncated = None{};
  Option<std::vector<std::string>> truncated_questions = None{};
};

struct Decision {
  /// One answer per question, in question order.
  std::vector<Answer> answers;
  Option<std::string> model = None{};
  Option<Usage> usage = None{};
  duration latency = duration::zero();
};

/// The largest question set that a hosted provider accepts. The provider
/// rejects a larger one in every request with HTTP 400.
struct Limits {
  /// The name of the provider, for diagnostics.
  std::string_view provider;
  size_t max_questions;
  /// The most options per `choice` question, if the provider limits them.
  Option<size_t> max_options;
  size_t max_levels;
  /// Whether question IDs must consist of at most 100 letters, digits, `_`,
  /// `.`, and `-`.
  bool restricted_ids = false;
};

/// Returns the limits of the provider that the path of an endpoint addresses,
/// if the provider is known to have any:
///
/// - The OpenAI Decisions API takes at most 200 questions, 255 options, and
///   10 levels, as probed on 2026-10-10.
/// - Cloudflare Workers AI takes at most 64 questions and 10 levels, and
///   restricts question IDs, as its Clef schema states.
auto limits_of(std::string_view path) -> Option<Limits>;

/// Returns whether a question ID is one that Workers AI accepts.
auto is_restricted_id(std::string_view id) -> bool;

/// Serializes a request body. `state` must be a JSON string, object, or array.
/// OpenAI takes the evidence as text, so it receives structured states as
/// their JSON text.
auto make_body(std::string_view model, QuestionSet const& questions,
               std::string_view state, Dialect dialect = Dialect::system_one)
  -> std::string;

/// Parses a successful response body. Fails unless every question has an
/// answer of its type or a refusal. Accepts the answers as a record keyed by
/// question ID, as System One servers send them, and as a list of named
/// answers, as OpenAI sends them. Reads through the `result` field in which
/// Workers AI wraps a response.
auto parse_body(std::string_view body, QuestionSet const& questions,
                duration latency) -> Result<Decision, std::string>;

class Client {
public:
  Client(Box<HttpPool> pool, std::vector<http::Header> headers,
         std::string model, QuestionSet questions,
         Dialect dialect = Dialect::system_one);

  /// Asks every question about `state`, a JSON string, object, or array.
  auto decide(std::string_view state) -> Task<Result<Decision, std::string>>;

  auto questions() const -> QuestionSet const&;

private:
  Box<HttpPool> pool_;
  std::vector<http::Header> headers_;
  std::string model_;
  QuestionSet questions_;
  Dialect dialect_;
};

} // namespace tenzir::ai::system_one
