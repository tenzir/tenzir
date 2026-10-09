//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/box.hpp>
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
/// Laya serve at `POST <endpoint>/systemone`.
namespace tenzir::ai::system_one {

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

/// The questions of a request, together with their JSON serialization.
struct QuestionSet {
  std::vector<Question> questions;
  std::string json;
};

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

using Answer = variant<NoulAnswer, ChoiceAnswer, ScoreAnswer>;

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

/// Serializes a request body. `state` must be a JSON string, object, or array.
auto make_body(std::string_view model, QuestionSet const& questions,
               std::string_view state) -> std::string;

/// Parses a successful response body. Fails unless every question has an
/// answer of its type.
auto parse_body(std::string_view body, QuestionSet const& questions,
                duration latency) -> Result<Decision, std::string>;

class Client {
public:
  Client(Box<HttpPool> pool, std::vector<http::Header> headers,
         std::string model, QuestionSet questions);

  /// Asks every question about `state`, a JSON string, object, or array.
  auto decide(std::string_view state) -> Task<Result<Decision, std::string>>;

  auto questions() const -> QuestionSet const&;

private:
  Box<HttpPool> pool_;
  std::vector<http::Header> headers_;
  std::string model_;
  QuestionSet questions_;
};

} // namespace tenzir::ai::system_one
