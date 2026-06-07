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

#include <cstdint>
#include <string>
#include <vector>

namespace tenzir::openai {

struct ResponsesRequest {
  std::string model;
  Option<std::string> instructions = None{};
  std::string input;
  double temperature = 0.0;
  Option<uint64_t> max_output_tokens = None{};
};

struct TokenUsage {
  Option<uint64_t> input_tokens = None{};
  Option<uint64_t> output_tokens = None{};
  Option<uint64_t> total_tokens = None{};
};

struct ResponsesResult {
  std::string text;
  Option<std::string> model = None{};
  Option<TokenUsage> usage = None{};
  duration latency = duration::zero();
};

/// Appends `/responses` to an OpenAI-compatible base endpoint.
auto make_responses_url(std::string endpoint)
  -> Result<std::string, std::string>;

/// Serializes a Responses API request body.
auto make_responses_body(ResponsesRequest const& request)
  -> Result<std::string, std::string>;

/// Parses a successful Responses API HTTP response body.
auto parse_responses_body(std::string_view body, duration latency)
  -> Result<ResponsesResult, std::string>;

class ResponsesClient {
public:
  ResponsesClient(Box<HttpPool> pool, std::vector<http::Header> headers);

  auto create(ResponsesRequest request)
    -> Task<Result<ResponsesResult, std::string>>;

private:
  Box<HttpPool> pool_;
  std::vector<http::Header> headers_;
};

} // namespace tenzir::openai
