//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/concept/printable/tenzir/json_printer_options.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/http.hpp>
#include <tenzir/openai.hpp>

#include <boost/url/parse.hpp>
#include <boost/url/url.hpp>
#include <fmt/format.h>

#include <chrono>
#include <simdjson.h>
#include <string_view>

namespace tenzir::openai {
namespace {

auto optional_string(simdjson::dom::object object, std::string_view field)
  -> Option<std::string> {
  auto value = simdjson::dom::element{};
  if (object[field].get(value) != simdjson::SUCCESS) {
    return None{};
  }
  auto text = std::string_view{};
  if (value.get_string().get(text) != simdjson::SUCCESS) {
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

auto append_output_text(simdjson::dom::element output_item, std::string& text)
  -> bool {
  auto found = false;
  auto item = simdjson::dom::object{};
  if (output_item.get_object().get(item) != simdjson::SUCCESS) {
    return found;
  }
  auto content = simdjson::dom::array{};
  if (item["content"].get_array().get(content) != simdjson::SUCCESS) {
    return found;
  }
  for (auto part_element : content) {
    auto part = simdjson::dom::object{};
    if (part_element.get_object().get(part) != simdjson::SUCCESS) {
      continue;
    }
    auto type = std::string_view{};
    if (part["type"].get_string().get(type) != simdjson::SUCCESS
        or type != "output_text") {
      continue;
    }
    auto part_text = std::string_view{};
    if (part["text"].get_string().get(part_text) == simdjson::SUCCESS) {
      text += part_text;
      found = true;
    }
  }
  return found;
}

} // namespace

auto make_responses_url(std::string endpoint)
  -> Result<std::string, std::string> {
  auto parsed = boost::urls::parse_uri(endpoint);
  if (not parsed) {
    return Err{
      fmt::format("failed to parse endpoint: {}", parsed.error().message())};
  }
  auto url = boost::urls::url{*parsed};
  if (url.scheme() != "http" and url.scheme() != "https") {
    return Err{std::string{"endpoint must use HTTP or HTTPS"}};
  }
  if (url.host().empty()) {
    return Err{std::string{"endpoint must include a host"}};
  }
  auto path = std::string{url.path()};
  if (path.empty()) {
    path = "/";
  }
  while (path.size() > 1 and path.ends_with('/')) {
    path.pop_back();
  }
  if (not path.ends_with("/responses")) {
    if (not path.ends_with('/')) {
      path += '/';
    }
    path += "responses";
  }
  url.set_path(path);
  return std::string{url.buffer()};
}

auto make_responses_body(ResponsesRequest const& request)
  -> Result<std::string, std::string> {
  auto body = record{};
  body.emplace("model", request.model);
  body.emplace("input", request.input);
  body.emplace("stream", false);
  body.emplace("store", false);
  body.emplace("temperature", request.temperature);
  if (request.instructions) {
    body.emplace("instructions", *request.instructions);
  }
  if (request.max_output_tokens) {
    body.emplace("max_output_tokens", *request.max_output_tokens);
  }
  auto json = to_json(data{std::move(body)}, json_printer_options{
                                               .oneline = true,
                                             });
  if (not json) {
    return Err{
      fmt::format("failed to serialize request body: {}", json.error())};
  }
  return std::move(*json);
}

auto parse_responses_body(std::string_view body, duration latency)
  -> Result<ResponsesResult, std::string> {
  auto padded = simdjson::padded_string{body};
  auto parser = simdjson::dom::parser{};
  auto doc = simdjson::dom::element{};
  if (auto error = parser.parse(padded).get(doc); error != simdjson::SUCCESS) {
    return Err{fmt::format("failed to parse response JSON: {}",
                           simdjson::error_message(error))};
  }
  auto object = simdjson::dom::object{};
  if (auto error = doc.get_object().get(object); error != simdjson::SUCCESS) {
    return Err{std::string{"expected response JSON object"}};
  }
  auto result = ResponsesResult{};
  result.latency = latency;
  result.model = optional_string(object, "model");
  auto output = simdjson::dom::array{};
  auto found_output_text = false;
  if (object["output"].get_array().get(output) == simdjson::SUCCESS) {
    for (auto item : output) {
      found_output_text |= append_output_text(item, result.text);
    }
  }
  if (not found_output_text) {
    return Err{std::string{"response did not contain output text"}};
  }
  auto usage = simdjson::dom::object{};
  if (object["usage"].get_object().get(usage) == simdjson::SUCCESS) {
    result.usage = TokenUsage{
      .input_tokens = optional_uint64(usage, "input_tokens"),
      .output_tokens = optional_uint64(usage, "output_tokens"),
      .total_tokens = optional_uint64(usage, "total_tokens"),
    };
  }
  return result;
}

ResponsesClient::ResponsesClient(Box<HttpPool> pool,
                                 std::vector<http::Header> headers)
  : pool_{std::move(pool)}, headers_{std::move(headers)} {
}

auto ResponsesClient::create(ResponsesRequest request)
  -> Task<Result<ResponsesResult, std::string>> {
  auto body = make_responses_body(request);
  if (body.is_err()) {
    co_return Err{std::move(body).unwrap_err()};
  }
  auto request_body = std::move(body).unwrap();
  auto headers = headers_;
  http::set(headers, "Content-Type", "application/json");
  http::set(headers, "Accept", "application/json");
  http::set(headers, "Content-Length", fmt::to_string(request_body.size()));
  auto start = std::chrono::steady_clock::now();
  auto response
    = co_await pool_->post(std::move(request_body), std::move(headers));
  auto stop = std::chrono::steady_clock::now();
  auto latency = std::chrono::duration_cast<duration>(stop - start);
  if (response.is_err()) {
    co_return Err{
      fmt::format("HTTP request failed: {}", std::move(response).unwrap_err())};
  }
  auto http_response = std::move(response).unwrap();
  if (not http_response.is_status_success()) {
    constexpr auto max_body_size = size_t{1024};
    auto body = std::string_view{http_response.body};
    if (body.size() > max_body_size) {
      body = body.substr(0, max_body_size);
    }
    co_return Err{fmt::format("HTTP request returned status {}: {}",
                              http_response.status_code, body)};
  }
  co_return parse_responses_body(http_response.body, latency);
}

} // namespace tenzir::openai
