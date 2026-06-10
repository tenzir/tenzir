//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/amazon.hpp"

#include "tenzir/detail/env.hpp"
#include "tenzir/diagnostics.hpp"

#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/endpoint/AWSPartitions.h>
#include <aws/core/http/standard/StandardHttpRequest.h>
#include <aws/core/utils/StringUtils.h>
#include <boost/url/parse.hpp>
#include <boost/url/url.hpp>

#include <regex>

namespace tenzir::amazon {

auto endpoint_override(std::string_view service_suffix) -> Option<std::string> {
  auto specific = fmt::format("AWS_ENDPOINT_URL_{}", service_suffix);
  if (auto endpoint = detail::getenv(specific)) {
    return *endpoint;
  }
  if (auto endpoint = detail::getenv("AWS_ENDPOINT_URL")) {
    return *endpoint;
  }
  return None{};
}

auto from_aws_string(Aws::String const& value) -> std::string {
  return std::string{std::string_view{value.data(), value.size()}};
}

auto aws_partition_dns_suffix(std::string_view region) -> std::string {
  auto partitions = Aws::Utils::Json::JsonValue{Aws::String{
    Aws::Endpoint::AWSPartitions::GetPartitionsBlob(),
    Aws::Endpoint::AWSPartitions::PartitionsBlobStrLen,
  }};
  if (not partitions.WasParseSuccessful()) {
    return "amazonaws.com";
  }
  auto region_name = Aws::String{region.data(), region.size()};
  auto region_text = std::string{region};
  auto partition_list = partitions.View().GetArray("partitions");
  for (auto i = size_t{}; i < partition_list.GetLength(); ++i) {
    auto partition = partition_list[i];
    auto outputs = partition.GetObject("outputs");
    auto suffix = from_aws_string(outputs.GetString("dnsSuffix"));
    if (partition.GetObject("regions").ValueExists(region_name)) {
      return suffix;
    }
    auto region_regex = from_aws_string(partition.GetString("regionRegex"));
    try {
      if (std::regex_match(region_text, std::regex{region_regex})) {
        return suffix;
      }
    } catch (std::regex_error const&) {
      // Ignore malformed SDK metadata and fall back to the default partition.
    }
  }
  return "amazonaws.com";
}

auto service_endpoint_url(std::string_view service, std::string_view region,
                          std::string_view service_suffix) -> std::string {
  if (auto endpoint = endpoint_override(service_suffix)) {
    return *endpoint;
  }
  return fmt::format("https://{}.{}.{}", service, region,
                     aws_partition_dns_suffix(region));
}

auto resolve_region(Option<std::string> explicit_region,
                    Option<resolved_aws_credentials> const& credentials)
  -> std::string {
  if (explicit_region and not explicit_region->empty()) {
    return *std::move(explicit_region);
  }
  if (credentials and not credentials->region.empty()) {
    return credentials->region;
  }
  if (auto region = detail::getenv("AWS_REGION");
      region and not region->empty()) {
    return *region;
  }
  if (auto region = detail::getenv("AWS_DEFAULT_REGION");
      region and not region->empty()) {
    return *region;
  }
  return std::string{Aws::Client::ClientConfiguration{}.region};
}

auto url_origin(std::string_view input) -> std::string {
  auto parsed = boost::urls::parse_uri_reference(input);
  auto view = parsed ? boost::urls::url_view{*parsed} : boost::urls::url_view{};
  if (not parsed or view.scheme().empty() or view.host().empty()) {
    return std::string{input};
  }
  auto url = boost::urls::url{view};
  url.set_path("");
  url.remove_query();
  url.remove_fragment();
  return std::string{url.buffer()};
}

auto url_relative(std::string_view input) -> std::string {
  auto parsed = boost::urls::parse_uri_reference(input);
  auto url
    = parsed ? boost::urls::url_view{*parsed} : boost::urls::url_view{input};
  auto result = std::string{url.encoded_target()};
  return result.empty() ? "/" : result;
}

auto aws_headers(std::vector<http::Header> const& headers)
  -> Aws::Http::HeaderValueCollection {
  auto result = Aws::Http::HeaderValueCollection{};
  for (auto const& header : headers) {
    result.emplace(Aws::String{header.name.c_str(), header.name.size()},
                   Aws::String{header.value.c_str(), header.value.size()});
  }
  return result;
}

auto http_headers(Aws::Http::HeaderValueCollection const& headers)
  -> std::vector<http::Header> {
  auto result = std::vector<http::Header>{};
  result.reserve(headers.size());
  for (auto const& [name, value] : headers) {
    result.push_back({
      std::string{name.c_str(), name.size()},
      std::string{value.c_str(), value.size()},
    });
  }
  return result;
}

auto to_aws_json_result(http::Response response)
  -> Aws::AmazonWebServiceResult<Aws::Utils::Json::JsonValue> {
  auto json_body = Aws::Utils::Json::JsonValue{
    Aws::String{response.body.c_str(), response.body.size()}};
  return {
    std::move(json_body),
    aws_headers(response.headers),
    static_cast<Aws::Http::HttpResponseCode>(response.status_code),
  };
}

auto extract_aws_error_code(std::string const& body) -> std::string {
  auto json
    = Aws::Utils::Json::JsonValue{Aws::String{body.c_str(), body.size()}};
  if (not json.WasParseSuccessful()) {
    return {};
  }
  auto view = json.View();
  for (auto key : {"__type", "code", "Code"}) {
    if (view.ValueExists(key)) {
      auto code = from_aws_string(view.GetString(key));
      if (auto pos = code.rfind('#'); pos != std::string::npos) {
        code.erase(0, pos + 1);
      }
      return code;
    }
  }
  return {};
}

auto extract_aws_error_message(std::string const& body) -> std::string {
  auto json
    = Aws::Utils::Json::JsonValue{Aws::String{body.c_str(), body.size()}};
  if (not json.WasParseSuccessful()) {
    return body;
  }
  auto view = json.View();
  for (auto key : {"message", "Message", "__type"}) {
    if (view.ValueExists(key)) {
      auto message = view.GetString(key);
      return {message.c_str(), message.size()};
    }
  }
  return body;
}

SignedHttpClient::SignedHttpClient(SignedHttpClientConfig config)
  : service_{std::move(config.service)},
    region_{std::move(config.region)},
    endpoint_{url_origin(config.endpoint)},
    base_path_{url_relative(config.endpoint)} {
  if (base_path_ == "/") {
    base_path_.clear();
  } else if (base_path_.ends_with("/")) {
    base_path_.pop_back();
  }
  if (config.sign_requests) {
    auto creds = config.credentials
                   ? std::optional{std::move(*config.credentials)}
                   : std::nullopt;
    auto provider
      = make_aws_credentials_provider(creds, std::optional{region_});
    if (not provider) {
      diagnostic::error(provider.error()).throw_();
    }
    credentials_ = std::move(*provider);
    signer_ = std::make_unique<Aws::Client::AWSAuthV4Signer>(
      credentials_, service_.c_str(),
      Aws::String{region_.c_str(), region_.size()});
  }
  auto pool_config = HttpPoolConfig{};
  pool_config.tls = endpoint_.starts_with("https://");
  pool_config.request_timeout = config.request_timeout;
  pool_config.max_retry_count = config.max_retry_count;
  pool_config.retry_delay = config.retry_delay;
  pool_config.max_concurrent_streams_per_connection = 1;
  pool_ = HttpPool::make(std::move(config.io_executor), endpoint_,
                         std::move(pool_config));
}

auto aws_response(Result<http::Response, std::string> response,
                  std::string_view operation)
  -> Result<http::Response, std::string> {
  if (response.is_err()) {
    return Err{fmt::format("{} request failed: {}", operation,
                           std::move(response).unwrap_err())};
  }
  auto http_response = std::move(response).unwrap();
  if (not http_response.is_status_success()) {
    auto detail = extract_aws_error_message(http_response.body);
    return Err{fmt::format("{} returned HTTP {}: {}", operation,
                           http_response.status_code, detail)};
  }
  return http_response;
}

auto SignedHttpClient::post(std::string path, std::string body,
                            Aws::Http::HeaderValueCollection headers,
                            std::string_view operation)
  -> Task<Result<http::Response, std::string>> {
  auto response = co_await raw_post(std::move(path), std::move(body),
                                    std::move(headers), operation);
  co_return aws_response(std::move(response), operation);
}

auto SignedHttpClient::raw_post(std::string path, std::string body,
                                Aws::Http::HeaderValueCollection headers,
                                std::string_view operation)
  -> Task<Result<http::Response, std::string>> {
  auto pool_path = request_path(path);
  auto make_headers
    = [this, path = std::move(path), body = body, headers = std::move(headers),
       operation = std::string{
         operation}]() -> Result<std::vector<http::Header>, std::string> {
    return sign_request(path, body, headers, operation);
  };
  auto response = co_await pool_->post(std::move(pool_path), std::move(body),
                                       std::move(make_headers));
  if (response.is_err()) {
    co_return Err{fmt::format("{} request failed: {}", operation,
                              std::move(response).unwrap_err())};
  }
  co_return std::move(response).unwrap();
}

auto SignedHttpClient::post(std::string path, std::string body,
                            std::vector<http::Header> headers,
                            std::string_view operation)
  -> Task<Result<http::Response, std::string>> {
  co_return co_await post(std::move(path), std::move(body),
                          aws_headers(headers), operation);
}

auto SignedHttpClient::post_unsigned(std::string path, std::string body,
                                     std::vector<http::Header> headers,
                                     std::string_view operation)
  -> Task<Result<http::Response, std::string>> {
  auto response = co_await pool_->post(request_path(std::move(path)),
                                       std::move(body), std::move(headers));
  co_return aws_response(std::move(response), operation);
}

auto SignedHttpClient::stream_post(std::string path, std::string body,
                                   Aws::Http::HeaderValueCollection headers,
                                   HttpStreamCallbacks callbacks,
                                   std::string_view operation)
  -> Task<Result<http::Response, std::string>> {
  auto pool_path = request_path(path);
  auto make_headers
    = [this, path = std::move(path), body = body, headers = std::move(headers),
       operation = std::string{
         operation}]() -> Result<std::vector<http::Header>, std::string> {
    return sign_request(path, body, headers, operation);
  };
  co_return co_await pool_->stream_post(std::move(pool_path), std::move(body),
                                        std::move(make_headers),
                                        std::move(callbacks));
}

auto SignedHttpClient::request_path(std::string path) const -> std::string {
  if (base_path_.empty()) {
    return path;
  }
  if (path == "/") {
    return base_path_;
  }
  TENZIR_ASSERT(path.starts_with("/"));
  return fmt::format("{}{}", base_path_, path);
}

auto SignedHttpClient::sign_request(std::string_view path,
                                    std::string_view body,
                                    Aws::Http::HeaderValueCollection headers,
                                    std::string_view operation)
  -> Result<std::vector<http::Header>, std::string> {
  if (not signer_) {
    return Err{fmt::format("{} requires AWS IAM credentials", operation)};
  }
  auto signed_path = request_path(std::string{path});
  auto url = fmt::format("{}{}", endpoint_, signed_path);
  auto uri = Aws::Http::URI{Aws::String{url.c_str(), url.size()}};
  auto request = Aws::Http::Standard::StandardHttpRequest{
    uri, Aws::Http::HttpMethod::HTTP_POST};
  for (auto const& [name, value] : headers) {
    request.SetHeaderValue(name, value);
  }
  auto body_stream = std::make_shared<Aws::StringStream>(
    Aws::String{body.data(), body.size()});
  request.AddContentBody(body_stream);
  request.SetHeaderValue(Aws::Http::CONTENT_LENGTH_HEADER,
                         Aws::Utils::StringUtils::to_string(body.size()));
  if (not signer_->SignRequest(request)) {
    return Err{fmt::format("failed to sign {} request", operation)};
  }
  return http_headers(request.GetHeaders());
}

} // namespace tenzir::amazon
