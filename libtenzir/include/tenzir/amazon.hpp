//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/async/task.hpp>
#include <tenzir/aws_credentials.hpp>
#include <tenzir/box.hpp>
#include <tenzir/http_pool.hpp>
#include <tenzir/option.hpp>
#include <tenzir/result.hpp>

#include <aws/core/AmazonWebServiceResult.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/signer/AWSAuthV4Signer.h>
#include <aws/core/http/HttpTypes.h>
#include <aws/core/utils/json/JsonSerializer.h>
#include <folly/Executor.h>

#include <chrono>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace tenzir::amazon {

/// Resolves an endpoint override from environment variables.
///
/// Checks `AWS_ENDPOINT_URL_<service_suffix>` before `AWS_ENDPOINT_URL`.
auto endpoint_override(std::string_view service_suffix) -> Option<std::string>;

/// Builds the standard regional AWS service endpoint, respecting endpoint
/// override environment variables.
auto service_endpoint_url(std::string_view service, std::string_view region,
                          std::string_view service_suffix) -> std::string;

/// Resolves the effective region for AWS requests.
///
/// Precedence: explicit region, resolved credentials region, `AWS_REGION`,
/// `AWS_DEFAULT_REGION`, AWS SDK default.
auto resolve_region(Option<std::string> explicit_region,
                    Option<resolved_aws_credentials> const& credentials)
  -> std::string;

/// Returns the origin (`scheme://host[:port]`) for an absolute URL.
auto url_origin(std::string_view input) -> std::string;

/// Returns the URL target path, query, and fragment, or `/` if empty.
auto url_relative(std::string_view input) -> std::string;

auto from_aws_string(Aws::String const& value) -> std::string;

auto aws_headers(std::vector<http::Header> const& headers)
  -> Aws::Http::HeaderValueCollection;

auto http_headers(Aws::Http::HeaderValueCollection const& headers)
  -> std::vector<http::Header>;

auto to_aws_json_result(http::Response response)
  -> Aws::AmazonWebServiceResult<Aws::Utils::Json::JsonValue>;

auto extract_aws_error_code(std::string const& body) -> std::string;

auto extract_aws_error_message(std::string const& body) -> std::string;

struct SignedHttpClientConfig {
  std::string service;
  std::string region;
  std::string endpoint;
  bool sign_requests = true;
  Option<resolved_aws_credentials> credentials;
  folly::Executor::KeepAlive<folly::IOExecutor> io_executor;
  std::chrono::milliseconds request_timeout = std::chrono::seconds{90};
  uint32_t max_retry_count = 10;
  std::chrono::milliseconds retry_delay = std::chrono::seconds{1};
};

class SignedHttpClient {
public:
  explicit SignedHttpClient(SignedHttpClientConfig config);

  template <class Request>
  auto api_call(std::string_view operation, Request& request)
    -> Task<Result<http::Response, std::string>> {
    auto payload = request.SerializePayload();
    auto body = std::string{payload.c_str(), payload.size()};
    co_return co_await post("/", std::move(body), request.GetHeaders(),
                            operation);
  }

  auto
  post(std::string path, std::string body,
       Aws::Http::HeaderValueCollection headers, std::string_view operation)
    -> Task<Result<http::Response, std::string>>;

  auto
  raw_post(std::string path, std::string body,
           Aws::Http::HeaderValueCollection headers, std::string_view operation)
    -> Task<Result<http::Response, std::string>>;

  auto post(std::string path, std::string body,
            std::vector<http::Header> headers, std::string_view operation)
    -> Task<Result<http::Response, std::string>>;

  auto
  post_unsigned(std::string path, std::string body,
                std::vector<http::Header> headers, std::string_view operation)
    -> Task<Result<http::Response, std::string>>;

  auto stream_post(std::string path, std::string body,
                   Aws::Http::HeaderValueCollection headers,
                   HttpStreamCallbacks callbacks, std::string_view operation)
    -> Task<Result<http::Response, std::string>>;

private:
  auto request_path(std::string path) const -> std::string;

  auto sign_request(std::string_view path, std::string_view body,
                    Aws::Http::HeaderValueCollection headers,
                    std::string_view operation)
    -> Result<std::vector<http::Header>, std::string>;

  std::string service_;
  std::string region_;
  std::string endpoint_;
  std::string base_path_;
  std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_;
  std::unique_ptr<Aws::Client::AWSAuthV4Signer> signer_;
  Box<HttpPool> pool_;
};

} // namespace tenzir::amazon
