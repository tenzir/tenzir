//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "async_sqs_queue.hpp"

#include <tenzir/aws_iam.hpp>
#include <tenzir/detail/env.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/diagnostics.hpp>

#include <aws/core/AmazonWebServiceResult.h>
#include <aws/core/http/standard/StandardHttpRequest.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/sqs/model/DeleteMessageRequest.h>
#include <aws/sqs/model/GetQueueUrlRequest.h>
#include <aws/sqs/model/GetQueueUrlResult.h>
#include <aws/sqs/model/MessageSystemAttributeName.h>
#include <aws/sqs/model/ReceiveMessageRequest.h>
#include <aws/sqs/model/ReceiveMessageResult.h>
#include <aws/sqs/model/SendMessageRequest.h>
#include <aws/sqs/model/SendMessageResult.h>
#include <folly/coro/Retry.h>

namespace tenzir::plugins::sqs {

namespace {

// Match the AWS SDK's DefaultRetryStrategy.
constexpr auto retry_max_retries = uint32_t{10};
constexpr auto retry_min_backoff = std::chrono::milliseconds{25};
constexpr auto retry_max_backoff = std::chrono::seconds{30};
constexpr auto retry_jitter = 0.0;

/// Thrown for transient SQS errors that should be retried.
struct sqs_retriable_error : std::runtime_error {
  uint16_t status_code;
  std::string detail;

  sqs_retriable_error(uint16_t status, std::string detail)
    : std::runtime_error(fmt::format("SQS API returned HTTP {}", status)),
      status_code{status},
      detail{std::move(detail)} {
  }

  explicit sqs_retriable_error(std::string transport_error)
    : std::runtime_error("SQS HTTP request failed"),
      status_code{0},
      detail{std::move(transport_error)} {
  }
};

/// Returns true for HTTP status codes that are worth retrying.
auto is_retriable_status(uint16_t status) -> bool {
  return status == 429 or (status >= 500 and status <= 599);
}

/// Determines the SQS endpoint URL from the region and environment variables.
auto sqs_endpoint_url(std::string_view region) -> std::string {
  // AWS_ENDPOINT_URL_SQS takes precedence over AWS_ENDPOINT_URL.
  if (auto url = detail::getenv("AWS_ENDPOINT_URL_SQS")) {
    return *url;
  }
  if (auto url = detail::getenv("AWS_ENDPOINT_URL")) {
    return *url;
  }
  return fmt::format("https://sqs.{}.amazonaws.com", region);
}

/// Returns the origin (`scheme://host[:port]`) of a URL, i.e., everything up
/// to (but excluding) the path. Used to point the HTTP pool at the right
/// host when a full queue URL is supplied as the queue argument.
auto url_origin(std::string_view url) -> std::string {
  auto scheme_end = url.find("://");
  if (scheme_end == std::string_view::npos) {
    return std::string{url};
  }
  auto path_start = url.find('/', scheme_end + 3);
  if (path_start == std::string_view::npos) {
    return std::string{url};
  }
  return std::string{url.substr(0, path_start)};
}

/// Extracts an error message from an SQS JSON error response body.
auto extract_sqs_error_message(const std::string& body) -> std::string {
  auto json
    = Aws::Utils::Json::JsonValue{Aws::String{body.c_str(), body.size()}};
  for (auto key : {"message", "Message", "__type"}) {
    if (json.View().ValueExists(key)) {
      auto msg = json.View().GetString(key);
      return {msg.c_str(), msg.size()};
    }
  }
  return body;
}

/// Sends a signed SQS API request via proxygen and returns the response body.
///
/// Constructs an AWS SDK HttpRequest from the given SQS request object, signs
/// it with SigV4, and sends it through the provided HttpPool. The response
/// is parsed into an AmazonWebServiceResult<JsonValue> for SDK result types.
///
/// Transient errors (HTTP 429, 5xx, transport failures) are retried with
/// exponential backoff. Non-retriable errors (4xx other than 429) propagate
/// immediately.
template <class Request>
auto sqs_api_call(HttpPool& pool, Aws::Client::AWSAuthV4Signer& signer,
                  const std::string& endpoint_url, Request& request)
  -> Task<Aws::AmazonWebServiceResult<Aws::Utils::Json::JsonValue>> {
  try {
    co_return co_await folly::coro::retryWithExponentialBackoff(
      retry_max_retries, retry_min_backoff, retry_max_backoff, retry_jitter,
      [&]() -> Task<Aws::AmazonWebServiceResult<Aws::Utils::Json::JsonValue>> {
        // Serialize and sign on each attempt so retries get a fresh timestamp.
        auto payload = request.SerializePayload();
        auto sdk_headers = request.GetHeaders();
        auto uri = Aws::Http::URI{Aws::String{endpoint_url.c_str()}};
        auto http_request = Aws::Http::Standard::StandardHttpRequest{
          uri, Aws::Http::HttpMethod::HTTP_POST};
        for (const auto& [name, value] : sdk_headers) {
          http_request.SetHeaderValue(name, value);
        }
        auto body_stream = std::make_shared<Aws::StringStream>(payload);
        http_request.AddContentBody(body_stream);
        http_request.SetHeaderValue(
          Aws::Http::CONTENT_LENGTH_HEADER,
          Aws::Utils::StringUtils::to_string(payload.size()));
        if (not signer.SignRequest(http_request)) {
          diagnostic::error("failed to sign SQS request").throw_();
        }
        auto headers = std::map<std::string, std::string>{};
        for (const auto& [name, value] : http_request.GetHeaders()) {
          headers[std::string{name.c_str(), name.size()}]
            = std::string{value.c_str(), value.size()};
        }
        auto body = std::string{payload.c_str(), payload.size()};
        auto response = co_await pool.post(std::move(body), std::move(headers));
        if (response.is_err()) {
          // Transport-level errors are retriable.
          throw sqs_retriable_error{std::move(response).unwrap_err()};
        }
        auto& resp = response.unwrap();
        if (resp.status_code < 200 or resp.status_code >= 300) {
          auto error_msg = extract_sqs_error_message(resp.body);
          if (is_retriable_status(resp.status_code)) {
            TENZIR_WARN("SQS API at {} returned HTTP {} (retrying): {}",
                        endpoint_url, resp.status_code, error_msg);
            throw sqs_retriable_error{resp.status_code, std::move(error_msg)};
          }
          diagnostic::error("SQS API returned HTTP {}", resp.status_code)
            .note("endpoint: {}", endpoint_url)
            .note("{}", error_msg)
            .throw_();
        }
        auto status_code
          = static_cast<Aws::Http::HttpResponseCode>(resp.status_code);
        auto json_body = Aws::Utils::Json::JsonValue{
          Aws::String{resp.body.c_str(), resp.body.size()}};
        auto aws_headers = Aws::Http::HeaderValueCollection{};
        for (const auto& [name, value] : resp.headers) {
          aws_headers[Aws::String{name.c_str(), name.size()}]
            = Aws::String{value.c_str(), value.size()};
        }
        co_return Aws::AmazonWebServiceResult<Aws::Utils::Json::JsonValue>{
          std::move(json_body), std::move(aws_headers), status_code};
      },
      [](const folly::exception_wrapper& ew) -> bool {
        return ew.is_compatible_with<sqs_retriable_error>();
      });
  } catch (sqs_retriable_error& e) {
    // All retries exhausted -- convert to a diagnostic.
    if (e.status_code != 0) {
      diagnostic::error("SQS API returned HTTP {} after {} retries",
                        e.status_code, retry_max_retries)
        .note("endpoint {}", endpoint_url)
        .note("{}", e.detail)
        .throw_();
    }
    diagnostic::error("SQS HTTP request failed after {} retries",
                      retry_max_retries)
      .note("endpoint {}", endpoint_url)
      .note("{}", e.detail)
      .throw_();
  }
}

} // namespace

// --- Free helpers ---

auto is_sqs_queue_url(std::string_view s) -> bool {
  return s.starts_with("http://") or s.starts_with("https://");
}

auto region_from_sqs_url(std::string_view url) -> Option<std::string> {
  auto scheme_end = url.find("://");
  if (scheme_end == std::string_view::npos) {
    return None{};
  }
  auto host_start = scheme_end + 3;
  auto host_end = url.find('/', host_start);
  auto host = url.substr(host_start, host_end - host_start);
  if (auto colon = host.find(':'); colon != std::string_view::npos) {
    host = host.substr(0, colon);
  }
  constexpr auto prefix = std::string_view{"sqs."};
  if (not host.starts_with(prefix)) {
    return None{};
  }
  host.remove_prefix(prefix.size());
  for (auto suffix : {std::string_view{".amazonaws.com.cn"},
                      std::string_view{".amazonaws.com"}}) {
    if (host.ends_with(suffix)) {
      auto region = host.substr(0, host.size() - suffix.size());
      if (not region.empty()) {
        return std::string{region};
      }
    }
  }
  return None{};
}

// --- AsyncSqsQueue ---

AsyncSqsQueue::AsyncSqsQueue(
  located<std::string> name, std::chrono::seconds poll_time, std::string region,
  Option<tenzir::resolved_aws_credentials> creds,
  folly::Executor::KeepAlive<folly::IOExecutor> io_executor)
  : name_{std::move(name)}, region_{std::move(region)} {
  // The credentials provider helper still uses `std::optional` at its API
  // boundary; convert at the call site rather than threading `Option` through.
  auto creds_std = creds ? std::optional{std::move(*creds)} : std::nullopt;
  auto credentials
    = tenzir::make_aws_credentials_provider(creds_std, std::optional{region_});
  if (not credentials) {
    diagnostic::error(credentials.error()).throw_();
  }
  credentials_ = std::move(*credentials);
  signer_ = std::make_unique<Aws::Client::AWSAuthV4Signer>(
    credentials_, "sqs", Aws::String{region_.c_str()});
  // When the user passes a full queue URL, use its origin as the SQS endpoint
  // and remember the URL so `init()` can skip the GetQueueUrl round-trip. For
  // queue names, fall back to the region-derived endpoint (and optional env
  // overrides), and let `init()` resolve the URL.
  if (is_sqs_queue_url(name_.inner)) {
    endpoint_url_ = url_origin(name_.inner);
    url_ = Aws::String{name_.inner.c_str(), name_.inner.size()};
  } else {
    endpoint_url_ = sqs_endpoint_url(region_);
  }
  auto timeout
    = std::chrono::milliseconds{poll_time} + std::chrono::seconds{10};
  auto tls = endpoint_url_.starts_with("https://");
  pool_
    = HttpPool::make(std::move(io_executor), endpoint_url_,
                     HttpPoolConfig{.tls = tls, .request_timeout = timeout});
}

auto AsyncSqsQueue::init() -> Task<void> {
  // Skip the URL lookup when the user supplied a full queue URL; the URL was
  // already populated in the constructor.
  if (not url_.empty()) {
    co_return;
  }
  auto request = Aws::SQS::Model::GetQueueUrlRequest{};
  request.SetQueueName(name_.inner);
  auto aws_result
    = co_await sqs_api_call(*pool_, *signer_, endpoint_url_, request);
  auto result = Aws::SQS::Model::GetQueueUrlResult{aws_result};
  url_ = result.GetQueueUrl();
}

auto AsyncSqsQueue::receive_messages(
  size_t num_messages, std::chrono::seconds poll_time,
  Option<std::chrono::seconds> visibility_timeout)
  -> Task<Aws::Vector<Aws::SQS::Model::Message>> {
  TENZIR_ASSERT(num_messages > 0);
  TENZIR_ASSERT(num_messages <= 10);
  auto request = Aws::SQS::Model::ReceiveMessageRequest{};
  request.SetQueueUrl(url_);
  request.SetMaxNumberOfMessages(detail::narrow_cast<int>(num_messages));
  request.SetWaitTimeSeconds(detail::narrow_cast<int>(poll_time.count()));
  if (visibility_timeout) {
    request.SetVisibilityTimeout(
      detail::narrow_cast<int>(visibility_timeout->count()));
  }
  // Request all system attributes so events can expose metadata such as
  // `sent_time`, `receive_count`, and `sender_id`.
  request.AddMessageSystemAttributeNames(
    Aws::SQS::Model::MessageSystemAttributeName::All);
  auto aws_result
    = co_await sqs_api_call(*pool_, *signer_, endpoint_url_, request);
  auto result = Aws::SQS::Model::ReceiveMessageResult{aws_result};
  co_return result.GetMessages();
}

auto AsyncSqsQueue::send_message(Aws::String data) -> Task<void> {
  auto request = Aws::SQS::Model::SendMessageRequest{};
  request.SetQueueUrl(url_);
  request.SetMessageBody(std::move(data));
  co_await sqs_api_call(*pool_, *signer_, endpoint_url_, request);
}

auto AsyncSqsQueue::delete_message(const Aws::SQS::Model::Message& message)
  -> Task<Option<diagnostic>> {
  auto request = Aws::SQS::Model::DeleteMessageRequest{};
  request.SetQueueUrl(url_);
  request.SetReceiptHandle(message.GetReceiptHandle());
  try {
    co_await sqs_api_call(*pool_, *signer_, endpoint_url_, request);
  } catch (diagnostic& d) {
    co_return diagnostic::warning("failed to delete message from SQS queue")
      .primary(name_.source)
      .note("URL: {}", url_)
      .note("message ID: {}", message.GetMessageId())
      .note("receipt handle: {}", message.GetReceiptHandle())
      .done();
  }
  co_return std::nullopt;
}

} // namespace tenzir::plugins::sqs
