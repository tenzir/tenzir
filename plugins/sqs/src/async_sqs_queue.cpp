//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "async_sqs_queue.hpp"

#include <tenzir/detail/narrow.hpp>
#include <tenzir/diagnostics.hpp>

#include <aws/sqs/model/DeleteMessageRequest.h>
#include <aws/sqs/model/GetQueueUrlRequest.h>
#include <aws/sqs/model/GetQueueUrlResult.h>
#include <aws/sqs/model/MessageSystemAttributeName.h>
#include <aws/sqs/model/ReceiveMessageRequest.h>
#include <aws/sqs/model/ReceiveMessageResult.h>
#include <aws/sqs/model/SendMessageRequest.h>
#include <aws/sqs/model/SendMessageResult.h>

#include <algorithm>

namespace tenzir::plugins::sqs {

namespace {

template <class Request>
auto sqs_api_call(amazon::SignedHttpClient& client, Request& request)
  -> Task<Aws::AmazonWebServiceResult<Aws::Utils::Json::JsonValue>> {
  auto response = co_await client.api_call("SQS API", request);
  if (response.is_err()) {
    diagnostic::error("{}", std::move(response).unwrap_err()).throw_();
  }
  co_return amazon::to_aws_json_result(std::move(response).unwrap());
}

} // namespace

// --- Free helpers ---

auto is_sqs_queue_url(std::string_view s) -> bool {
  return s.starts_with("http://") or s.starts_with("https://");
}

auto is_valid_sqs_queue_name(std::string_view s) -> bool {
  // AWS rules: up to 80 characters; alphanumeric, `-`, and `_`; FIFO queues
  // additionally end with `.fifo` (the suffix counts toward the 80 chars).
  // See:
  // https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_CreateQueue.html#API_CreateQueue_RequestParameters
  if (s.empty() or s.size() > 80) {
    return false;
  }
  auto base = s;
  if (base.ends_with(".fifo")) {
    base.remove_suffix(5);
  }
  if (base.empty()) {
    return false;
  }
  return std::ranges::all_of(base, [](char c) {
    return (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z')
           or (c >= '0' and c <= '9') or c == '-' or c == '_';
  });
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
  // When the user passes a full queue URL, use its origin as the SQS endpoint
  // and remember the URL so `init()` can skip the GetQueueUrl round-trip. For
  // queue names, fall back to the region-derived endpoint (and optional env
  // overrides), and let `init()` resolve the URL.
  auto endpoint = std::string{};
  if (is_sqs_queue_url(name_.inner)) {
    endpoint = amazon::url_origin(name_.inner);
    url_ = Aws::String{name_.inner.c_str(), name_.inner.size()};
  } else {
    endpoint = amazon::service_endpoint_url("sqs", region_, "SQS");
  }
  auto timeout
    = std::chrono::milliseconds{poll_time} + std::chrono::seconds{10};
  client_ = Box<amazon::SignedHttpClient>{
    std::in_place, amazon::SignedHttpClientConfig{
                     .service = "sqs",
                     .region = region_,
                     .endpoint = std::move(endpoint),
                     .credentials = std::move(creds),
                     .io_executor = std::move(io_executor),
                     .request_timeout = timeout,
                     .retry_delay = std::chrono::milliseconds{25},
                   }};
}

auto AsyncSqsQueue::init() -> Task<void> {
  // Skip the URL lookup when the user supplied a full queue URL; the URL was
  // already populated in the constructor.
  if (not url_.empty()) {
    co_return;
  }
  auto request = Aws::SQS::Model::GetQueueUrlRequest{};
  request.SetQueueName(name_.inner);
  auto aws_result = co_await sqs_api_call(*client_, request);
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
  auto aws_result = co_await sqs_api_call(*client_, request);
  auto result = Aws::SQS::Model::ReceiveMessageResult{aws_result};
  co_return result.GetMessages();
}

auto AsyncSqsQueue::send_message(Aws::String data) -> Task<void> {
  auto request = Aws::SQS::Model::SendMessageRequest{};
  request.SetQueueUrl(url_);
  request.SetMessageBody(std::move(data));
  co_await sqs_api_call(*client_, request);
}

auto AsyncSqsQueue::delete_message(const Aws::SQS::Model::Message& message)
  -> Task<Option<diagnostic>> {
  auto request = Aws::SQS::Model::DeleteMessageRequest{};
  request.SetQueueUrl(url_);
  request.SetReceiptHandle(message.GetReceiptHandle());
  try {
    co_await sqs_api_call(*client_, request);
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
