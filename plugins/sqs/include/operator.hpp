//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/aws_credentials.hpp>
#include <tenzir/aws_iam.hpp>
#include <tenzir/detail/env.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/secret.hpp>
#include <tenzir/secret_resolution.hpp>
#include <tenzir/series_builder.hpp>

#include <aws/core/Aws.h>
#include <aws/core/utils/Outcome.h>
#include <aws/sqs/SQSClient.h>
#include <aws/sqs/model/CreateQueueRequest.h>
#include <aws/sqs/model/DeleteMessageRequest.h>
#include <aws/sqs/model/GetQueueUrlRequest.h>
#include <aws/sqs/model/ReceiveMessageRequest.h>
#include <aws/sqs/model/SendMessageRequest.h>

#include <string_view>

using namespace std::chrono_literals;

namespace tenzir::plugins::sqs {

namespace {

/// The default poll time.
constexpr auto default_poll_time = 3s;
static_assert(default_poll_time >= 1s && default_poll_time <= 20s);

auto to_aws_string(chunk_ptr chunk) -> Aws::String {
  const auto* ptr = reinterpret_cast<Aws::String::const_pointer>(chunk->data());
  auto size = chunk->size();
  return {ptr, size};
}

/// A wrapper around SQS.
class sqs_queue {
public:
  explicit sqs_queue(located<std::string> name, std::chrono::seconds poll_time,
                     std::optional<std::string> region,
                     std::optional<tenzir::resolved_aws_credentials> creds
                     = std::nullopt)
    : name_{std::move(name)} {
    auto config = Aws::Client::ClientConfiguration{};
    // Set the region if provided.
    if (region) {
      config.region = *region;
      TENZIR_VERBOSE("[sqs] using region {}", *region);
    }
    // TODO: remove this after upgrading to Arrow 15, as it's no longer
    // necessary. This is just a bandaid fix to make an old version of the SDK
    // honer the AWS_ENDPOINT_URL variable.
    if (auto endpoint_url = detail::getenv("AWS_ENDPOINT_URL")) {
      config.endpointOverride = *endpoint_url;
    }
    if (auto endpoint_url = detail::getenv("AWS_ENDPOINT_URL_SQS")) {
      config.endpointOverride = *endpoint_url;
    }
    // Proxy settings should be honored.
    // This is documented as "Off by default for legacy reasons" at
    // https://sdk.amazonaws.com/cpp/api/LATEST/aws-cpp-sdk-core/html/struct_aws_1_1_client_1_1_client_configuration.html#a0197eb33dffeb845f98d14e5058921c1
    config.allowSystemProxy = true;
    // The HTTP request timeout should be longer than the poll time. The overall
    // request timeout, including retries, should be even larger.
    auto poll_time_ms = std::chrono::milliseconds{poll_time};
    auto extra_time_for_http_request = 2s;
    auto extra_time_for_retries_and_backoff = 2s;
    auto http_request_timeout = poll_time_ms + extra_time_for_http_request;
    auto request_timeout
      = http_request_timeout + extra_time_for_retries_and_backoff;
    config.httpRequestTimeoutMs = long{http_request_timeout.count()};
    config.requestTimeoutMs = long{request_timeout.count()};
    // Create the credentials provider using the shared helper.
    auto credentials = tenzir::make_aws_credentials_provider(creds, region);
    if (not credentials) {
      diagnostic::error(credentials.error()).throw_();
    }
    // Create the client with the configuration and credentials provider.
    client_ = Aws::SQS::SQSClient{*credentials, nullptr, config};
    // Get the queue URL.
    url_ = queue_url();
  }

  /// Receives N messages from the queue.
  auto receive_messages(size_t num_messages, std::chrono::seconds poll_time) {
    TENZIR_ASSERT(num_messages > 0);
    TENZIR_ASSERT(num_messages <= 10);
    TENZIR_DEBUG("receiving {} messages from {}", num_messages, url_);
    auto request = Aws::SQS::Model::ReceiveMessageRequest{};
    request.SetQueueUrl(url_);
    // TODO: adjust once we have limit pushdown. We still can lose messages
    // because we eagerly fetch them without waiting for ACKs from downstream.
    request.SetMaxNumberOfMessages(detail::narrow_cast<int>(num_messages));
    request.SetWaitTimeSeconds(detail::narrow_cast<int>(poll_time.count()));
    auto outcome = client_.ReceiveMessage(request);
    if (not outcome.IsSuccess()) {
      diagnostic::error("failed receiving message from SQS queue")
        .primary(name_.source)
        .note("URL: {}", url_)
        .note("{}", outcome.GetError().GetMessage())
        .throw_();
    }
    return outcome.GetResult().GetMessages();
  }

  /// Sends a message to the queue.
  auto send_message(Aws::String data) -> void {
    TENZIR_DEBUG("sending {}-byte message to SQS queue '{}'", data.size(),
                 name_.inner);
    auto request = Aws::SQS::Model::SendMessageRequest{};
    request.SetQueueUrl(url_);
    request.SetMessageBody(std::move(data));
    auto outcome = client_.SendMessage(request);
    if (not outcome.IsSuccess()) {
      diagnostic::error("failed to send message to SQS queue")
        .primary(name_.source)
        .note("URL: {}", url_)
        .note("{}", outcome.GetError().GetMessage())
        .throw_();
    }
  }

  /// Deletes a message from the queue.
  auto delete_message(const auto& message) -> std::optional<diagnostic> {
    TENZIR_DEBUG("deleting message {}", message.GetMessageId());
    auto request = Aws::SQS::Model::DeleteMessageRequest{};
    request.SetQueueUrl(url_);
    request.SetReceiptHandle(message.GetReceiptHandle());
    auto outcome = client_.DeleteMessage(request);
    if (not outcome.IsSuccess()) {
      return diagnostic::warning("failed to delete message from SQS queue")
        .primary(name_.source)
        .note("URL: {}", url_)
        .note("message ID: {}", message.GetMessageId())
        .note("receipt handle: {}", message.GetReceiptHandle())
        .done();
    }
    return std::nullopt;
  }

private:
  auto queue_url() -> Aws::String {
    TENZIR_DEBUG("retrieving URL for queue {}", name_.inner);
    auto request = Aws::SQS::Model::GetQueueUrlRequest{};
    request.SetQueueName(name_.inner);
    auto outcome = client_.GetQueueUrl(request);
    if (not outcome.IsSuccess()) {
      const auto& err = outcome.GetError();
      diagnostic::error("failed to get URL for SQS queue")
        .primary(name_.source)
        .note("{}", err.GetMessage())
        .note("error code: {}", err.GetExceptionName())
        .hint("ensure that $AWS_ENDPOINT_URL points to valid endpoint")
        .throw_();
    }
    return outcome.GetResult().GetQueueUrl();
  }

  located<std::string> name_;
  Aws::String url_;
  Aws::SQS::SQSClient client_;
};

struct connector_args {
  located<std::string> queue;
  std::optional<located<std::chrono::seconds>> poll_time;
  std::optional<located<std::string>> aws_region;
  std::optional<tenzir::aws_iam_options> aws;

  template <class Inspector>
  friend auto inspect(Inspector& f, connector_args& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.sqs.connector_args")
      .fields(f.field("queue", x.queue), f.field("poll_time", x.poll_time),
              f.field("aws_region", x.aws_region), f.field("aws", x.aws));
  }
};

class sqs_loader final : public crtp_operator<sqs_loader> {
public:
  sqs_loader() = default;

  explicit sqs_loader(connector_args args) : args_{std::move(args)} {
  }

  auto operator()(operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    auto& dh = ctrl.diagnostics();
    // Resolve all secrets from aws_iam configuration.
    auto resolved_creds = std::optional<tenzir::resolved_aws_credentials>{};
    if (args_.aws) {
      resolved_creds.emplace();
      auto requests = args_.aws->make_secret_requests(*resolved_creds, dh);
      co_yield ctrl.resolve_secrets_must_yield(std::move(requests));
    }
    try {
      auto poll_time
        = args_.poll_time ? args_.poll_time->inner : default_poll_time;
      // Use top-level aws_region if provided, otherwise fall back to aws_iam
      auto region = args_.aws_region
                      ? std::optional{args_.aws_region->inner}
                      : (resolved_creds and not resolved_creds->region.empty()
                           ? std::optional{resolved_creds->region}
                           : std::nullopt);
      auto queue = sqs_queue{args_.queue, poll_time, region, resolved_creds};
      co_yield {};
      while (true) {
        constexpr auto num_messages = size_t{1};
        auto messages = queue.receive_messages(num_messages, poll_time);
        if (messages.empty()) {
          co_yield {};
        } else {
          for (const auto& message : messages) {
            TENZIR_DEBUG("got message {} ({})", message.GetMessageId(),
                         message.GetReceiptHandle());
            // It seems there's no way to get the Aws::String out of the
            // message to move it into the chunk. So we have to copy it.
            const auto& body = message.GetBody();
            auto str = std::string_view{body.data(), body.size()};
            co_yield chunk::copy(str);
            queue.delete_message(message);
          }
        }
      }
    } catch (diagnostic& d) {
      dh.emit(std::move(d));
    }
  }

  auto detached() const -> bool override {
    return true;
  }

  auto optimize(const expression&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  auto name() const -> std::string override {
    return "load_sqs";
  }

  friend auto inspect(auto& f, sqs_loader& x) -> bool {
    return f.object(x)
      .pretty_name("sqs_loader")
      .fields(f.field("args", x.args_));
  }

private:
  connector_args args_;
};

class sqs_saver final : public crtp_operator<sqs_saver> {
public:
  sqs_saver() = default;

  sqs_saver(connector_args args) : args_{std::move(args)} {
  }

  auto
  operator()(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    auto& dh = ctrl.diagnostics();
    // Resolve all secrets from aws_iam configuration.
    auto resolved_creds = std::optional<tenzir::resolved_aws_credentials>{};
    if (args_.aws) {
      resolved_creds.emplace();
      auto requests = args_.aws->make_secret_requests(*resolved_creds, dh);
      co_yield ctrl.resolve_secrets_must_yield(std::move(requests));
    }
    auto poll_time
      = args_.poll_time ? args_.poll_time->inner : default_poll_time;
    auto queue = std::shared_ptr<sqs_queue>{};
    try {
      // Use top-level aws_region if provided, otherwise fall back to aws_iam
      auto region = args_.aws_region
                      ? std::optional{args_.aws_region->inner}
                      : (resolved_creds and not resolved_creds->region.empty()
                           ? std::optional{resolved_creds->region}
                           : std::nullopt);
      queue = std::make_shared<sqs_queue>(args_.queue, poll_time, region,
                                          resolved_creds);
    } catch (diagnostic& d) {
      dh.emit(std::move(d));
    }
    for (auto chunk : input) {
      if (not chunk || chunk->size() == 0) {
        co_yield {};
        continue;
      }
      try {
        queue->send_message(to_aws_string(std::move(chunk)));
      } catch (diagnostic& d) {
        dh.emit(std::move(d));
      }
    }
  }

  auto detached() const -> bool override {
    return true;
  }

  auto optimize(const expression&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  auto name() const -> std::string override {
    return "save_sqs";
  }

  friend auto inspect(auto& f, sqs_saver& x) -> bool {
    return f.object(x).pretty_name("sqs_saver").fields(f.field("args", x.args_));
  }

private:
  connector_args args_;
};
} // namespace
} // namespace tenzir::plugins::sqs
