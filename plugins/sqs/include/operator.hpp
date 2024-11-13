//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/detail/env.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>

#include <aws/core/Aws.h>
#include <aws/core/utils/Outcome.h>
#include <aws/sqs/SQSClient.h>
#include <aws/sqs/model/CreateQueueRequest.h>
#include <aws/sqs/model/DeleteMessageRequest.h>
#include <aws/sqs/model/GetQueueUrlRequest.h>
#include <aws/sqs/model/ReceiveMessageRequest.h>
#include <aws/sqs/model/SendMessageRequest.h>

using namespace std::chrono_literals;

namespace tenzir::plugins::sqs {

namespace {

/// The default poll time.
static constexpr auto default_poll_time = 3s;
static_assert(default_poll_time >= 1s && default_poll_time <= 20s);

auto to_aws_string(chunk_ptr chunk) -> Aws::String {
  const auto* ptr = reinterpret_cast<Aws::String::const_pointer>(chunk->data());
  auto size = chunk->size();
  return {ptr, size};
}

/// A wrapper around SQS.
class sqs_queue {
public:
  explicit sqs_queue(located<std::string> name, std::chrono::seconds poll_time)
    : name_{std::move(name)} {
    auto config = Aws::Client::ClientConfiguration{};
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
    // Recreate the client, as the config is copied upon construction.
    client_ = {config};
    // Do the equivalent of `aws sqs list-queues`.
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

  /// Deletes a message to the queue.
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
      diagnostic::error("failed to get URL for SQS queue")
        .primary(name_.source)
        .note("{}", outcome.GetError().GetMessage())
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

  template <class Inspector>
  friend auto inspect(Inspector& f, connector_args& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.sqs.connector_args")
      .fields(f.field("queue", x.queue), f.field("poll_time", x.poll_time));
  }
};

class sqs_loader final : public plugin_loader {
public:
  sqs_loader() = default;

  explicit sqs_loader(connector_args args) : args_{std::move(args)} {
  }

  auto instantiate(operator_control_plane& ctrl) const
    -> std::optional<generator<chunk_ptr>> override {
    auto make = [](operator_control_plane& ctrl,
                   connector_args args) mutable -> generator<chunk_ptr> {
      try {
        auto poll_time
          = args.poll_time ? args.poll_time->inner : default_poll_time;
        auto queue = sqs_queue{args.queue, poll_time};
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
        ctrl.diagnostics().emit(std::move(d));
      }
    };
    return make(ctrl, args_);
  }

  auto name() const -> std::string override {
    return "sqs";
  }

  auto default_parser() const -> std::string override {
    return "json";
  }

  friend auto inspect(auto& f, sqs_loader& x) -> bool {
    return f.object(x)
      .pretty_name("sqs_loader")
      .fields(f.field("args", x.args_));
  }

private:
  connector_args args_;
};

class sqs_saver final : public plugin_saver {
public:
  sqs_saver() = default;

  sqs_saver(connector_args args) : args_{std::move(args)} {
  }

  auto instantiate(operator_control_plane& ctrl, std::optional<printer_info>)
    -> caf::expected<std::function<void(chunk_ptr)>> override {
    auto poll_time
      = args_.poll_time ? args_.poll_time->inner : default_poll_time;
    auto queue = std::shared_ptr<sqs_queue>{};
    try {
      queue = std::make_shared<sqs_queue>(args_.queue, poll_time);
    } catch (const diagnostic& d) {
      return d.to_error();
    }
    return [&ctrl, queue = std::move(queue)](chunk_ptr chunk) mutable {
      if (!chunk || chunk->size() == 0) {
        return;
      }
      try {
        queue->send_message(to_aws_string(std::move(chunk)));
      } catch (diagnostic& d) {
        ctrl.diagnostics().emit(std::move(d));
      }
      return;
    };
  }

  auto name() const -> std::string override {
    return "sqs";
  }

  auto default_printer() const -> std::string override {
    return "json";
  }

  auto is_joining() const -> bool override {
    return true;
  }

  friend auto inspect(auto& f, sqs_saver& x) -> bool {
    return f.object(x).pretty_name("sqs_saver").fields(f.field("args", x.args_));
  }

private:
  connector_args args_;
};
} // namespace
} // namespace tenzir::plugins::sqs
