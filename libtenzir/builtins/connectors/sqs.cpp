//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
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

auto to_aws_string(chunk_ptr chunk) -> Aws::String {
  const auto* ptr = reinterpret_cast<Aws::String::const_pointer>(chunk->data());
  auto size = chunk->size();
  return {ptr, size};
}

/// A wrapper around SQS.
class sqs_queue {
public:
  explicit sqs_queue(located<std::string> name,
                     std::optional<std::chrono::seconds> poll_time = {})
    : name_{std::move(name)}, poll_time_{poll_time} {
    if (poll_time_) {
      auto config = Aws::Client::ClientConfiguration{};
      // The long-poll timeout is a lower bound for other AWS client timeouts.
      auto poll_time_ms = long{poll_time_->count()} * 1'000;
      config.requestTimeoutMs = std::max(config.requestTimeoutMs, poll_time_ms);
      config.httpRequestTimeoutMs
        = std::max(config.httpRequestTimeoutMs, poll_time_ms);
      // Recreate the client, as the config is copied upon construction.
      client_ = {config};
    }
    // Do the equivalent of `aws sqs list-queues`.
    url_ = queue_url();
  }

  /// Receives messages from the queue.
  auto receive_messages() {
    TENZIR_DEBUG("receiving messages from {}", url_);
    auto request = Aws::SQS::Model::ReceiveMessageRequest{};
    request.SetQueueUrl(url_);
    request.SetMaxNumberOfMessages(10);
    if (poll_time_) {
      request.SetWaitTimeSeconds(detail::narrow_cast<int>(poll_time_->count()));
    }
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
        .throw_();
    }
    return outcome.GetResult().GetQueueUrl();
  }

  located<std::string> name_;
  Aws::String url_;
  Aws::SQS::SQSClient client_;
  std::optional<std::chrono::seconds> poll_time_;
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
          = args.poll_time
              ? std::optional<std::chrono::seconds>{args.poll_time->inner}
              : std::optional<std::chrono::seconds>{std::nullopt};
        auto queue = sqs_queue{args.queue, poll_time};
        while (true) {
          auto messages = queue.receive_messages();
          for (const auto& message : messages) {
            TENZIR_DEBUG("got message {} ({})", message.GetMessageId(),
                         message.GetReceiptHandle());
            // It seems there's no way to get the Aws::String out of the message
            // to move it into the chunk. So we have to copy it.
            const auto& body = message.GetBody();
            auto str = std::string_view{body.data(), body.size()};
            co_yield chunk::copy(str);
            queue.delete_message(message);
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
    auto queue = sqs_queue{args_.queue};
    return [&ctrl, queue = std::make_shared<sqs_queue>(std::move(queue))](
             chunk_ptr chunk) mutable {
      if (!chunk || chunk->size() == 0)
        return;
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

class plugin final : public virtual loader_plugin<sqs_loader>,
                     public virtual saver_plugin<sqs_saver> {
public:
  auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader> override {
    auto args = parse_args(p);
    return std::make_unique<sqs_loader>(std::move(args));
  }

  auto parse_saver(parser_interface& p) const
    -> std::unique_ptr<plugin_saver> override {
    auto args = parse_args(p);
    return std::make_unique<sqs_saver>(std::move(args));
  }

  auto name() const -> std::string override {
    return "sqs";
  }

private:
  static auto parse_args(parser_interface& p) -> connector_args {
    auto parser
      = argument_parser{"sqs", "https://docs.tenzir.com/connectors/sqs"};
    auto result = connector_args{};
    parser.add(result.queue, "<queue>");
    parser.add("--poll-time", result.poll_time, "<duration>");
    parser.parse(p);
    if (result.queue.inner.empty()) {
      diagnostic::error("queue must not be empty")
        .primary(result.queue.source)
        .hint("provide a non-empty string as queue name")
        .throw_();
    }
    if (result.poll_time) {
      if (result.poll_time->inner < 1s || result.poll_time->inner > 20s) {
        diagnostic::error("invalid poll time: {}", result.poll_time->inner)
          .primary(result.poll_time->source)
          .hint("poll time must be in the interval [1s, 20s]")
          .throw_();
      }
    }
    return result;
  }
};

} // namespace

} // namespace tenzir::plugins::sqs

TENZIR_REGISTER_PLUGIN(tenzir::plugins::sqs::plugin)
