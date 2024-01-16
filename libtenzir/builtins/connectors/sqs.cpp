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
  sqs_queue(std::string name) : name_{std::move(name)}, client_{config_} {
  }

  /// Creates the queue.
  auto create(std::optional<std::chrono::seconds> long_poll_time = {}) -> void {
    TENZIR_DEBUG("creating SQS queue: {}", name_);
    auto request = Aws::SQS::Model::CreateQueueRequest{};
    request.SetQueueName(std::string{name_});
    if (long_poll_time) {
      request.AddAttributes(
        Aws::SQS::Model::QueueAttributeName::ReceiveMessageWaitTimeSeconds,
        std::to_string(long_poll_time->count()));
    }
    auto outcome = client_.CreateQueue(request);
    if (not outcome.IsSuccess()) {
      return diagnostic::error("failed to create SQS queue")
        .note("queue: {}", name_)
        .note("{}", outcome.GetError().GetMessage())
        .throw_();
    }
    TENZIR_DEBUG("successfully created SQS queue");
  }

  /// Receives messages from the queue.
  auto receive_messages(std::optional<std::chrono::seconds> wait_time = {}) {
    TENZIR_DEBUG("receiving messages from {}", url_);
    if (url_.empty()) {
      retrieve_url();
    }
    auto request = Aws::SQS::Model::ReceiveMessageRequest{};
    request.SetQueueUrl(url_);
    request.SetMaxNumberOfMessages(10);
    if (wait_time) {
      request.SetWaitTimeSeconds(detail::narrow_cast<int>(wait_time->count()));
    }
    auto outcome = client_.ReceiveMessage(request);
    if (not outcome.IsSuccess()) {
      diagnostic::error("failed receiving message from SQS queue")
        .note("queue: {}", name_)
        .note("URL: {}", url_)
        .note("{}", outcome.GetError().GetMessage())
        .throw_();
    }
    return outcome.GetResult().GetMessages();
  }

  auto send_message(Aws::String data) {
    auto request = Aws::SQS::Model::SendMessageRequest{};
    request.SetQueueUrl(url_);
    request.SetMessageBody(std::move(data));
    auto outcome = client_.SendMessage(request);
    if (not outcome.IsSuccess()) {
      diagnostic::error("failed sending message to SQS queue")
        .note("queue: {}", name_)
        .note("URL: {}", url_)
        .note("{}", outcome.GetError().GetMessage())
        .throw_();
    }
  }

  /// Deletes a message from the queue.
  auto delete_message(const auto& message) -> std::optional<diagnostic> {
    TENZIR_DEBUG("deleting message {}", message.GetMessageId());
    if (url_.empty()) {
      retrieve_url();
    }
    auto request = Aws::SQS::Model::DeleteMessageRequest{};
    request.SetQueueUrl(url_);
    request.SetReceiptHandle(message.GetReceiptHandle());
    auto outcome = client_.DeleteMessage(request);
    if (not outcome.IsSuccess()) {
      return diagnostic::warning("failed to delete message from SQS queue")
        .note("queue: {}", name_)
        .note("URL: {}", url_)
        .note("message ID: {}", message.GetMessageId())
        .note("receipt handle: {}", message.GetReceiptHandle())
        .done();
    }
    return std::nullopt;
  }

private:
  auto retrieve_url() -> void {
    TENZIR_DEBUG("retrieving URL for queue {}", name_);
    auto request = Aws::SQS::Model::GetQueueUrlRequest{};
    request.SetQueueName(name_);
    auto outcome = client_.GetQueueUrl(request);
    if (not outcome.IsSuccess()) {
      diagnostic::error("failed to get URL for SQS queue")
        .note("queue: {}", name_)
        .note("{}", outcome.GetError().GetMessage())
        .throw_();
      url_ = outcome.GetResult().GetQueueUrl();
      TENZIR_DEBUG("got URL for queue '{}': {}", name_, url_);
    }
  }

  std::string name_;
  Aws::String url_;
  Aws::Client::ClientConfiguration config_;
  Aws::SQS::SQSClient client_;
};

struct connector_args {
  std::string queue;
  bool create_queue;
  bool delete_message;
  std::optional<std::chrono::seconds> poll_time;

  template <class Inspector>
  friend auto inspect(Inspector& f, connector_args& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.sqs.connector_args")
      .fields(f.field("queue", x.queue),
              f.field("create_queue", x.create_queue),
              f.field("delete_message", x.delete_message),
              f.field("poll_time", x.poll_time));
  }
};

struct loader_args : connector_args {
  bool delete_message;

  template <class Inspector>
  friend auto inspect(Inspector& f, loader_args& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.sqs.loader_args")
      .fields(f.field("queue", x.queue),
              f.field("create_queue", x.create_queue),
              f.field("delete_message", x.delete_message),
              f.field("poll_time", x.poll_time));
  }
};

struct saver_args : connector_args {
  template <class Inspector>
  friend auto inspect(Inspector& f, saver_args& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.sqs.saver_args")
      .fields(f.field("queue", x.queue),
              f.field("create_queue", x.create_queue),
              f.field("delete_message", x.delete_message),
              f.field("poll_time", x.poll_time));
  }
};

class sqs_loader final : public plugin_loader {
public:
  sqs_loader() = default;

  explicit sqs_loader(loader_args args) : args_{std::move(args)} {
  }

  auto instantiate(operator_control_plane& ctrl) const
    -> std::optional<generator<chunk_ptr>> override {
    auto make = [](loader_args args) mutable -> generator<chunk_ptr> {
      auto queue = sqs_queue{args.queue};
      if (args.create_queue) {
        queue.create(args.poll_time);
      }
      auto messages = queue.receive_messages(args.poll_time);
      for (const auto& message : messages) {
        // If we need to make the message ID available downstream, we could copy
        // it into the metadata of chunk.
        TENZIR_DEBUG("got message {} ({})", message.GetMessageId(),
                     message.GetReceiptHandle());
        const auto& body = message.GetBody();
        auto str = std::string_view{body.data(), body.size()};
        co_yield chunk::copy(str);
        if (args.delete_message) {
          queue.delete_message(message);
        }
      }
    };
    return make(args_);
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
  loader_args args_;
};

class sqs_saver final : public plugin_saver {
public:
  sqs_saver() = default;

  sqs_saver(connector_args args) : args_{std::move(args)} {
  }

  auto instantiate(operator_control_plane& ctrl, std::optional<printer_info>)
    -> caf::expected<std::function<void(chunk_ptr)>> override {
    auto queue = sqs_queue{args_.queue};
    if (args_.create_queue) {
      queue.create(args_.poll_time);
    }
    return [queue = std::make_shared<sqs_queue>(std::move(queue))](
             chunk_ptr chunk) mutable {
      if (!chunk || chunk->size() == 0)
        return;
      queue->send_message(to_aws_string(std::move(chunk)));
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
  saver_args args_;
};

class plugin final : public virtual loader_plugin<sqs_loader>,
                     public virtual saver_plugin<sqs_saver> {
public:
  auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader> override {
    auto args = parse_args<loader_args>(p);
    return std::make_unique<sqs_loader>(std::move(args));
  }

  auto parse_saver(parser_interface& p) const
    -> std::unique_ptr<plugin_saver> override {
    auto args = parse_args<saver_args>(p);
    return std::make_unique<sqs_saver>(std::move(args));
  }

  auto name() const -> std::string override {
    return "sqs";
  }

private:
  template <class Args>
  static auto parse_args(parser_interface& p) -> Args {
    auto parser
      = argument_parser{"sqs", "https://docs.tenzir.com/connectors/sqs"};
    auto result = Args{};
    auto queue = located<std::string>{};
    auto poll_time = std::optional<located<std::chrono::seconds>>{};
    parser.add(queue, "<queue>");
    parser.add("--create", result.create_queue);
    parser.add("--poll-time", poll_time, "<duration>");
    if constexpr (std::is_same_v<Args, loader_args>) {
      parser.add("--delete", result.delete_message);
    }
    parser.parse(p);
    if (queue.inner.empty()) {
      diagnostic::error("queue must not be empty")
        .primary(queue.source)
        .hint("provide a non-empty string as queue name")
        .throw_();
    } else {
      result.queue = std::move(queue.inner);
    }
    if (poll_time) {
      if (poll_time->inner < 1s || poll_time->inner > 20s) {
        diagnostic::error("invalid poll time: {}", poll_time->inner)
          .primary(poll_time->source)
          .hint("poll time must be in the interval [1s, 20s]")
          .throw_();
      }
      result.poll_time = poll_time->inner;
    }
    return result;
  }
};

} // namespace

} // namespace tenzir::plugins::sqs

TENZIR_REGISTER_PLUGIN(tenzir::plugins::sqs::plugin)
