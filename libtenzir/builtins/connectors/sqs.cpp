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
  sqs_queue(std::string name) : client_{config_} {
  }

  /// Creates the queue.
  auto create() -> void {
    TENZIR_DEBUG("creating SQS queue: {}", name_);
    auto request = Aws::SQS::Model::CreateQueueRequest{};
    request.SetQueueName(std::string{name_});
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
  auto receive_messages(int n = 10) {
    TENZIR_DEBUG("receiving messages from {}", url_);
    if (url_.empty()) {
      retrieve_url();
    }
    auto request = Aws::SQS::Model::ReceiveMessageRequest{};
    request.SetQueueUrl(url_);
    request.SetMaxNumberOfMessages(n);
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

  template <class Inspector>
  friend auto inspect(Inspector& f, connector_args& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.sqs.connector_args")
      .fields(f.field("queue", x.queue),
              f.field("create_queue", x.create_queue),
              f.field("delete_message", x.delete_message));
  }
};

class sqs_loader final : public plugin_loader {
public:
  sqs_loader() = default;

  explicit sqs_loader(connector_args args) : args_{std::move(args)} {
  }

  auto instantiate(operator_control_plane&) const
    -> std::optional<generator<chunk_ptr>> override {
    auto make = [](connector_args args) mutable -> generator<chunk_ptr> {
      auto sqs = sqs_queue{args.queue};
      if (args.create_queue) {
        sqs.create();
      }
      auto messages = sqs.receive_messages();
      for (const auto& message : messages) {
        // If we need to make the message ID available downstream, we could copy
        // it into the metadata of chunk.
        TENZIR_DEBUG("got message {} ({})", message.GetMessageId(),
                     message.GetReceiptHandle());
        const auto& body = message.GetBody();
        auto str = std::string_view{body.data(), body.size()};
        co_yield chunk::copy(str);
        if (args.delete_message) {
          sqs.delete_message(message);
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
  connector_args args_;
};

class sqs_saver final : public plugin_saver {
public:
  sqs_saver() = default;

  sqs_saver(connector_args args) : args_{std::move(args)} {
  }

  auto instantiate(operator_control_plane& ctrl, std::optional<printer_info>)
    -> caf::expected<std::function<void(chunk_ptr)>> override {
    auto sqs = sqs_queue{args_.queue};
    if (args_.create_queue) {
      sqs.create();
    }
    return [sqs = std::make_shared<sqs_queue>(std::move(sqs))](
             chunk_ptr chunk) mutable {
      if (!chunk || chunk->size() == 0)
        return;
      sqs->send_message(to_aws_string(std::move(chunk)));
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
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/connectors/{}", name())};
    auto args = connector_args{};
    parser.add(args.queue, "<queue>");
    parser.add("--create", args.create_queue,
               "create queue if it doesn't exist");
    parser.add("--delete", args.delete_message,
               "delete message after reception");
    parser.parse(p);
    if (args.queue.empty()) {
      diagnostic::error("queue must not be empty")
        .hint("provide a non-empty string as queue name")
        .throw_();
    }
    return std::make_unique<sqs_loader>(std::move(args));
  }

  auto parse_saver(parser_interface& p) const
    -> std::unique_ptr<plugin_saver> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/connectors/{}", name())};
    auto args = connector_args{};
    parser.add(args.queue, "<queue>");
    parser.add("--create", args.create_queue,
               "create queue if it doesn't exist");
    parser.parse(p);
    if (args.queue.empty()) {
      diagnostic::error("queue must not be empty")
        .hint("provide a non-empty string as queue name")
        .throw_();
    }
    return std::make_unique<sqs_saver>(std::move(args));
  }

  auto name() const -> std::string override {
    return "sqs";
  }
};

} // namespace

} // namespace tenzir::plugins::sqs

TENZIR_REGISTER_PLUGIN(tenzir::plugins::sqs::plugin)
