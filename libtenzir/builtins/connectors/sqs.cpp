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
#include <aws/sqs/model/GetQueueUrlRequest.h>
#include <aws/sqs/model/ReceiveMessageRequest.h>
#include <aws/sqs/model/SendMessageRequest.h>

namespace tenzir::plugins::sqs {

namespace {

struct connector_args {
  std::string queue;
  bool create;

  template <class Inspector>
  friend auto inspect(Inspector& f, connector_args& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.sqs.connector_args")
      .fields(f.field("queue", x.queue), f.field("create", x.create));
  }
};

class sqs_loader final : public plugin_loader {
public:
  sqs_loader() = default;

  explicit sqs_loader(connector_args args) : args_{std::move(args)} {
  }

  auto instantiate(operator_control_plane& ctrl) const
    -> std::optional<generator<chunk_ptr>> override {
    auto make = [&ctrl](connector_args args) mutable -> generator<chunk_ptr> {
      auto config = Aws::Client::ClientConfiguration{};
      auto client = Aws::SQS::SQSClient{config};
      if (args.create) {
        TENZIR_DEBUG("creating SQS queue: {}", args.queue);
        auto request = Aws::SQS::Model::CreateQueueRequest{};
        request.SetQueueName(args.queue);
        auto outcome = client.CreateQueue(request);
        if (not outcome.IsSuccess()) {
          diagnostic::error("failed to create SQS queue: {}", args.queue)
            .note("{}", outcome.GetError().GetMessage())
            .emit(ctrl.diagnostics());
          co_return;
        }
        TENZIR_DEBUG("successfully created SQS queue");
      }
      TENZIR_DEBUG("retrieving URL for queue");
      auto url = Aws::String{};
      {
        auto request = Aws::SQS::Model::GetQueueUrlRequest{};
        request.SetQueueName(args.queue);
        auto outcome = client.GetQueueUrl(request);
        if (not outcome.IsSuccess()) {
          diagnostic::error("failed to get URL for SQS queue: {}", args.queue)
            .note("{}", outcome.GetError().GetMessage())
            .emit(ctrl.diagnostics());
          co_return;
        }
        url = outcome.GetResult().GetQueueUrl();
        TENZIR_DEBUG("got URL for queue '{}': {}", args.queue, url);
      }
      TENZIR_DEBUG("receiving messages");
      auto request = Aws::SQS::Model::ReceiveMessageRequest{};
      request.SetQueueUrl(url);
      request.SetMaxNumberOfMessages(1);
      auto outcome = client.ReceiveMessage(request);
      if (not outcome.IsSuccess()) {
        diagnostic::error("failed receiving message from SQS queue: {}",
                          args.queue)
          .note("{}", outcome.GetError().GetMessage())
          .emit(ctrl.diagnostics());
        co_return;
      }
      for (const auto& message : outcome.GetResult().GetMessages()) {
        // If we need to make the message ID available downstream, we could copy
        // it into the metadata of chunk.
        TENZIR_DEBUG("got message with ID {} (handle: {})",
                     message.GetMessageId(), message.GetReceiptHandle());
        const auto& body = message.GetBody();
        auto str = std::string_view{body.data(), body.size()};
        co_yield chunk::copy(str);
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

class plugin final : public virtual loader_plugin<sqs_loader> {
public:
  auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader> override {
    auto parser = argument_parser{
      name(),
      fmt::format("https://docs.tenzir.com/docs/connectors/{}", name())};
    auto args = connector_args{};
    parser.add(args.queue, "<queue>");
    parser.add("--create", args.create, "create queue if it doesn't exist");
    parser.parse(p);
    if (args.queue.empty()) {
      diagnostic::error("queue must not be empty")
        .hint("provide a non-empty string as queue name")
        .throw_();
    }
    return std::make_unique<sqs_loader>(std::move(args));
  }

  auto name() const -> std::string override {
    return "sqs";
  }
};

} // namespace

} // namespace tenzir::plugins::sqs

TENZIR_REGISTER_PLUGIN(tenzir::plugins::sqs::plugin)
