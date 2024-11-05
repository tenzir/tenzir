//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
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

#include "operator.hpp"

using namespace std::chrono_literals;

namespace tenzir::plugins::sqs {

namespace {

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
