//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/plugin.hpp"

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

template <class Operator>
class plugin final : public virtual operator_plugin2<Operator> {
public:
  auto make(operator_factory_plugin::invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = connector_args{};
    auto dur = std::optional<located<duration>>{};
    auto iam_opts = std::optional<located<record>>{};
    TRY(argument_parser2::operator_(this->name())
          .positional("queue", args.queue)
          .named("poll_time", dur)
          .named("aws_iam", iam_opts)
          .parse(inv, ctx));
    if (iam_opts) {
      TRY(args.aws,
          aws_iam_options::from_record(std::move(iam_opts).value(), ctx));
    }
    if (args.queue.inner.empty()) {
      diagnostic::error("queue must not be empty")
        .primary(args.queue.source)
        .hint("provide a non-empty string as queue name")
        .emit(ctx);
      return failure::promise();
    }
    if (args.queue.inner.starts_with("sqs://")) {
      args.queue.inner.erase(0, 6);
    }
    // HACK: The connector args should probably just take a duration instead.
    if (dur) {
      args.poll_time
        = located{std::chrono::duration_cast<std::chrono::seconds>(dur->inner),
                  dur->source};
    }
    if (args.poll_time) {
      if (args.poll_time->inner < 1s || args.poll_time->inner > 20s) {
        diagnostic::error("invalid poll time: {}", args.poll_time->inner)
          .primary(args.poll_time->source)
          .hint("poll time must be in the interval [1s, 20s]")
          .emit(ctx);
        return failure::promise();
      }
    }
    return std::make_unique<Operator>(std::move(args));
  }

  auto load_properties() const
    -> operator_factory_plugin::load_properties_t override {
    if constexpr (std::same_as<Operator, sqs_loader>) {
      return {
        .schemes = {"sqs"},
        .strip_scheme = true,
      };
    } else {
      return operator_factory_plugin::load_properties();
    }
  }

  auto save_properties() const
    -> operator_factory_plugin::save_properties_t override {
    if constexpr (std::same_as<Operator, sqs_saver>) {
      return {
        .schemes = {"sqs"},
        .strip_scheme = true,
      };
    } else {
      return operator_factory_plugin::save_properties();
    }
  }
};

} // namespace
using load_plugin = plugin<sqs_loader>;
using save_plugin = plugin<sqs_saver>;

} // namespace tenzir::plugins::sqs

TENZIR_REGISTER_PLUGIN(tenzir::plugins::sqs::load_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::sqs::save_plugin)
