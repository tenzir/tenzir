//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/plugin.hpp"

#include "tenzir/detail/string_literal.hpp"

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

template <template <class, detail::string_literal = ""> class Adapter,
          class Plugin>
class plugin final : public virtual operator_plugin2<Adapter<Plugin>> {
public:
  auto make(operator_factory_plugin::invocation inv,
            session ctx) const -> failure_or<operator_ptr> override {
    auto args = connector_args{};
    auto dur = std::optional<located<duration>>{};
    TRY(argument_parser2::operator_(this->name())
          .add(args.queue, "<queue>")
          .add("poll_time", dur)
          .parse(inv, ctx));
    if (args.queue.inner.empty()) {
      diagnostic::error("queue must not be empty")
        .primary(args.queue.source)
        .hint("provide a non-empty string as queue name")
        .emit(ctx);
      return failure::promise();
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
    return std::make_unique<Adapter<Plugin>>(Plugin{std::move(args)});
  }
};

} // namespace
using load_plugin = plugin<loader_adapter, sqs_loader>;
using save_plugin = plugin<saver_adapter, sqs_saver>;

} // namespace tenzir::plugins::sqs

TENZIR_REGISTER_PLUGIN(tenzir::plugins::sqs::load_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::sqs::save_plugin)
