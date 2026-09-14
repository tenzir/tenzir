//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/plugin.hpp"

#include <tenzir/compile_ctx.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin/register.hpp>

#include "operators.hpp"

using namespace std::chrono_literals;

namespace tenzir::plugins::sqs {

namespace {

constexpr auto max_visibility_timeout = 12h;

class from_plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "from_amazon_sqs";
  }

  auto describe() const -> Description override {
    auto d = Describer<FromSqsArgs, FromSqs>{};
    d.operator_location(&FromSqsArgs::operator_location);
    auto queue = d.positional("queue", &FromSqsArgs::queue);
    d.named("keep_messages", &FromSqsArgs::keep_messages);
    auto batch_size = d.named("batch_size", &FromSqsArgs::batch_size);
    d.named("aws_region", &FromSqsArgs::aws_region);
    d.named("aws_iam", &FromSqsArgs::aws_iam);
    auto pt = d.named("poll_time", &FromSqsArgs::poll_time);
    auto vt = d.named("visibility_timeout", &FromSqsArgs::visibility_timeout);
    d.validate([queue, batch_size, pt, vt](DescribeCtx& ctx) -> Empty {
      TRY(auto q, ctx.get(queue));
      if (q.inner.empty()) {
        diagnostic::error("queue must not be empty")
          .primary(q.source)
          .hint("provide a non-empty string as queue name")
          .emit(ctx);
      }
      if (auto bs = ctx.get(batch_size)) {
        if (bs->inner < 1 or bs->inner > 10) {
          diagnostic::error("invalid batch size: {}", bs->inner)
            .primary(bs->source)
            .hint("batch size must be in the interval [1, 10]")
            .emit(ctx);
        }
      }
      if (auto poll_time = ctx.get(pt)) {
        auto secs
          = std::chrono::duration_cast<std::chrono::seconds>(poll_time->inner);
        if (secs < 1s or secs > 20s) {
          diagnostic::error("invalid poll time: {}", poll_time->inner)
            .primary(poll_time->source)
            .hint("poll time must be in the interval [1s, 20s]")
            .emit(ctx);
        }
      }
      if (auto visibility_timeout = ctx.get(vt)) {
        if (visibility_timeout->inner < 0s
            or visibility_timeout->inner > max_visibility_timeout) {
          diagnostic::error("invalid visibility timeout: {}",
                            visibility_timeout->inner)
            .primary(visibility_timeout->source)
            .hint("visibility timeout must be in the interval [0s, 12h]")
            .emit(ctx);
        }
      }
      return {};
    });
    return d.without_optimize();
  }
};

class to_plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "to_amazon_sqs";
  }

  auto describe() const -> Description override {
    auto initial = ToSqsArgs{};
    initial.message = default_to_amazon_sqs_message_expression();
    auto d = Describer<ToSqsArgs, ToSqs>{std::move(initial)};
    d.operator_location(&ToSqsArgs::operator_location);
    auto queue = d.positional("queue", &ToSqsArgs::queue);
    d.named_optional("message", &ToSqsArgs::message, "blob|string");
    d.named("aws_region", &ToSqsArgs::aws_region);
    d.named("aws_iam", &ToSqsArgs::aws_iam);
    d.validate([queue](DescribeCtx& ctx) -> Empty {
      TRY(auto q, ctx.get(queue));
      if (q.inner.empty()) {
        diagnostic::error("queue must not be empty")
          .primary(q.source)
          .hint("provide a non-empty string as queue name")
          .emit(ctx);
      }
      return {};
    });
    // Every instance opens its own SQS client, and SQS itself is a
    // concurrency-safe broker: standard queues never guaranteed order across
    // messages, and this sink does not set a message group ID. Replicating the
    // operator therefore weakens no guarantee that a single instance provided.
    d.parallelizable();
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::sqs

TENZIR_REGISTER_PLUGIN(tenzir::plugins::sqs::from_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::sqs::to_plugin)
