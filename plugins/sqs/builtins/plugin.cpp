//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/plugin.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator_plugin.hpp>

#include "operators.hpp"

using namespace std::chrono_literals;

namespace tenzir::plugins::sqs {

namespace {

constexpr auto max_visibility_timeout = 12h;

auto parse_connector_args(const std::string& name,
                          operator_factory_invocation& inv, session ctx)
  -> failure_or<connector_args> {
  auto args = connector_args{};
  auto dur = std::optional<located<duration>>{};
  auto iam_opts = std::optional<located<record>>{};
  TRY(argument_parser2::operator_(name)
        .positional("queue", args.queue)
        .named("poll_time", dur)
        .named("aws_region", args.aws_region)
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
  if (dur) {
    args.poll_time
      = located{std::chrono::duration_cast<std::chrono::seconds>(dur->inner),
                dur->source};
  }
  if (args.poll_time) {
    if (args.poll_time->inner < 1s or args.poll_time->inner > 20s) {
      diagnostic::error("invalid poll time: {}", args.poll_time->inner)
        .primary(args.poll_time->source)
        .hint("poll time must be in the interval [1s, 20s]")
        .emit(ctx);
      return failure::promise();
    }
  }
  return args;
}

class load_plugin final : public virtual operator_plugin2<sqs_loader>,
                          public virtual operator_compiler_plugin {
public:
  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    TRY(auto args, parse_connector_args(name(), inv, ctx));
    return std::make_unique<sqs_loader>(std::move(args));
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<ir::CompileResult> override {
    diagnostic::error("`load_sqs` is not supported by the new executor")
      .primary(inv.op.get_location())
      .hint("use `from_sqs` to produce events directly")
      .emit(ctx);
    return failure::promise();
  }

  auto load_properties() const
    -> operator_factory_plugin::load_properties_t override {
    return {
      .schemes = {"sqs"},
      .strip_scheme = true,
    };
  }
};

class save_plugin final : public virtual operator_plugin2<sqs_saver>,
                          public virtual operator_compiler_plugin {
public:
  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    TRY(auto args, parse_connector_args(name(), inv, ctx));
    return std::make_unique<sqs_saver>(std::move(args));
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<ir::CompileResult> override {
    diagnostic::error("`save_sqs` is not supported by the new executor")
      .primary(inv.op.get_location())
      .hint("use `to_sqs` to send events directly")
      .emit(ctx);
    return failure::promise();
  }

  auto save_properties() const
    -> operator_factory_plugin::save_properties_t override {
    return {
      .schemes = {"sqs"},
      .strip_scheme = true,
    };
  }
};

class from_plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "from_sqs";
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
    return "to_sqs";
  }

  auto describe() const -> Description override {
    auto initial = ToSqsArgs{};
    initial.message = default_to_sqs_message_expression();
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
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::sqs

TENZIR_REGISTER_PLUGIN(tenzir::plugins::sqs::load_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::sqs::save_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::sqs::from_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::sqs::to_plugin)
