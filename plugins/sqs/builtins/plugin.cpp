//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/plugin.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/operator_plugin.hpp>

#include "operators_neo.hpp"

using namespace std::chrono_literals;

namespace tenzir::plugins::sqs {

namespace {

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
                          public virtual OperatorPlugin {
public:
  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    TRY(auto args, parse_connector_args(name(), inv, ctx));
    return std::make_unique<sqs_loader>(std::move(args));
  }

  auto describe() const -> Description override {
    auto d = Describer<sqs_args, LoadSqs>{};
    d.operator_location(&sqs_args::operator_location);
    auto queue = d.positional("queue", &sqs_args::queue);
    d.named("aws_region", &sqs_args::aws_region);
    d.named("aws_iam", &sqs_args::aws_iam);
    auto pt = d.named("poll_time", &sqs_args::poll_time);
    d.validate([queue, pt](DescribeCtx& ctx) -> Empty {
      TRY(auto q, ctx.get(queue));
      if (q.inner.empty()) {
        diagnostic::error("queue must not be empty")
          .primary(q.source)
          .hint("provide a non-empty string as queue name")
          .emit(ctx);
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
      return {};
    });
    return d.without_optimize();
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
                          public virtual OperatorPlugin {
public:
  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    TRY(auto args, parse_connector_args(name(), inv, ctx));
    if (args.poll_time) {
      diagnostic::warning("`poll_time` is deprecated for `save_sqs` and will "
                          "be ignored")
        .primary(args.poll_time->source)
        .emit(ctx);
    }
    return std::make_unique<sqs_saver>(std::move(args));
  }

  auto describe() const -> Description override {
    auto d = Describer<sqs_args, SaveSqs>{};
    d.operator_location(&sqs_args::operator_location);
    auto queue = d.positional("queue", &sqs_args::queue);
    d.named("aws_region", &sqs_args::aws_region);
    d.named("aws_iam", &sqs_args::aws_iam);
    auto pt = d.named("poll_time", &sqs_args::poll_time);
    d.validate([queue, pt](DescribeCtx& ctx) -> Empty {
      TRY(auto q, ctx.get(queue));
      if (q.inner.empty()) {
        diagnostic::error("queue must not be empty")
          .primary(q.source)
          .hint("provide a non-empty string as queue name")
          .emit(ctx);
      }
      if (auto poll_time = ctx.get(pt)) {
        diagnostic::warning("`poll_time` is deprecated for `save_sqs` and will "
                            "be ignored")
          .primary(poll_time->source)
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }

  auto save_properties() const
    -> operator_factory_plugin::save_properties_t override {
    return {
      .schemes = {"sqs"},
      .strip_scheme = true,
    };
  }
};

} // namespace

} // namespace tenzir::plugins::sqs

TENZIR_REGISTER_PLUGIN(tenzir::plugins::sqs::load_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::sqs::save_plugin)
