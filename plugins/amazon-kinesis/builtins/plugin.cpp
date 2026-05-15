//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/diagnostics.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin/register.hpp>

#include "operators.hpp"

using namespace std::chrono_literals;

namespace tenzir::plugins::amazon_kinesis {

namespace {

constexpr auto max_batch_size = uint64_t{500};
constexpr auto max_records_per_call = uint64_t{10'000};

auto validate_stream(const located<std::string>& stream, diagnostic_handler& dh)
  -> void {
  if (stream.inner.empty()) {
    diagnostic::error("stream must not be empty")
      .primary(stream.source)
      .hint("provide a non-empty Kinesis stream name")
      .emit(dh);
  }
}

auto validate_start(const located<data>& start, diagnostic_handler& dh)
  -> void {
  if (auto string = try_as<std::string>(start.inner)) {
    if (*string == "latest" or *string == "trim_horizon") {
      return;
    }
    diagnostic::error("invalid start position: `{}`", *string)
      .primary(start.source)
      .hint("use `latest`, `trim_horizon`, or a timestamp")
      .emit(dh);
    return;
  }
  if (try_as<time>(start.inner)) {
    return;
  }
  diagnostic::error("expected `string` or `time` for `start`")
    .primary(start.source)
    .emit(dh);
}

class from_plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "from_amazon_kinesis";
  }

  auto describe() const -> Description override {
    auto d = Describer<FromAmazonKinesisArgs, FromAmazonKinesis>{};
    d.operator_location(&FromAmazonKinesisArgs::operator_location);
    auto stream = d.positional("stream", &FromAmazonKinesisArgs::stream);
    auto start = d.named("start", &FromAmazonKinesisArgs::start);
    auto count = d.named("count", &FromAmazonKinesisArgs::count);
    d.named("exit", &FromAmazonKinesisArgs::exit);
    auto records_per_call
      = d.named("records_per_call", &FromAmazonKinesisArgs::records_per_call);
    auto poll_idle = d.named("poll_idle", &FromAmazonKinesisArgs::poll_idle);
    d.named("aws_region", &FromAmazonKinesisArgs::aws_region);
    d.named("aws_iam", &FromAmazonKinesisArgs::aws_iam);
    d.named("endpoint", &FromAmazonKinesisArgs::endpoint);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      TRY(auto s, ctx.get(stream));
      validate_stream(s, ctx);
      if (auto value = ctx.get(start)) {
        validate_start(*value, ctx);
      }
      if (auto value = ctx.get(count)) {
        if (value->inner == 0) {
          diagnostic::error("`count` must be greater than zero")
            .primary(value->source)
            .emit(ctx);
        }
      }
      if (auto value = ctx.get(records_per_call)) {
        if (value->inner == 0 or value->inner > max_records_per_call) {
          diagnostic::error("`records_per_call` must be in the interval [1, "
                            "{}]",
                            max_records_per_call)
            .primary(value->source)
            .emit(ctx);
        }
      }
      if (auto value = ctx.get(poll_idle)) {
        if (value->inner < duration::zero()) {
          diagnostic::error("`poll_idle` must be non-negative")
            .primary(value->source)
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
    return "to_amazon_kinesis";
  }

  auto describe() const -> Description override {
    auto initial = ToAmazonKinesisArgs{};
    initial.message = default_message_expression();
    auto d
      = Describer<ToAmazonKinesisArgs, ToAmazonKinesis>{std::move(initial)};
    d.operator_location(&ToAmazonKinesisArgs::operator_location);
    auto stream = d.positional("stream", &ToAmazonKinesisArgs::stream);
    d.named_optional("message", &ToAmazonKinesisArgs::message, "blob|string");
    d.named("partition_key", &ToAmazonKinesisArgs::partition_key, "string");
    auto batch_size = d.named("batch_size", &ToAmazonKinesisArgs::batch_size);
    auto batch_timeout
      = d.named("batch_timeout", &ToAmazonKinesisArgs::batch_timeout);
    auto parallel = d.named("parallel", &ToAmazonKinesisArgs::parallel);
    d.named("aws_region", &ToAmazonKinesisArgs::aws_region);
    d.named("aws_iam", &ToAmazonKinesisArgs::aws_iam);
    d.named("endpoint", &ToAmazonKinesisArgs::endpoint);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      TRY(auto s, ctx.get(stream));
      validate_stream(s, ctx);
      if (auto value = ctx.get(batch_size)) {
        if (value->inner == 0 or value->inner > max_batch_size) {
          diagnostic::error("`batch_size` must be in the interval [1, {}]",
                            max_batch_size)
            .primary(value->source)
            .emit(ctx);
        }
      }
      if (auto value = ctx.get(batch_timeout)) {
        if (value->inner <= duration::zero()) {
          diagnostic::error("`batch_timeout` must be positive")
            .primary(value->source)
            .emit(ctx);
        }
      }
      if (auto value = ctx.get(parallel)) {
        if (value->inner == 0) {
          diagnostic::error("`parallel` must be greater than zero")
            .primary(value->source)
            .emit(ctx);
        }
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::amazon_kinesis

TENZIR_REGISTER_PLUGIN(tenzir::plugins::amazon_kinesis::from_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::amazon_kinesis::to_plugin)
