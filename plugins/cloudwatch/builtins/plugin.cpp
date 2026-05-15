//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "cloudwatch/operators.hpp"

#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin/register.hpp>

#include <chrono>
#include <limits>

namespace tenzir::plugins::cloudwatch {

namespace {

using namespace std::chrono_literals;

auto is_mode(std::string_view value) -> bool {
  return value == "live" or value == "filter" or value == "get";
}

auto is_method(std::string_view value) -> bool {
  return value == "put_log_events" or value == "hlc";
}

class from_plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "from_cloudwatch";
  }

  auto describe() const -> Description override {
    auto d = Describer<FromCloudWatchArgs, FromCloudWatch>{};
    d.operator_location(&FromCloudWatchArgs::operator_location);
    auto log_group = d.positional("log_group", &FromCloudWatchArgs::log_group);
    auto mode = d.named_optional("mode", &FromCloudWatchArgs::mode);
    d.named("log_group_identifiers",
            &FromCloudWatchArgs::log_group_identifiers);
    auto log_stream = d.named("log_stream", &FromCloudWatchArgs::log_stream);
    auto log_streams = d.named("log_streams", &FromCloudWatchArgs::log_streams);
    auto log_stream_prefix
      = d.named("log_stream_prefix", &FromCloudWatchArgs::log_stream_prefix);
    auto filter = d.named("filter", &FromCloudWatchArgs::filter);
    auto start = d.named("start", &FromCloudWatchArgs::start);
    auto end = d.named("end", &FromCloudWatchArgs::end);
    auto limit = d.named("limit", &FromCloudWatchArgs::limit);
    auto start_from_head
      = d.named("start_from_head", &FromCloudWatchArgs::start_from_head);
    d.named("unmask", &FromCloudWatchArgs::unmask);
    d.named("aws_region", &FromCloudWatchArgs::aws_region);
    d.named("aws_iam", &FromCloudWatchArgs::aws_iam);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      TRY(auto group, ctx.get(log_group));
      auto effective_mode = std::string{"live"};
      if (auto value = ctx.get(mode)) {
        effective_mode = value->inner;
        if (not is_mode(effective_mode)) {
          diagnostic::error("invalid CloudWatch mode `{}`", effective_mode)
            .primary(value->source)
            .hint("expected `live`, `filter`, or `get`")
            .emit(ctx);
        }
      }
      if (group.inner.empty()) {
        diagnostic::error("log group must not be empty")
          .primary(group.source)
          .emit(ctx);
      }
      if (effective_mode == "live") {
        if (auto option = ctx.get(start)) {
          diagnostic::error("historical read option is not valid for "
                            "`mode=\"live\"`")
            .primary(option->source)
            .emit(ctx);
        }
        if (auto option = ctx.get(end)) {
          diagnostic::error("historical read option is not valid for "
                            "`mode=\"live\"`")
            .primary(option->source)
            .emit(ctx);
        }
        if (auto option = ctx.get(limit)) {
          diagnostic::error("historical read option is not valid for "
                            "`mode=\"live\"`")
            .primary(option->source)
            .emit(ctx);
        }
        if (auto option = ctx.get(start_from_head)) {
          diagnostic::error("historical read option is not valid for "
                            "`mode=\"live\"`")
            .primary(option->source)
            .emit(ctx);
        }
      }
      if (auto value = ctx.get(limit);
          value and value->inner > std::numeric_limits<int>::max()) {
        diagnostic::error("limit must not exceed {}",
                          std::numeric_limits<int>::max())
          .primary(value->source)
          .emit(ctx);
        return std::nullopt;
      }
      if (effective_mode != "get" and ctx.get(log_stream)
          and ctx.get(log_streams)) {
        diagnostic::error("`log_stream` and `log_streams` are mutually "
                          "exclusive")
          .primary(ctx.get(log_streams)->source)
          .emit(ctx);
        return std::nullopt;
      }
      if (effective_mode != "get" and ctx.get(log_streams)
          and ctx.get(log_stream_prefix)) {
        diagnostic::error("`log_streams` and `log_stream_prefix` are "
                          "mutually exclusive")
          .primary(ctx.get(log_stream_prefix)->source)
          .emit(ctx);
        return std::nullopt;
      }
      if (effective_mode != "get" and ctx.get(log_stream)
          and ctx.get(log_stream_prefix)) {
        diagnostic::error("`log_stream` and `log_stream_prefix` are "
                          "mutually exclusive")
          .primary(ctx.get(log_stream_prefix)->source)
          .emit(ctx);
        return std::nullopt;
      }
      if (effective_mode == "get") {
        if (not ctx.get(log_stream)) {
          diagnostic::error("`mode=\"get\"` requires `log_stream`")
            .primary(group.source)
            .emit(ctx);
        }
        if (auto option = ctx.get(filter)) {
          diagnostic::error("option is not valid for `mode=\"get\"`")
            .primary(option->source)
            .emit(ctx);
        }
        if (auto option = ctx.get(log_streams)) {
          diagnostic::error("option is not valid for `mode=\"get\"`")
            .primary(option->source)
            .emit(ctx);
        }
        if (auto option = ctx.get(log_stream_prefix)) {
          diagnostic::error("option is not valid for `mode=\"get\"`")
            .primary(option->source)
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
    return "to_cloudwatch";
  }

  auto describe() const -> Description override {
    auto initial = ToCloudWatchArgs{};
    initial.message = default_to_cloudwatch_message_expression();
    initial.timestamp = ast::expression{
      ast::root_field{ast::identifier{"timestamp", location::unknown}},
    };
    auto d = Describer<ToCloudWatchArgs, ToCloudWatch>{std::move(initial)};
    d.operator_location(&ToCloudWatchArgs::operator_location);
    auto log_group = d.positional("log_group", &ToCloudWatchArgs::log_group);
    auto log_stream = d.positional("log_stream", &ToCloudWatchArgs::log_stream);
    auto method = d.named_optional("method", &ToCloudWatchArgs::method);
    d.named_optional("message", &ToCloudWatchArgs::message, "blob|string");
    d.named_optional("timestamp", &ToCloudWatchArgs::timestamp, "time");
    auto batch_timeout
      = d.named("batch_timeout", &ToCloudWatchArgs::batch_timeout);
    auto batch_size = d.named("batch_size", &ToCloudWatchArgs::batch_size);
    auto parallel = d.named("parallel", &ToCloudWatchArgs::parallel);
    auto token = d.named("token", &ToCloudWatchArgs::token);
    d.named("endpoint", &ToCloudWatchArgs::endpoint);
    auto aws_region = d.named("aws_region", &ToCloudWatchArgs::aws_region);
    auto aws_iam = d.named("aws_iam", &ToCloudWatchArgs::aws_iam);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      TRY(auto group, ctx.get(log_group));
      TRY(auto stream, ctx.get(log_stream));
      auto effective_method = std::string{"put_log_events"};
      if (auto value = ctx.get(method)) {
        effective_method = value->inner;
        if (not is_method(effective_method)) {
          diagnostic::error("invalid CloudWatch method `{}`", effective_method)
            .primary(value->source)
            .hint("expected `put_log_events` or `hlc`")
            .emit(ctx);
        }
      }
      if (group.inner.empty()) {
        diagnostic::error("log group must not be empty")
          .primary(group.source)
          .emit(ctx);
      }
      if (stream.inner.empty()) {
        diagnostic::error("log stream must not be empty")
          .primary(stream.source)
          .emit(ctx);
      }
      if (auto value = ctx.get(batch_timeout); value and value->inner <= 0s) {
        diagnostic::error("batch timeout must be greater than zero")
          .primary(value->source)
          .emit(ctx);
      }
      if (auto value = ctx.get(batch_size); value and value->inner == 0) {
        diagnostic::error("batch size must be greater than zero")
          .primary(value->source)
          .emit(ctx);
      }
      if (auto value = ctx.get(parallel); value and value->inner == 0) {
        diagnostic::error("parallel must be greater than zero")
          .primary(value->source)
          .emit(ctx);
      }
      if (effective_method == "hlc") {
        if (not ctx.get(token)) {
          diagnostic::error("`method=\"hlc\"` requires `token`")
            .primary(group.source)
            .emit(ctx);
        }
        if (auto option = ctx.get(aws_iam)) {
          diagnostic::error("AWS IAM options are not valid for "
                            "`method=\"hlc\"`")
            .primary(option->source)
            .emit(ctx);
        }
        if (auto option = ctx.get(aws_region)) {
          diagnostic::error("AWS IAM options are not valid for "
                            "`method=\"hlc\"`")
            .primary(option->source)
            .emit(ctx);
        }
      }
      if (effective_method == "put_log_events" and ctx.get(token)) {
        diagnostic::error("`token` is not valid for "
                          "`method=\"put_log_events\"`")
          .primary(ctx.get(token)->source)
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::cloudwatch

TENZIR_REGISTER_PLUGIN(tenzir::plugins::cloudwatch::from_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::cloudwatch::to_plugin)
