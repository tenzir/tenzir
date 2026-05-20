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

#include <boost/url/parse.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <limits>
#include <tuple>

namespace tenzir::plugins::cloudwatch {

namespace {

using namespace std::chrono_literals;

constexpr auto max_filter_streams = size_t{100};
constexpr auto max_read_count = uint64_t{2'147'483'647};
constexpr auto max_parallel_queue_capacity
  = uint64_t{std::numeric_limits<uint32_t>::max()};

auto valid_endpoint_url(std::string_view endpoint) -> bool {
  auto parsed = boost::urls::parse_uri_reference(endpoint);
  if (not parsed) {
    return false;
  }
  auto view = boost::urls::url_view{*parsed};
  return not view.scheme().empty() and not view.host().empty();
}

auto string_or_list_size(located<data> const& value) -> Option<size_t> {
  if (try_as<std::string>(&value.inner)) {
    return size_t{1};
  }
  auto const* values = try_as<list>(&value.inner);
  if (not values) {
    return None{};
  }
  if (not std::ranges::all_of(*values, [](auto const& item) {
        return try_as<std::string>(&item) != nullptr;
      })) {
    return None{};
  }
  return values->size();
}

auto has_empty_string(located<data> const& value) -> bool {
  if (auto const* str = try_as<std::string>(&value.inner)) {
    return str->empty();
  }
  auto const* values = try_as<list>(&value.inner);
  TENZIR_ASSERT(values);
  return std::ranges::any_of(*values, [](auto const& item) {
    auto const* str = try_as<std::string>(&item);
    TENZIR_ASSERT(str);
    return str->empty();
  });
}

auto has_non_identifier_group(located<data> const& value) -> bool {
  if (auto const* str = try_as<std::string>(&value.inner)) {
    return not str->starts_with("arn:")
           or str->find(":log-group:") == std::string::npos;
  }
  auto const* values = try_as<list>(&value.inner);
  TENZIR_ASSERT(values);
  return std::ranges::any_of(*values, [](auto const& item) {
    auto const* str = try_as<std::string>(&item);
    TENZIR_ASSERT(str);
    return not str->starts_with("arn:")
           or str->find(":log-group:") == std::string::npos;
  });
}

auto is_valid_log_group_name(std::string_view value) -> bool {
  return std::ranges::all_of(value, [](char c) {
    return c == '.' or c == '-' or c == '_' or c == '/' or c == '#'
           or (c >= 'A' and c <= 'Z') or (c >= 'a' and c <= 'z')
           or (c >= '0' and c <= '9');
  });
}

auto is_valid_log_stream_name(std::string_view value) -> bool {
  return value.find_first_of(":*") == std::string_view::npos;
}

auto emit_invalid_log_group_name(location source, diagnostic_handler& dh)
  -> void {
  diagnostic::error("invalid CloudWatch log group name")
    .primary(source)
    .hint("log group names may only contain `.`, `-`, `_`, `/`, `#`, "
          "letters, and digits")
    .emit(dh);
}

auto emit_invalid_log_stream_name(location source, diagnostic_handler& dh)
  -> void {
  diagnostic::error("invalid CloudWatch log stream name")
    .primary(source)
    .hint("log stream names must not contain `:` or `*`")
    .emit(dh);
}

auto has_wildcard_group_identifier(located<data> const& value) -> bool {
  if (auto const* str = try_as<std::string>(&value.inner)) {
    return str->ends_with('*');
  }
  auto const* values = try_as<list>(&value.inner);
  TENZIR_ASSERT(values);
  return std::ranges::any_of(*values, [](auto const& item) {
    auto const* str = try_as<std::string>(&item);
    TENZIR_ASSERT(str);
    return str->ends_with('*');
  });
}

auto initial_to_cloudwatch_args(std::string method) -> ToCloudWatchArgs {
  auto result = ToCloudWatchArgs{};
  result.method = {std::move(method), location::unknown};
  result.payload = default_to_amazon_cloudwatch_message_expression();
  result.timestamp = ast::expression{
    ast::root_field{ast::identifier{"timestamp", location::unknown}},
  };
  return result;
}

class from_plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "from_amazon_cloudwatch";
  }

  auto describe() const -> Description override {
    auto d = Describer<FromCloudWatchArgs, FromCloudWatch>{};
    d.operator_location(&FromCloudWatchArgs::operator_location);
    auto group = d.positional("group", &FromCloudWatchArgs::group,
                              "string|list<string>");
    auto mode = d.named_optional("mode", &FromCloudWatchArgs::mode);
    auto stream
      = d.named("stream", &FromCloudWatchArgs::stream, "string|list<string>");
    auto stream_prefix
      = d.named("stream_prefix", &FromCloudWatchArgs::stream_prefix);
    auto filter = d.named("filter", &FromCloudWatchArgs::filter);
    auto start = d.named("start", &FromCloudWatchArgs::start);
    auto end = d.named("end", &FromCloudWatchArgs::end);
    auto count = d.named("count", &FromCloudWatchArgs::count);
    auto from_start = d.named("from_start", &FromCloudWatchArgs::from_start);
    auto unmask = d.named("unmask", &FromCloudWatchArgs::unmask);
    d.named("aws_region", &FromCloudWatchArgs::aws_region);
    d.named("aws_iam", &FromCloudWatchArgs::aws_iam);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      TRY(auto group_value, ctx.get(group));
      auto effective_mode = std::string{"live"};
      auto mode_source = location::unknown;
      if (auto value = ctx.get(mode)) {
        effective_mode = value->inner;
        mode_source = value->source;
        if (effective_mode != "live" and effective_mode != "search"
            and effective_mode != "replay") {
          diagnostic::error("invalid CloudWatch mode `{}`", effective_mode)
            .primary(value->source)
            .hint("expected `live`, `search`, or `replay`")
            .emit(ctx);
        }
      }
      auto group_count = string_or_list_size(group_value);
      if (not group_count) {
        diagnostic::error("`group` must be a string or a list of strings")
          .primary(group_value.source)
          .emit(ctx);
        return std::nullopt;
      }
      if (*group_count == size_t{0} or has_empty_string(group_value)) {
        diagnostic::error("group must not be empty")
          .primary(group_value.source)
          .emit(ctx);
      }
      if (effective_mode != "live" and *group_count > size_t{1}) {
        diagnostic::error("multiple groups require `mode=\"live\"`")
          .primary(mode_source)
          .emit(ctx);
      }
      if (effective_mode != "live") {
        auto invalid_group = [](std::string const& group) {
          return not group.starts_with("arn:")
                 and not is_valid_log_group_name(group);
        };
        auto has_invalid_group = false;
        if (auto const* group = try_as<std::string>(&group_value.inner)) {
          has_invalid_group = invalid_group(*group);
        } else {
          auto const* groups = try_as<list>(&group_value.inner);
          TENZIR_ASSERT(groups);
          has_invalid_group
            = std::ranges::any_of(*groups, [&](auto const& item) {
                auto const* group = try_as<std::string>(&item);
                TENZIR_ASSERT(group);
                return invalid_group(*group);
              });
        }
        if (has_invalid_group) {
          emit_invalid_log_group_name(group_value.source, ctx);
        }
      }
      if (effective_mode == "live" and *group_count > size_t{10}) {
        diagnostic::error("`mode=\"live\"` accepts at most 10 groups")
          .primary(mode_source ? mode_source : group_value.source.subloc(0, 1))
          .emit(ctx);
      }
      if (effective_mode == "live" and has_non_identifier_group(group_value)) {
        diagnostic::error("`mode=\"live\"` requires log group ARNs")
          .primary(*group_count == size_t{1} ? group_value.source
                                             : group_value.source.subloc(0, 1))
          .emit(ctx);
      }
      if (effective_mode == "live"
          and has_wildcard_group_identifier(group_value)) {
        diagnostic::error("`mode=\"live\"` requires log group ARNs without "
                          "wildcard suffixes")
          .primary(*group_count == size_t{1} ? group_value.source
                                             : group_value.source.subloc(0, 1))
          .emit(ctx);
      }
      auto stream_count = Option<size_t>{};
      if (auto value = ctx.get(stream)) {
        stream_count = string_or_list_size(*value);
        if (not stream_count) {
          diagnostic::error("`stream` must be a string or a list of strings")
            .primary(value->source)
            .emit(ctx);
          return std::nullopt;
        }
        if (*stream_count == size_t{0} or has_empty_string(*value)) {
          diagnostic::error("stream must not be empty")
            .primary(value->source)
            .emit(ctx);
        }
        auto has_invalid_stream = false;
        if (auto const* stream = try_as<std::string>(&value->inner)) {
          has_invalid_stream = not is_valid_log_stream_name(*stream);
        } else {
          auto const* streams = try_as<list>(&value->inner);
          TENZIR_ASSERT(streams);
          has_invalid_stream
            = std::ranges::any_of(*streams, [](auto const& item) {
                auto const* stream = try_as<std::string>(&item);
                TENZIR_ASSERT(stream);
                return not is_valid_log_stream_name(*stream);
              });
        }
        if (has_invalid_stream) {
          emit_invalid_log_stream_name(value->source, ctx);
        }
        if ((effective_mode == "search" or effective_mode == "live")
            and *stream_count > max_filter_streams) {
          diagnostic::error("`mode=\"{}\"` accepts at most 100 streams",
                            effective_mode)
            .primary(value->source.subloc(0, 1))
            .emit(ctx);
        }
      }
      if (auto value = ctx.get(stream_prefix)) {
        if (value->inner.empty()) {
          diagnostic::error("stream prefix must not be empty")
            .primary(value->source)
            .emit(ctx);
        }
        if (not is_valid_log_stream_name(value->inner)) {
          emit_invalid_log_stream_name(value->source, ctx);
        }
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
        if (auto option = ctx.get(count)) {
          diagnostic::error("historical read option is not valid for "
                            "`mode=\"live\"`")
            .primary(option->source)
            .emit(ctx);
        }
        if (auto option = ctx.get(from_start)) {
          diagnostic::error("option is only valid for `mode=\"replay\"`")
            .primary(option->source)
            .emit(ctx);
        }
        if (auto option = ctx.get(unmask)) {
          diagnostic::error("`unmask` is not valid for `mode=\"live\"`")
            .primary(option->source)
            .emit(ctx);
        }
      } else if (effective_mode == "search") {
        if (auto option = ctx.get(from_start)) {
          diagnostic::error("option is only valid for `mode=\"replay\"`")
            .primary(option->source)
            .emit(ctx);
        }
      }
      if (auto value = ctx.get(count); effective_mode != "live" and value) {
        if (value->inner == 0) {
          diagnostic::error("count must be greater than zero")
            .primary(value->source)
            .emit(ctx);
          return std::nullopt;
        }
        if (value->inner > max_read_count) {
          diagnostic::error("count must be less than or equal to {}",
                            max_read_count)
            .primary(value->source)
            .emit(ctx);
          return std::nullopt;
        }
      }
      if (ctx.get(stream) and ctx.get(stream_prefix)) {
        diagnostic::error("`stream` and `stream_prefix` are mutually exclusive")
          .primary(ctx.get(stream_prefix)->source)
          .emit(ctx);
        return std::nullopt;
      }
      if (effective_mode == "live" and ctx.get(stream)
          and *group_count > size_t{1}) {
        diagnostic::error("`stream` requires exactly one group")
          .primary(ctx.get(stream)->source)
          .emit(ctx);
        return std::nullopt;
      }
      if (effective_mode == "live" and ctx.get(stream_prefix)
          and *group_count > size_t{1}) {
        diagnostic::error("`stream_prefix` requires exactly one group")
          .primary(ctx.get(stream_prefix)->source)
          .emit(ctx);
        return std::nullopt;
      }
      if (effective_mode == "replay") {
        if (not ctx.get(stream)) {
          diagnostic::error("`mode=\"replay\"` requires `stream`")
            .primary(mode_source)
            .emit(ctx);
        }
        if (stream_count and *stream_count != size_t{1}) {
          diagnostic::error("`mode=\"replay\"` requires exactly one stream")
            .primary(ctx.get(stream)->source)
            .emit(ctx);
        }
        if (auto option = ctx.get(filter)) {
          diagnostic::error("option is not valid for `mode=\"replay\"`")
            .primary(option->source)
            .emit(ctx);
        }
        if (auto option = ctx.get(stream_prefix)) {
          diagnostic::error("option is not valid for `mode=\"replay\"`")
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
    return "to_amazon_cloudwatch";
  }

  auto describe() const -> Description override {
    auto initial = initial_to_cloudwatch_args("put");
    auto d = Describer<ToCloudWatchArgs, ToCloudWatch>{std::move(initial)};
    d.operator_location(&ToCloudWatchArgs::operator_location);
    auto group = d.positional("group", &ToCloudWatchArgs::log_group);
    auto stream = d.named("stream", &ToCloudWatchArgs::log_stream);
    auto method = d.named_optional("method", &ToCloudWatchArgs::method);
    auto payload
      = d.named_optional("payload", &ToCloudWatchArgs::payload, "blob|string");
    d.named_optional("timestamp", &ToCloudWatchArgs::timestamp, "time");
    auto batch_timeout
      = d.named("batch_timeout", &ToCloudWatchArgs::batch_timeout);
    auto batch_size = d.named("batch_size", &ToCloudWatchArgs::batch_size);
    auto parallel = d.named("parallel", &ToCloudWatchArgs::parallel);
    auto token = d.named("token", &ToCloudWatchArgs::token);
    auto endpoint = d.named("endpoint", &ToCloudWatchArgs::endpoint);
    d.named("aws_region", &ToCloudWatchArgs::aws_region);
    auto aws_iam = d.named("aws_iam", &ToCloudWatchArgs::aws_iam);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      TRY(auto group_value, ctx.get(group));
      auto stream_value = ctx.get(stream);
      if (group_value.inner.empty()) {
        diagnostic::error("log group must not be empty")
          .primary(group_value.source)
          .emit(ctx);
      }
      if (not is_valid_log_group_name(group_value.inner)) {
        emit_invalid_log_group_name(group_value.source, ctx);
      }
      if (not stream_value) {
        diagnostic::error("`stream` is required")
          .primary(group_value.source)
          .emit(ctx);
        return std::nullopt;
      }
      if (stream_value->inner.empty()) {
        diagnostic::error("log stream must not be empty")
          .primary(stream_value->source)
          .emit(ctx);
      }
      if (not is_valid_log_stream_name(stream_value->inner)) {
        emit_invalid_log_stream_name(stream_value->source, ctx);
      }
      auto effective_method = std::string{"put"};
      auto method_source = location::unknown;
      if (auto value = ctx.get(method)) {
        effective_method = value->inner;
        method_source = value->source;
      }
      if (effective_method != "put" and effective_method != "hlc"
          and effective_method != "ndjson" and effective_method != "json") {
        diagnostic::error("invalid CloudWatch method `{}`", effective_method)
          .primary(method_source)
          .hint("expected `put`, `hlc`, `ndjson`, or `json`")
          .emit(ctx);
      }
      if (effective_method == "put" and ctx.get(token)) {
        diagnostic::error("`token` is not valid for `method=\"put\"`")
          .primary(ctx.get(token)->source)
          .emit(ctx);
      }
      if (effective_method != "put" and ctx.get(token) and ctx.get(aws_iam)) {
        diagnostic::error("`token` and `aws_iam` are mutually exclusive")
          .primary(ctx.get(token)->source)
          .emit(ctx);
      }
      if (auto value = ctx.get(endpoint);
          value and not valid_endpoint_url(value->inner)) {
        diagnostic::error("failed to initialize CloudWatch HTTP client: "
                          "invalid url: {}",
                          value->inner)
          .primary(value->source)
          .emit(ctx);
      }
      std::ignore = ctx.get(payload);
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
      if (auto value = ctx.get(parallel);
          value and value->inner >= max_parallel_queue_capacity) {
        diagnostic::error("parallel must be less than {}",
                          max_parallel_queue_capacity)
          .primary(value->source)
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
