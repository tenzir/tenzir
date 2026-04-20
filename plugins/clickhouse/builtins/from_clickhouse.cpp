//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/arguments.hpp"
#include "clickhouse/easy_client.hpp"
#include "clickhouse/transformers.hpp"
#include "tenzir/arc.hpp"
#include "tenzir/async.hpp"
#include "tenzir/atomic.hpp"
#include "tenzir/box.hpp"
#include "tenzir/co_match.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/series.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/value_path.hpp"

#include <arpa/inet.h>
#include <clickhouse/client.h>
#include <clickhouse/columns/array.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/decimal.h>
#include <clickhouse/columns/enum.h>
#include <clickhouse/columns/ip4.h>
#include <clickhouse/columns/ip6.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/tuple.h>
#include <fmt/format.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/BoundedQueue.h>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cinttypes>
#include <cstddef>
#include <cstring>
#include <limits>
#include <memory>
#include <unordered_map>
#include <utility>

namespace tenzir::plugins::clickhouse {

namespace {

constexpr auto clickhouse_plaintext_port = uint64_t{9000};
constexpr auto clickhouse_tls_port = uint64_t{9440};

struct FromClickhouseArgs {
  located<secret> uri = {secret::make_literal(""), location::unknown};
  Option<located<std::string>> table;
  located<secret> host = {secret::make_literal("localhost"), location::unknown};
  Option<located<uint64_t>> port = None{};
  located<secret> user = {secret::make_literal("default"), location::unknown};
  located<secret> password = {secret::make_literal(""), location::unknown};
  Option<located<std::string>> sql;
  Option<located<data>> tls;
  location operator_location;
};

struct QueryPlan {
  std::string query;
  std::string schema_name;
  Option<std::string> described_table = None{};
};

struct ParsedType;

struct ParsedField {
  std::string name;
  Box<ParsedType> value;
};

struct ParsedType {
  struct Empty {};

  struct RecordFields {
    std::vector<ParsedField> fields;
  };

  struct ListChild {
    Box<ParsedType> child;
  };

  struct TimePrecision {
    size_t value = 0;
  };

  struct DecimalScale {
    size_t value = 0;
  };

  ::clickhouse::Type::Code code = ::clickhouse::Type::Void;
  bool nullable = false;
  type tenzir_type = type{null_type{}};
  variant<Empty, RecordFields, ListChild, TimePrecision, DecimalScale> payload
    = Empty{};

  template <class Self>
  auto record_fields(this Self&& self) -> decltype(auto) {
    TENZIR_ASSERT(self.code == ::clickhouse::Type::Tuple);
    return (std::get<RecordFields>(std::forward<Self>(self).payload).fields);
  }

  template <class Self>
  auto list_child(this Self&& self) -> decltype(auto) {
    TENZIR_ASSERT(self.code == ::clickhouse::Type::Array);
    return (*std::get<ListChild>(std::forward<Self>(self).payload).child);
  }

  template <class Self>
  auto time_precision(this Self&& self) -> size_t {
    TENZIR_ASSERT(self.code == ::clickhouse::Type::DateTime64);
    return std::get<TimePrecision>(std::forward<Self>(self).payload).value;
  }

  template <class Self>
  auto decimal_scale(this Self&& self) -> size_t {
    TENZIR_ASSERT(self.code == ::clickhouse::Type::Decimal
                  or self.code == ::clickhouse::Type::Decimal32
                  or self.code == ::clickhouse::Type::Decimal64
                  or self.code == ::clickhouse::Type::Decimal128);
    return std::get<DecimalScale>(std::forward<Self>(self).payload).value;
  }
};

struct QuerySchema {
  std::string name;
  std::vector<ParsedField> columns;
};

struct SliceMessage {
  table_slice slice;
};

struct DoneMessage {};

using Message = variant<SliceMessage, DoneMessage>;
using MessageQueue = folly::coro::BoundedQueue<Message, true, true>;
constexpr auto message_queue_capacity = uint32_t{16};
constexpr auto message_queue_backoff = std::chrono::milliseconds{1};

auto has_primary_annotation(diagnostic const& diag) -> bool {
  return std::any_of(diag.annotations.begin(), diag.annotations.end(),
                     [](auto const& annotation) {
                       return annotation.primary;
                     });
}

struct RuntimeState {
  RuntimeState() : queue{message_queue_capacity} {
  }

  MessageQueue queue;
  Atomic<bool> stop_requested = false;
};

class FromClickhouse final : public Operator<void, table_slice> {
public:
  FromClickhouse() = default;

  explicit FromClickhouse(FromClickhouseArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto plan = make_query_plan(args_, ctx);
    if (not plan) {
      co_return;
    }
    auto ssl = args_.tls ? tls_options{*args_.tls} : tls_options{};
    tls_enabled_ = ssl.get_tls(&ctx.actor_system().config()).inner;
    auto const default_port
      = tls_enabled_ ? clickhouse_tls_port : clickhouse_plaintext_port;
    auto client_args = easy_client::arguments{
      .host = "",
      .port = args_.port ? *args_.port
                         : located<uint64_t>{default_port, location::unknown},
      .user = "default",
      .password = "",
      .default_database = None{},
      .ssl = std::move(ssl),
      .table = {},
      .mode = {mode::append, location::unknown},
      .primary = None{},
      .operator_location = args_.operator_location,
    };
    auto uri = std::string{};
    auto requests = std::vector<secret_request>{};
    auto has_uri = args_.uri.inner != secret::make_literal("");
    if (has_uri) {
      requests.push_back(make_secret_request("uri", args_.uri, uri, ctx.dh()));
    } else {
      requests.push_back(
        make_secret_request("host", args_.host, client_args.host, ctx.dh()));
      requests.push_back(
        make_secret_request("user", args_.user, client_args.user, ctx.dh()));
      requests.push_back(make_secret_request("password", args_.password,
                                             client_args.password, ctx.dh()));
    }
    auto ok = co_await ctx.resolve_secrets(std::move(requests));
    if (not ok) {
      co_return;
    }
    if (has_uri) {
      auto parsed = parse_connection_uri(uri, args_.uri.source, ctx.dh());
      if (not parsed) {
        co_return;
      }
      apply_connection_uri(client_args, *parsed);
      if (not parsed->has_port()) {
        client_args.port = located<uint64_t>{default_port, location::unknown};
      }
    }
    auto options = client_args.make_options(ctx.actor_system().config());
    ctx.spawn_task([this, options = std::move(options), plan = std::move(*plan),
                    &dh = ctx.dh(),
                    loc = args_.operator_location]() mutable -> Task<void> {
      auto transformed_dh = transforming_diagnostic_handler{
        dh, [loc](diagnostic diag) {
          if (not has_primary_annotation(diag)) {
            diag.annotations.emplace_back(true, std::string{}, loc);
          }
          return diag;
        }};
      co_await run_query(std::move(options), std::move(plan), transformed_dh);
    });
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    if (done_) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    co_return co_await runtime_->queue.dequeue();
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx&)
    -> Task<void> override {
    auto message = std::move(result).as<Message>();
    co_await co_match(
      std::move(message),
      [&](SliceMessage x) -> Task<void> {
        co_await push(std::move(x.slice));
        co_return;
      },
      [&](DoneMessage) -> Task<void> {
        done_ = true;
        co_return;
      });
    co_return;
  }

  auto stop(OpCtx&) -> Task<void> override {
    runtime_->stop_requested.store(true, std::memory_order_release);
    co_return;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::unspecified;
  }

private:
  static auto make_unique_field_names(std::vector<ParsedField>& fields,
                                      std::string_view prefix) -> void {
    auto used = std::unordered_map<std::string, size_t>{};
    for (auto i = size_t{0}; i < fields.size(); ++i) {
      auto base = fields[i].name;
      if (base.empty()) {
        base = fmt::format("{}{}", prefix, i);
      }
      auto count = used[base];
      if (count == 0) {
        used[base] = 1;
        fields[i].name = std::move(base);
        continue;
      }
      auto candidate = std::string{};
      do {
        ++count;
        candidate = fmt::format("{}_{}", base, count);
      } while (used.contains(candidate));
      used[base] = count;
      used[candidate] = 1;
      fields[i].name = std::move(candidate);
    }
  }

  static auto format_int128(::clickhouse::Int128 value) -> std::string {
    auto result = std::string{};
    auto negative = value < 0;
    auto magnitude = absl::uint128{};
    if (negative) {
      magnitude = static_cast<absl::uint128>(-(value + 1));
      ++magnitude;
    } else {
      magnitude = static_cast<absl::uint128>(value);
    }
    while (magnitude != 0) {
      auto digit = static_cast<uint8_t>(magnitude % absl::uint128{10});
      result += static_cast<char>('0' + digit);
      magnitude /= absl::uint128{10};
    }
    if (result.empty()) {
      result = "0";
    } else if (negative) {
      result.push_back('-');
    }
    std::reverse(result.begin(), result.end());
    return result;
  }

  static auto format_scaled_integer(std::string digits, ParsedType const& desc)
    -> std::string {
    auto scale = desc.decimal_scale();
    auto negative = false;
    if (not digits.empty() and digits.front() == '-') {
      negative = true;
      digits.erase(digits.begin());
    }
    if (scale > 0) {
      if (digits.size() <= scale) {
        digits.insert(0, scale - digits.size() + 1, '0');
      }
      digits.insert(digits.size() - scale, 1, '.');
    }
    if (negative) {
      digits.insert(digits.begin(), '-');
    }
    return digits;
  }

  static auto format_uuid(::clickhouse::UUID value) -> std::string {
    return fmt::format("{:08x}-{:04x}-{:04x}-{:04x}-{:012x}", value.first >> 32,
                       (value.first >> 16) & 0xffff, value.first & 0xffff,
                       value.second >> 48, value.second & 0xffffffffffffULL);
  }

  static auto pow10(size_t exponent) -> int64_t {
    auto result = int64_t{1};
    for (auto i = size_t{0}; i < exponent; ++i) {
      result *= 10;
    }
    return result;
  }

  static auto
  check_time_nanos_range(::clickhouse::Int128 value, std::string_view type_name,
                         value_path path, diagnostic_handler& dh)
    -> failure_or<int64_t> {
    auto min = ::clickhouse::Int128{std::numeric_limits<int64_t>::min()};
    auto max = ::clickhouse::Int128{std::numeric_limits<int64_t>::max()};
    if (value < min or value > max) {
      diagnostic::error("{} value for `{}` is out of range after rescaling to "
                        "nanoseconds",
                        type_name, path)
        .emit(dh);
      return failure::promise();
    }
    return static_cast<int64_t>(value);
  }

  static auto
  rescale_datetime64_to_nanos(int64_t value, ParsedType const& desc,
                              value_path path, diagnostic_handler& dh)
    -> failure_or<int64_t> {
    auto precision = desc.time_precision();
    if (precision == 9) {
      return value;
    }
    if (precision < 9) {
      auto factor = ::clickhouse::Int128{pow10(9 - precision)};
      return check_time_nanos_range(::clickhouse::Int128{value} * factor,
                                    "DateTime64", path, dh);
    }
    return value / pow10(precision - 9);
  }

  static auto
  time_from_unix_seconds(int64_t seconds, std::string_view type_name,
                         value_path path, diagnostic_handler& dh)
    -> failure_or<time> {
    TRY(auto nanos,
        check_time_nanos_range(::clickhouse::Int128{seconds}
                                 * ::clickhouse::Int128{1'000'000'000},
                               type_name, path, dh));
    return time{
      std::chrono::duration_cast<duration>(std::chrono::nanoseconds{nanos})};
  }

  static auto time_from_unix_days(int64_t days, std::string_view type_name,
                                  value_path path, diagnostic_handler& dh)
    -> failure_or<time> {
    auto seconds = ::clickhouse::Int128{days} * ::clickhouse::Int128{86400};
    TRY(auto nanos,
        check_time_nanos_range(seconds * ::clickhouse::Int128{1'000'000'000},
                               type_name, path, dh));
    return time{
      std::chrono::duration_cast<duration>(std::chrono::nanoseconds{nanos})};
  }

  static auto is_blob_type(ParsedType const& desc) -> bool {
    auto child = std::get_if<ParsedType::ListChild>(&desc.payload);
    return desc.code == ::clickhouse::Type::Array and child
           and not child->child->nullable
           and child->child->code == ::clickhouse::Type::UInt8;
  }

  static auto is_scalar_type(ParsedType const& desc) -> bool {
    return desc.code != ::clickhouse::Type::Tuple
           and desc.code != ::clickhouse::Type::Array;
  }

  static auto
  unsupported_type_diagnostic(value_path path, std::string_view text)
    -> diagnostic_builder {
    return diagnostic::error("ClickHouse column `{}` has unsupported "
                             "ClickHouse type `{}`",
                             path, text);
  }

  static auto parse_clickhouse_type(std::string_view text, value_path path,
                                    diagnostic_handler& dh)
    -> failure_or<ParsedType> {
    auto malformed = [&]() -> failure {
      diagnostic::error("ClickHouse column `{}` has malformed type `{}`", path,
                        text)
        .emit(dh);
      return failure::promise();
    };
    text = detail::trim(text);
    if (text.empty()) {
      diagnostic::error("ClickHouse column `{}` has an empty type name", path)
        .emit(dh);
      return failure::promise();
    }
    if (auto inner = unwrap_clickhouse_type_call(text, "LowCardinality")) {
      return parse_clickhouse_type(*inner, path, dh);
    }
    if (auto inner
        = unwrap_clickhouse_type_call(text, "SimpleAggregateFunction")) {
      auto parts = split_top_level_clickhouse_type_arguments(*inner);
      if (parts.size() != 2) {
        return malformed();
      }
      return parse_clickhouse_type(parts[1], path, dh);
    }
    if (auto inner = unwrap_clickhouse_type_call(text, "Nullable")) {
      TRY(auto nested, parse_clickhouse_type(*inner, path, dh));
      nested.nullable = true;
      return nested;
    }
    if (auto inner = unwrap_clickhouse_type_call(text, "Array")) {
      TRY(auto nested, parse_clickhouse_type(*inner, path.list(), dh));
      auto result = ParsedType{.code = ::clickhouse::Type::Array};
      if (nested.code == ::clickhouse::Type::UInt8 and not nested.nullable) {
        result.tenzir_type = type{blob_type{}};
      } else {
        result.tenzir_type = type{list_type{nested.tenzir_type}};
      }
      result.payload = ParsedType::ListChild{
        .child = Box<ParsedType>{std::in_place, std::move(nested)},
      };
      return result;
    }
    if (auto inner = unwrap_clickhouse_type_call(text, "Tuple")) {
      auto result = ParsedType{.code = ::clickhouse::Type::Tuple};
      result.payload = ParsedType::RecordFields{};
      auto parts = split_top_level_clickhouse_type_arguments(*inner);
      result.record_fields().reserve(parts.size());
      for (auto i = size_t{0}; i < parts.size(); ++i) {
        auto part = detail::trim(parts[i]);
        auto split = find_top_level_clickhouse_type_space(part);
        auto field_name = std::string{};
        auto field_type = std::string_view{};
        if (split == std::string_view::npos) {
          field_name = fmt::format("field{}", i);
          field_type = part;
        } else {
          field_name
            = unquote_identifier_component(detail::trim(part.substr(0, split)));
          field_type = detail::trim(part.substr(split + 1));
        }
        TRY(auto nested,
            parse_clickhouse_type(field_type, path.field(field_name), dh));
        result.record_fields().push_back({
          .name = std::move(field_name),
          .value = Box<ParsedType>{std::in_place, std::move(nested)},
        });
      }
      make_unique_field_names(result.record_fields(), "field");
      if (result.record_fields().empty()) {
        diagnostic::error("ClickHouse column `{}` is an empty tuple, which "
                          "is not supported",
                          path)
          .emit(dh);
        return failure::promise();
      }
      auto fields = std::vector<struct record_type::field>{};
      fields.reserve(result.record_fields().size());
      for (auto const& field : result.record_fields()) {
        fields.emplace_back(field.name, field.value->tenzir_type);
      }
      result.tenzir_type = type{record_type{fields}};
      return result;
    }
    if (text == "Nothing" or text == "Null") {
      auto result = ParsedType{};
      result.tenzir_type = type{null_type{}};
      return result;
    }
    if (text == "Bool") {
      return ParsedType{
        .code = ::clickhouse::Type::UInt8,
        .tenzir_type = type{bool_type{}},
      };
    }
    if (text == "Int8") {
      return ParsedType{
        .code = ::clickhouse::Type::Int8,
        .tenzir_type = type{int64_type{}},
      };
    }
    if (text == "Int16") {
      return ParsedType{
        .code = ::clickhouse::Type::Int16,
        .tenzir_type = type{int64_type{}},
      };
    }
    if (text == "Int32") {
      return ParsedType{
        .code = ::clickhouse::Type::Int32,
        .tenzir_type = type{int64_type{}},
      };
    }
    if (text == "Int64") {
      return ParsedType{
        .code = ::clickhouse::Type::Int64,
        .tenzir_type = type{int64_type{}},
      };
    }
    if (text == "UInt8") {
      return ParsedType{
        .code = ::clickhouse::Type::UInt8,
        .tenzir_type = type{uint64_type{}},
      };
    }
    if (text == "UInt16") {
      return ParsedType{
        .code = ::clickhouse::Type::UInt16,
        .tenzir_type = type{uint64_type{}},
      };
    }
    if (text == "UInt32") {
      return ParsedType{
        .code = ::clickhouse::Type::UInt32,
        .tenzir_type = type{uint64_type{}},
      };
    }
    if (text == "UInt64") {
      return ParsedType{
        .code = ::clickhouse::Type::UInt64,
        .tenzir_type = type{uint64_type{}},
      };
    }
    if (text == "Float32") {
      return ParsedType{
        .code = ::clickhouse::Type::Float32,
        .tenzir_type = type{double_type{}},
      };
    }
    if (text == "Float64") {
      return ParsedType{
        .code = ::clickhouse::Type::Float64,
        .tenzir_type = type{double_type{}},
      };
    }
    if (text == "String") {
      return ParsedType{
        .code = ::clickhouse::Type::String,
        .tenzir_type = type{string_type{}},
      };
    }
    if (text.starts_with("FixedString(")) {
      return ParsedType{
        .code = ::clickhouse::Type::FixedString,
        .tenzir_type = type{string_type{}},
      };
    }
    if (text == "UUID") {
      return ParsedType{
        .code = ::clickhouse::Type::UUID,
        .tenzir_type = type{string_type{}},
      };
    }
    if (text == "Int128") {
      return ParsedType{
        .code = ::clickhouse::Type::Int128,
        .tenzir_type = type{string_type{}},
      };
    }
    if (text.starts_with("Enum8(")) {
      return ParsedType{
        .code = ::clickhouse::Type::Enum8,
        .tenzir_type = type{string_type{}},
      };
    }
    if (text.starts_with("Enum16(")) {
      return ParsedType{
        .code = ::clickhouse::Type::Enum16,
        .tenzir_type = type{string_type{}},
      };
    }
    if (text == "Date" or text == "Date32" or text == "DateTime"
        or text.starts_with("DateTime(")) {
      auto code = ::clickhouse::Type::DateTime;
      if (text == "Date") {
        code = ::clickhouse::Type::Date;
      } else if (text == "Date32") {
        code = ::clickhouse::Type::Date32;
      }
      return ParsedType{
        .code = code,
        .tenzir_type = type{time_type{}},
      };
    }
    if (text.starts_with("DateTime64(")) {
      auto inner = unwrap_clickhouse_type_call(text, "DateTime64");
      TENZIR_ASSERT(inner);
      auto parts = split_top_level_clickhouse_type_arguments(*inner);
      if (parts.empty()) {
        return malformed();
      }
      auto precision = size_t{0};
      if (not parse_clickhouse_size(parts[0], precision)) {
        return malformed();
      }
      return ParsedType{
        .code = ::clickhouse::Type::DateTime64,
        .tenzir_type = type{time_type{}},
        .payload = ParsedType::TimePrecision{.value = precision},
      };
    }
    if (text == "IPv4") {
      return ParsedType{
        .code = ::clickhouse::Type::IPv4,
        .tenzir_type = type{ip_type{}},
      };
    }
    if (text == "IPv6") {
      return ParsedType{
        .code = ::clickhouse::Type::IPv6,
        .tenzir_type = type{ip_type{}},
      };
    }
    if (text.starts_with("Decimal(")) {
      auto inner = unwrap_clickhouse_type_call(text, "Decimal");
      TENZIR_ASSERT(inner);
      auto parts = split_top_level_clickhouse_type_arguments(*inner);
      if (parts.size() != 2) {
        return malformed();
      }
      auto precision = size_t{0};
      auto scale = size_t{0};
      if (not parse_clickhouse_size(parts[0], precision)
          or not parse_clickhouse_size(parts[1], scale)) {
        return malformed();
      }
      if (precision > 38) {
        unsupported_type_diagnostic(path, text)
          .hint("Decimal precisions above 38 are currently not supported")
          .emit(dh);
        return failure::promise();
      }
      return ParsedType{
        .code = ::clickhouse::Type::Decimal,
        .tenzir_type = type{string_type{}},
        .payload = ParsedType::DecimalScale{.value = scale},
      };
    }
    if (text.starts_with("Decimal256(")) {
      unsupported_type_diagnostic(path, text)
        .hint("Decimal256 values are currently not supported")
        .emit(dh);
      return failure::promise();
    }
    if (text.starts_with("Decimal32(") or text.starts_with("Decimal64(")
        or text.starts_with("Decimal128(")) {
      auto prefix = std::string_view{};
      if (text.starts_with("Decimal32(")) {
        prefix = "Decimal32";
      } else if (text.starts_with("Decimal64(")) {
        prefix = "Decimal64";
      } else {
        prefix = "Decimal128";
      }
      auto inner = unwrap_clickhouse_type_call(text, prefix);
      TENZIR_ASSERT(inner);
      auto scale = size_t{0};
      if (not parse_clickhouse_size(*inner, scale)) {
        return malformed();
      }
      auto code = ::clickhouse::Type::Decimal128;
      if (prefix == "Decimal32") {
        code = ::clickhouse::Type::Decimal32;
      } else if (prefix == "Decimal64") {
        code = ::clickhouse::Type::Decimal64;
      }
      return ParsedType{
        .code = code,
        .tenzir_type = type{string_type{}},
        .payload = ParsedType::DecimalScale{.value = scale},
      };
    }
    if (text.starts_with("Map(")) {
      unsupported_type_diagnostic(path, text)
        .hint("maps are currently not supported")
        .emit(dh);
      return failure::promise();
    }
    unsupported_type_diagnostic(path, text)
      .hint("cast unsupported columns in SQL or omit them from the result")
      .emit(dh);
    return failure::promise();
  }

  static auto
  make_query_schema(std::string name, std::vector<ParsedField> columns,
                    diagnostic_handler& dh) -> failure_or<QuerySchema> {
    if (columns.empty()) {
      diagnostic::error("ClickHouse query returned zero columns").emit(dh);
      return failure::promise();
    }
    make_unique_field_names(columns, "column");
    return QuerySchema{
      .name = std::move(name),
      .columns = std::move(columns),
    };
  }

  static auto
  infer_schema_from_block(::clickhouse::Block const& block,
                          std::string schema_name, diagnostic_handler& dh)
    -> failure_or<QuerySchema> {
    auto columns = std::vector<ParsedField>{};
    columns.reserve(block.GetColumnCount());
    for (auto i = size_t{0}; i < block.GetColumnCount(); ++i) {
      auto type_name
        = remove_non_significant_whitespace(block[i]->Type()->GetName());
      auto name = std::string{block.GetColumnName(i)};
      TRY(auto parsed,
          parse_clickhouse_type(type_name, value_path{}.field(name), dh));
      columns.push_back({
        .name = std::move(name),
        .value = Box<ParsedType>{std::in_place, std::move(parsed)},
      });
    }
    return make_query_schema(std::move(schema_name), std::move(columns), dh);
  }

  static auto
  describe_table_schema(::clickhouse::Client& client, std::string_view table,
                        std::string schema_name, diagnostic_handler& dh)
    -> failure_or<QuerySchema> {
    auto columns = std::vector<ParsedField>{};
    auto failed = false;
    auto query = ::clickhouse::Query{fmt::format(
      "DESCRIBE TABLE {} SETTINGS describe_compact_output=1", table)};
    query.OnDataCancelable([&](::clickhouse::Block const& block) {
      if (block.GetColumnCount() == 0) {
        return true;
      }
      if (block.GetColumnCount() < 2) {
        diagnostic::error("unexpected ClickHouse response to DESCRIBE TABLE")
          .emit(dh);
        failed = true;
        return false;
      }
      auto names = block[0]->As<::clickhouse::ColumnString>();
      auto types = block[1]->As<::clickhouse::ColumnString>();
      if (not names or not types) {
        diagnostic::error("unexpected ClickHouse DESCRIBE TABLE column layout")
          .emit(dh);
        failed = true;
        return false;
      }
      for (auto row = size_t{0}; row < block.GetRowCount(); ++row) {
        auto name = std::string{names->At(row)};
        auto type_name = remove_non_significant_whitespace(types->At(row));
        auto parsed
          = parse_clickhouse_type(type_name, value_path{}.field(name), dh);
        if (not parsed) {
          failed = true;
          return false;
        }
        columns.push_back({
          .name = std::move(name),
          .value = Box<ParsedType>{std::in_place, std::move(*parsed)},
        });
      }
      return true;
    });
    client.Select(query);
    if (failed) {
      return failure::promise();
    }
    return make_query_schema(std::move(schema_name), std::move(columns), dh);
  }

  static auto append_decoded_scalar(builder_ref builder,
                                    ::clickhouse::ColumnRef const& column,
                                    size_t row, ParsedType const& desc,
                                    value_path path, diagnostic_handler& dh)
    -> failure_or<void> {
    if (desc.code == ::clickhouse::Type::Decimal
        or desc.code == ::clickhouse::Type::Decimal32
        or desc.code == ::clickhouse::Type::Decimal64
        or desc.code == ::clickhouse::Type::Decimal128) {
      if (auto decimal = column->As<::clickhouse::ColumnDecimal>()) {
        builder.data(
          format_scaled_integer(format_int128(decimal->At(row)), desc));
        return {};
      }
      auto bytes = column->GetItem(row).AsBinaryData();
      switch (bytes.size()) {
        case 4: {
          auto value = int32_t{0};
          std::memcpy(&value, bytes.data(), sizeof(value));
          builder.data(format_scaled_integer(std::to_string(value), desc));
          return {};
        }
        case 8: {
          auto value = int64_t{0};
          std::memcpy(&value, bytes.data(), sizeof(value));
          builder.data(format_scaled_integer(std::to_string(value), desc));
          return {};
        }
        case 16: {
          auto value = ::clickhouse::Int128{};
          std::memcpy(&value, bytes.data(), sizeof(value));
          builder.data(format_scaled_integer(format_int128(value), desc));
          return {};
        }
        default:
          break;
      }
      diagnostic::error("expected decimal ClickHouse type for `{}`", path)
        .emit(dh);
      return failure::promise();
    }
    if (desc.code == ::clickhouse::Type::IPv4
        or desc.code == ::clickhouse::Type::IPv6) {
      if (auto ipv4 = column->As<::clickhouse::ColumnIPv4>()) {
        auto text = ipv4->AsString(row);
        auto addr = in_addr{};
        if (inet_pton(AF_INET, text.c_str(), &addr) == 1) {
          auto view = std::span<const uint8_t, 4>{
            reinterpret_cast<const uint8_t*>(&addr), 4};
          builder.data(ip::v4(view));
          return {};
        }
      }
      if (auto ipv6 = column->As<::clickhouse::ColumnIPv6>()) {
        auto text = ipv6->AsString(row);
        auto addr = in6_addr{};
        if (inet_pton(AF_INET6, text.c_str(), &addr) == 1) {
          auto view = std::span<const uint8_t, 16>{
            reinterpret_cast<const uint8_t*>(&addr), 16};
          builder.data(ip::v6(view));
          return {};
        }
      }
      auto bytes = column->GetItem(row).AsBinaryData();
      if (bytes.size() == 4) {
        auto view = std::span<const uint8_t, 4>{
          reinterpret_cast<const uint8_t*>(bytes.data()), 4};
        builder.data(ip::v4(view));
        return {};
      }
      if (bytes.size() == 16) {
        auto view = std::span<const uint8_t, 16>{
          reinterpret_cast<const uint8_t*>(bytes.data()), 16};
        builder.data(ip::v6(view));
        return {};
      }
      diagnostic::error("expected IP ClickHouse type for `{}`", path).emit(dh);
      return failure::promise();
    }
    auto item = column->GetItem(row);
    switch (desc.code) {
      case ::clickhouse::Type::Void:
        builder.null();
        return {};
      case ::clickhouse::Type::UInt8:
        if (desc.tenzir_type == type{bool_type{}}) {
          builder.data(item.get<uint8_t>() != 0);
          return {};
        }
        builder.data(uint64_t{item.get<uint8_t>()});
        return {};
      case ::clickhouse::Type::UInt16:
        builder.data(uint64_t{item.get<uint16_t>()});
        return {};
      case ::clickhouse::Type::UInt32:
        builder.data(uint64_t{item.get<uint32_t>()});
        return {};
      case ::clickhouse::Type::UInt64:
        builder.data(item.get<uint64_t>());
        return {};
      case ::clickhouse::Type::Int8:
        builder.data(int64_t{item.get<int8_t>()});
        return {};
      case ::clickhouse::Type::Int16:
        builder.data(int64_t{item.get<int16_t>()});
        return {};
      case ::clickhouse::Type::Int32:
        builder.data(int64_t{item.get<int32_t>()});
        return {};
      case ::clickhouse::Type::Int64:
        builder.data(item.get<int64_t>());
        return {};
      case ::clickhouse::Type::Float32:
        builder.data(double{item.get<float>()});
        return {};
      case ::clickhouse::Type::Float64:
        builder.data(item.get<double>());
        return {};
      case ::clickhouse::Type::String:
      case ::clickhouse::Type::FixedString:
        builder.data(std::string{item.get<std::string_view>()});
        return {};
      case ::clickhouse::Type::UUID: {
        auto data = item.AsBinaryData();
        auto first = uint64_t{0};
        auto second = uint64_t{0};
        std::memcpy(&first, data.data(), sizeof(first));
        std::memcpy(&second, data.data() + sizeof(first), sizeof(second));
        builder.data(format_uuid(::clickhouse::UUID{first, second}));
        return {};
      }
      case ::clickhouse::Type::Enum8:
      case ::clickhouse::Type::Enum16:
        if (auto enum8 = column->As<::clickhouse::ColumnEnum8>()) {
          builder.data(std::string{enum8->NameAt(row)});
          return {};
        }
        if (auto enum16 = column->As<::clickhouse::ColumnEnum16>()) {
          builder.data(std::string{enum16->NameAt(row)});
          return {};
        }
        if (item.type == ::clickhouse::Type::Enum8) {
          builder.data(std::to_string(item.get<int8_t>()));
          return {};
        }
        if (item.type == ::clickhouse::Type::Enum16) {
          builder.data(std::to_string(item.get<int16_t>()));
          return {};
        }
        diagnostic::error("expected enum ClickHouse type for `{}`", path)
          .emit(dh);
        return failure::promise();
      case ::clickhouse::Type::Int128:
        builder.data(format_int128(item.get<::clickhouse::Int128>()));
        return {};
      case ::clickhouse::Type::Date: {
        auto days = int64_t{item.get<uint16_t>()};
        TRY(auto value, time_from_unix_days(days, "Date", path, dh));
        builder.data(value);
        return {};
      }
      case ::clickhouse::Type::Date32: {
        auto days = int64_t{item.get<int32_t>()};
        TRY(auto value, time_from_unix_days(days, "Date32", path, dh));
        builder.data(value);
        return {};
      }
      case ::clickhouse::Type::DateTime: {
        TRY(auto value,
            time_from_unix_seconds(item.get<uint32_t>(), "DateTime", path, dh));
        builder.data(value);
        return {};
      }
      case ::clickhouse::Type::DateTime64: {
        TRY(auto ns,
            rescale_datetime64_to_nanos(item.get<int64_t>(), desc, path, dh));
        builder.data(time{
          std::chrono::duration_cast<duration>(std::chrono::nanoseconds{ns})});
        return {};
      }
      case ::clickhouse::Type::IPv4:
      case ::clickhouse::Type::IPv6:
      case ::clickhouse::Type::Decimal:
      case ::clickhouse::Type::Decimal32:
      case ::clickhouse::Type::Decimal64:
      case ::clickhouse::Type::Decimal128:
        TENZIR_UNREACHABLE();
      case ::clickhouse::Type::Nullable:
      case ::clickhouse::Type::Array:
      case ::clickhouse::Type::Tuple:
      case ::clickhouse::Type::LowCardinality:
      case ::clickhouse::Type::Map:
      case ::clickhouse::Type::Point:
      case ::clickhouse::Type::Ring:
      case ::clickhouse::Type::Polygon:
      case ::clickhouse::Type::MultiPolygon:
        break;
    }
    diagnostic::error("unexpected ClickHouse type mapping for `{}`", path)
      .emit(dh);
    return failure::promise();
  }

  static auto append_decoded_value(builder_ref builder,
                                   ::clickhouse::ColumnRef const& column,
                                   size_t row, ParsedType const& desc,
                                   value_path path, diagnostic_handler& dh)
    -> failure_or<void> {
    auto effective_column = column;
    if (desc.nullable) {
      if (auto nullable = column->As<::clickhouse::ColumnNullable>()) {
        if (nullable->IsNull(row)) {
          builder.null();
          return {};
        }
        effective_column = nullable->Nested();
      } else if (is_scalar_type(desc)
                 and column->GetItem(row).type == ::clickhouse::Type::Void) {
        builder.null();
        return {};
      }
    }
    if (desc.code == ::clickhouse::Type::Tuple) {
      auto tuple = effective_column->As<::clickhouse::ColumnTuple>();
      if (not tuple) {
        diagnostic::error("expected tuple ClickHouse type for `{}`", path)
          .emit(dh);
        return failure::promise();
      }
      if (tuple->TupleSize() != desc.record_fields().size()) {
        diagnostic::error("unexpected tuple size for `{}`", path).emit(dh);
        return failure::promise();
      }
      auto record = builder.record();
      for (auto i = size_t{0}; i < desc.record_fields().size(); ++i) {
        auto const& field = desc.record_fields()[i];
        TRY(append_decoded_value(record.field(field.name), tuple->At(i), row,
                                 *field.value, path.field(field.name), dh));
      }
      return {};
    }
    if (desc.code == ::clickhouse::Type::Array) {
      auto array = effective_column->As<::clickhouse::ColumnArray>();
      if (not array) {
        diagnostic::error("expected array ClickHouse type for `{}`", path)
          .emit(dh);
        return failure::promise();
      }
      if (is_blob_type(desc)) {
        auto nested = array->GetAsColumn(row);
        auto bytes = nested->As<::clickhouse::ColumnUInt8>();
        if (not bytes) {
          diagnostic::error("expected Array(UInt8) for `{}`", path).emit(dh);
          return failure::promise();
        }
        auto value = blob{};
        value.reserve(bytes->Size());
        for (auto i = size_t{0}; i < bytes->Size(); ++i) {
          value.push_back(static_cast<std::byte>(bytes->At(i)));
        }
        builder.data(std::move(value));
        return {};
      }
      auto& child = desc.list_child();
      auto nested = array->GetAsColumn(row);
      auto list = builder.list();
      for (auto i = size_t{0}; i < nested->Size(); ++i) {
        TRY(append_decoded_value(list, nested, i, child, path.list(), dh));
      }
      return {};
    }
    return append_decoded_scalar(builder, effective_column, row, desc, path,
                                 dh);
  }

  static auto
  build_scalar_series(::clickhouse::ColumnRef const& column,
                      ParsedType const& desc, value_path path,
                      diagnostic_handler& dh,
                      std::shared_ptr<::clickhouse::ColumnNullable> nullable
                      = nullptr) -> failure_or<series> {
    auto builder = series_builder{desc.tenzir_type};
    auto field = builder_ref{builder};
    for (auto row = size_t{0}; row < column->Size(); ++row) {
      if (nullable and nullable->IsNull(row)) {
        field.null();
        continue;
      }
      TRY(append_decoded_scalar(field, column, row, desc, path, dh));
    }
    return builder.finish_assert_one_array();
  }

  static auto
  build_blob_series(::clickhouse::ColumnRef const& column,
                    ParsedType const& desc, value_path path,
                    diagnostic_handler& dh,
                    std::shared_ptr<::clickhouse::ColumnNullable> nullable
                    = nullptr) -> failure_or<series> {
    auto array = column->As<::clickhouse::ColumnArray>();
    if (not array) {
      diagnostic::error("expected byte array ClickHouse type for `{}`", path)
        .emit(dh);
      return failure::promise();
    }
    auto builder = series_builder{desc.tenzir_type};
    auto field = builder_ref{builder};
    for (auto row = size_t{0}; row < column->Size(); ++row) {
      if (nullable and nullable->IsNull(row)) {
        field.null();
        continue;
      }
      auto nested = array->GetAsColumn(row);
      auto bytes = nested->As<::clickhouse::ColumnUInt8>();
      if (not bytes) {
        diagnostic::error("expected Array(UInt8) for `{}`", path).emit(dh);
        return failure::promise();
      }
      auto value = blob{};
      value.reserve(bytes->Size());
      for (auto i = size_t{0}; i < bytes->Size(); ++i) {
        value.push_back(static_cast<std::byte>(bytes->At(i)));
      }
      field.data(std::move(value));
    }
    return builder.finish_assert_one_array();
  }

  static auto
  build_record_series(::clickhouse::ColumnRef const& column,
                      ParsedType const& desc, value_path path,
                      diagnostic_handler& dh,
                      std::shared_ptr<::clickhouse::ColumnNullable> nullable
                      = nullptr) -> failure_or<series> {
    auto tuple = column->As<::clickhouse::ColumnTuple>();
    if (not tuple) {
      diagnostic::error("expected tuple ClickHouse type for `{}`", path)
        .emit(dh);
      return failure::promise();
    }
    if (tuple->TupleSize() != desc.record_fields().size()) {
      diagnostic::error("unexpected tuple size for `{}`", path).emit(dh);
      return failure::promise();
    }
    auto builder = series_builder{desc.tenzir_type};
    auto field = builder_ref{builder};
    for (auto row = size_t{0}; row < column->Size(); ++row) {
      if (nullable and nullable->IsNull(row)) {
        field.null();
        continue;
      }
      auto record = field.record();
      for (auto i = size_t{0}; i < desc.record_fields().size(); ++i) {
        auto const& nested_field = desc.record_fields()[i];
        TRY(append_decoded_value(record.field(nested_field.name), tuple->At(i),
                                 row, *nested_field.value,
                                 path.field(nested_field.name), dh));
      }
    }
    return builder.finish_assert_one_array();
  }

  static auto
  build_list_series(::clickhouse::ColumnRef const& column,
                    ParsedType const& desc, value_path path,
                    diagnostic_handler& dh,
                    std::shared_ptr<::clickhouse::ColumnNullable> nullable
                    = nullptr) -> failure_or<series> {
    auto array = column->As<::clickhouse::ColumnArray>();
    if (not array) {
      diagnostic::error("expected array ClickHouse type for `{}`", path)
        .emit(dh);
      return failure::promise();
    }
    auto builder = series_builder{desc.tenzir_type};
    auto field = builder_ref{builder};
    for (auto row = size_t{0}; row < column->Size(); ++row) {
      if (nullable and nullable->IsNull(row)) {
        field.null();
        continue;
      }
      auto nested = array->GetAsColumn(row);
      auto list = field.list();
      auto const& child = desc.list_child();
      for (auto i = size_t{0}; i < nested->Size(); ++i) {
        TRY(append_decoded_value(list, nested, i, child, path.list(), dh));
      }
    }
    return builder.finish_assert_one_array();
  }

  static auto ensure_nullable_column(::clickhouse::ColumnRef const& column,
                                     ParsedType const& desc)
    -> std::shared_ptr<::clickhouse::ColumnNullable> {
    if (auto nullable = column->As<::clickhouse::ColumnNullable>()) {
      return nullable;
    }
    auto nulls = std::make_shared<::clickhouse::ColumnUInt8>();
    nulls->Reserve(column->Size());
    auto detect_voids = is_scalar_type(desc);
    for (auto row = size_t{0}; row < column->Size(); ++row) {
      auto is_null = false;
      if (detect_voids) {
        is_null = column->GetItem(row).type == ::clickhouse::Type::Void;
      }
      nulls->Append(is_null ? 1 : 0);
    }
    return std::make_shared<::clickhouse::ColumnNullable>(column, nulls);
  }

  static auto
  build_series(::clickhouse::ColumnRef const& column, ParsedType const& desc,
               value_path path, diagnostic_handler& dh) -> failure_or<series> {
    auto effective_column = column;
    auto nullable = std::shared_ptr<::clickhouse::ColumnNullable>{};
    if (desc.nullable) {
      nullable = ensure_nullable_column(column, desc);
      effective_column = nullable->Nested();
    }
    if (desc.code == ::clickhouse::Type::Tuple) {
      return build_record_series(effective_column, desc, path, dh, nullable);
    }
    if (desc.code == ::clickhouse::Type::Array) {
      if (is_blob_type(desc)) {
        return build_blob_series(effective_column, desc, path, dh, nullable);
      }
      return build_list_series(effective_column, desc, path, dh, nullable);
    }
    return build_scalar_series(effective_column, desc, path, dh, nullable);
  }

  static auto make_runtime_schema(QuerySchema const& schema)
    -> failure_or<type> {
    auto fields = std::vector<struct record_type::field>{};
    fields.reserve(schema.columns.size());
    for (auto const& column : schema.columns) {
      fields.emplace_back(column.name, column.value->tenzir_type);
    }
    return type{schema.name, record_type{fields}};
  }

  static auto block_to_slices(::clickhouse::Block const& block,
                              QuerySchema const& schema, diagnostic_handler& dh)
    -> failure_or<std::vector<table_slice>> {
    if (block.GetColumnCount() != schema.columns.size()) {
      diagnostic::error("ClickHouse query changed its column count "
                        "unexpectedly")
        .emit(dh);
      return failure::promise();
    }
    TRY(auto runtime_schema, make_runtime_schema(schema));
    auto arrays = std::vector<std::shared_ptr<arrow::Array>>{};
    arrays.reserve(schema.columns.size());
    for (auto column = size_t{0}; column < schema.columns.size(); ++column) {
      TRY(auto result,
          build_series(block[column], *schema.columns[column].value,
                       value_path{}.field(schema.columns[column].name), dh));
      arrays.push_back(std::move(result.array));
    }
    auto batch = arrow::RecordBatch::Make(runtime_schema.to_arrow_schema(),
                                          block.GetRowCount(), arrays);
    return std::vector<table_slice>{
      table_slice{std::move(batch), std::move(runtime_schema)}};
  }

  static auto split_validated_table_name(std::string_view table)
    -> split_table_name_result {
    if (auto split = table_name_quoting.split_at_unquoted(table, '.')) {
      return {split->first, split->second};
    }
    return {None{}, table};
  }

  static auto make_select_query(std::string_view table,
                                std::vector<ParsedField> const& columns)
    -> std::string {
    auto result = std::string{"SELECT "};
    for (auto i = size_t{0}; i < columns.size(); ++i) {
      if (i != 0) {
        result += ", ";
      }
      result += quote_identifier_component(columns[i].name);
    }
    result += fmt::format(" FROM {}", table);
    return result;
  }

  static auto make_schema_name_from_table(std::string_view table)
    -> std::string {
    auto split = split_validated_table_name(table);
    auto table_name = unquote_identifier_component(split.table);
    if (split.database) {
      return fmt::format("clickhouse.{}.{}",
                         unquote_identifier_component(*split.database),
                         table_name);
    }
    return fmt::format("clickhouse.{}", table_name);
  }

  static auto make_query_plan(FromClickhouseArgs const& args, OpCtx& ctx)
    -> Option<QueryPlan> {
    if (args.sql) {
      return QueryPlan{
        .query = args.sql->inner,
        .schema_name = "clickhouse.query",
        .described_table = None{},
      };
    }
    if (not args.table) {
      diagnostic::error("no query specified")
        .hint("specify `table` or `sql`")
        .emit(ctx);
      return None{};
    }
    auto qualified = std::string{args.table->inner};
    return QueryPlan{
      .query = fmt::format("SELECT * FROM {}", qualified),
      .schema_name = make_schema_name_from_table(args.table->inner),
      .described_table = qualified,
    };
  }

  static auto enqueue_message(RuntimeState& runtime, Message message)
    -> Task<bool> {
    while (not runtime.stop_requested.load(std::memory_order_acquire)) {
      if (runtime.queue.try_enqueue(std::move(message))) {
        co_return true;
      }
      co_await sleep_for(message_queue_backoff);
    }
    co_return false;
  }

  auto run_query(::clickhouse::ClientOptions options, QueryPlan plan,
                 diagnostic_handler& dh) -> Task<void> {
    auto emitted_terminal_diagnostic = false;
    try {
      auto client = ::clickhouse::Client{std::move(options)};
      auto schema = Option<QuerySchema>{};
      if (plan.described_table) {
        auto described = describe_table_schema(client, *plan.described_table,
                                               plan.schema_name, dh);
        if (not described) {
          co_return;
        } else {
          schema = std::move(*described);
        }
      }
      auto failed = false;
      auto query_text = std::string{plan.query};
      if (plan.described_table) {
        TENZIR_ASSERT(schema);
        query_text = make_select_query(*plan.described_table, schema->columns);
      }
      auto query = ::clickhouse::Query{query_text};
      query.OnDataCancelable([&](::clickhouse::Block const& block) {
        if (runtime_->stop_requested.load(std::memory_order_acquire)) {
          return false;
        }
        if (block.GetColumnCount() == 0) {
          return not runtime_->stop_requested.load(std::memory_order_acquire);
        }
        if (not schema) {
          auto inferred = infer_schema_from_block(block, plan.schema_name, dh);
          if (not inferred) {
            failed = true;
            return false;
          }
          schema = std::move(*inferred);
        }
        if (block.GetRowCount() == 0) {
          return not runtime_->stop_requested.load(std::memory_order_acquire);
        }
        auto slices = block_to_slices(block, *schema, dh);
        if (not slices) {
          failed = true;
          return false;
        }
        for (auto& slice : *slices) {
          if (not folly::coro::blockingWait(enqueue_message(
                *runtime_, Message{SliceMessage{.slice = std::move(slice)}}))) {
            return false;
          }
        }
        return not runtime_->stop_requested.load(std::memory_order_acquire);
      });
      client.Select(query);
      if (failed) {
        co_return;
      }
      if (runtime_->stop_requested.load(std::memory_order_acquire)) {
        co_return;
      }
      co_await enqueue_message(*runtime_, Message{DoneMessage{}});
    } catch (const panic_exception&) {
      throw;
    } catch (const ::clickhouse::ServerError& e) {
      add_tls_client_diagnostic_hints(
        diagnostic::error("ClickHouse error {}: {}", e.GetCode(), e.what()),
        tls_enabled_, "ClickHouse", clickhouse_plaintext_port,
        clickhouse_tls_port)
        .emit(dh);
      emitted_terminal_diagnostic = true;
    } catch (const std::exception& e) {
      add_tls_client_diagnostic_hints(
        diagnostic::error("ClickHouse error: {}", e.what()), tls_enabled_,
        "ClickHouse", clickhouse_plaintext_port, clickhouse_tls_port)
        .emit(dh);
      emitted_terminal_diagnostic = true;
    }
    if (emitted_terminal_diagnostic) {
      co_return;
    }
    co_return;
  }

  FromClickhouseArgs args_;
  mutable Arc<RuntimeState> runtime_ = Arc<RuntimeState>{std::in_place};
  bool tls_enabled_ = false;
  bool done_ = false;
};

class Plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "from_clickhouse";
  }

  auto describe() const -> Description override {
    auto d = Describer<FromClickhouseArgs, FromClickhouse>{};
    auto uri_arg = d.named_optional("uri", &FromClickhouseArgs::uri);
    auto table_arg = d.named("table", &FromClickhouseArgs::table);
    auto host_arg = d.named_optional("host", &FromClickhouseArgs::host);
    auto port_arg = d.named("port", &FromClickhouseArgs::port);
    auto user_arg = d.named_optional("user", &FromClickhouseArgs::user);
    auto password_arg
      = d.named_optional("password", &FromClickhouseArgs::password);
    auto sql_arg = d.named("sql", &FromClickhouseArgs::sql);
    auto tls_validate
      = tls_options{}.add_to_describer(d, &FromClickhouseArgs::tls);
    d.operator_location(&FromClickhouseArgs::operator_location);
    d.validate([=, tls_validate
                   = std::move(tls_validate)](DescribeCtx& ctx) -> Empty {
      tls_validate(ctx);
      auto has_table = ctx.get(table_arg).has_value();
      auto has_sql = ctx.get(sql_arg).has_value();
      auto has_uri = ctx.get(uri_arg).has_value();
      auto has_host = ctx.get(host_arg).has_value();
      auto has_port = ctx.get(port_arg).has_value();
      auto has_user = ctx.get(user_arg).has_value();
      auto has_password = ctx.get(password_arg).has_value();
      if (has_uri and (has_host or has_port or has_user or has_password)) {
        diagnostic::error(
          "`uri` and explicit connection arguments are mutually exclusive")
          .primary(ctx.get_location(uri_arg).value_or(location::unknown))
          .emit(ctx);
        return {};
      }
      if (not has_table and not has_sql) {
        diagnostic::error("no query specified")
          .hint("specify `table` or `sql`")
          .emit(ctx);
        return {};
      }
      if (has_sql and has_table) {
        diagnostic::error("`sql` and `table` are mutually exclusive").emit(ctx);
        return {};
      }
      if (auto port = ctx.get(port_arg)) {
        if (port->inner == 0 or port->inner > 65535) {
          diagnostic::error("`port` must be between 1 and 65535")
            .primary(port->source, "got `{}`", port->inner)
            .emit(ctx);
        }
      }
      if (auto table = ctx.get(table_arg)) {
        if (not validate_table_name<true>(table->inner, table->source, ctx)) {
          return {};
        }
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::clickhouse

TENZIR_REGISTER_PLUGIN(tenzir::plugins::clickhouse::Plugin)
