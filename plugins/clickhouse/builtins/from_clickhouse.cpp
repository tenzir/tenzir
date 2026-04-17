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

namespace tenzir::plugins::clickhouse {

namespace {

constexpr auto clickhouse_plaintext_port = uint64_t{9000};
constexpr auto clickhouse_tls_port = uint64_t{9440};
inline const auto clickhouse_type_quoting = detail::quoting_escaping_policy{
  .quotes = R"('"`)",
  .doubled_quotes_escape = true,
};

auto unquote_identifier_component(std::string_view text) -> std::string {
  return table_name_quoting.unquote_unescape(text);
}

auto quote_identifier_component(std::string_view text) -> std::string {
  return fmt::format("\"{}\"", detail::double_escape(text, "\""));
}

struct FromClickhouseArgs {
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
  enum class Kind {
    null_,
    nullable_,
    bool_,
    int64_,
    uint64_,
    double_,
    string_,
    time_,
    ip_,
    blob_,
    uuid_,
    enum_name_,
    int128_string_,
    decimal_string_,
    record_,
    list_,
  };

  Kind kind = Kind::null_;
  std::vector<ParsedField> fields;
  Option<Box<ParsedType>> child = None{};
  size_t time_precision = 0;
  size_t decimal_scale = 0;
};

struct QuerySchema {
  std::string name;
  std::vector<ParsedField> columns;
  type schema;
};

struct SliceMessage {
  table_slice slice;
};

struct ErrorMessage {
  std::vector<diagnostic> diagnostics;
  bool add_tls_hints = false;
};

struct DoneMessage {};

using Message = variant<SliceMessage, ErrorMessage, DoneMessage>;
using MessageQueue = folly::coro::BoundedQueue<Message, true, true>;
constexpr auto message_queue_capacity = uint32_t{16};
constexpr auto message_queue_backoff = std::chrono::milliseconds{1};

auto has_primary_annotation(diagnostic const& diag) -> bool {
  return std::any_of(diag.annotations.begin(), diag.annotations.end(),
                     [](auto const& annotation) {
                       return annotation.primary;
                     });
}

auto make_error_message(collecting_diagnostic_handler&& dh,
                        std::string_view fallback, bool add_tls_hints = false)
  -> ErrorMessage {
  auto diagnostics = std::move(dh).collect();
  if (diagnostics.empty()) {
    diagnostics.push_back(diagnostic::error("{}", fallback).done());
  }
  return ErrorMessage{
    .diagnostics = std::move(diagnostics),
    .add_tls_hints = add_tls_hints,
  };
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
      done_ = true;
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
      .user = "",
      .password = "",
      .ssl = std::move(ssl),
      .table = {},
      .mode = {mode::append, location::unknown},
      .primary = None{},
      .operator_location = args_.operator_location,
    };
    auto requests = std::vector<secret_request>{
      make_secret_request("host", args_.host, client_args.host, ctx.dh()),
      make_secret_request("user", args_.user, client_args.user, ctx.dh()),
      make_secret_request("password", args_.password, client_args.password,
                          ctx.dh()),
    };
    auto ok = co_await ctx.resolve_secrets(std::move(requests));
    if (not ok) {
      done_ = true;
      co_return;
    }
    auto options = client_args.make_options(ctx.actor_system().config());
    ctx.spawn_task([this, options = std::move(options),
                    plan = std::move(*plan)]() mutable -> Task<void> {
      co_await run_query(std::move(options), std::move(plan));
    });
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    if (done_) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    co_return co_await runtime_->queue.dequeue();
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto message = std::move(result).as<Message>();
    co_await co_match(
      std::move(message),
      [&](SliceMessage x) -> Task<void> {
        co_await push(std::move(x.slice));
        co_return;
      },
      [&](ErrorMessage x) -> Task<void> {
        for (auto& diag : x.diagnostics) {
          auto add_primary = not has_primary_annotation(diag);
          auto builder = std::move(diag).modify();
          if (add_primary) {
            builder = std::move(builder).primary(args_.operator_location);
          }
          if (x.add_tls_hints) {
            add_tls_client_diagnostic_hints(std::move(builder), tls_enabled_,
                                            "ClickHouse",
                                            clickhouse_plaintext_port,
                                            clickhouse_tls_port)
              .emit(ctx);
          } else {
            std::move(builder).emit(ctx);
          }
        }
        done_ = true;
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
  static auto skip_quoted_token(std::string_view text, size_t& i) -> bool {
    if (not clickhouse_type_quoting.is_quote_character(text[i])) {
      return false;
    }
    if (auto closing = clickhouse_type_quoting.find_closing_quote(text, i);
        closing != std::string_view::npos) {
      i = closing;
    } else {
      i = text.size() - 1;
    }
    return true;
  }

  static auto split_top_level(std::string_view text)
    -> std::vector<std::string_view> {
    auto result = std::vector<std::string_view>{};
    auto depth = size_t{0};
    auto begin = size_t{0};
    for (auto i = size_t{0}; i < text.size(); ++i) {
      if (skip_quoted_token(text, i)) {
        continue;
      }
      auto c = text[i];
      if (c == '(') {
        ++depth;
        continue;
      }
      if (c == ')') {
        TENZIR_ASSERT(depth > 0);
        --depth;
        continue;
      }
      if (c == ',' and depth == 0) {
        result.push_back(detail::trim(text.substr(begin, i - begin)));
        begin = i + 1;
      }
    }
    result.push_back(detail::trim(text.substr(begin)));
    return result;
  }

  static auto find_top_level_space(std::string_view text) -> size_t {
    auto depth = size_t{0};
    for (auto i = size_t{0}; i < text.size(); ++i) {
      if (skip_quoted_token(text, i)) {
        continue;
      }
      auto c = text[i];
      if (c == '(') {
        ++depth;
        continue;
      }
      if (c == ')') {
        TENZIR_ASSERT(depth > 0);
        --depth;
        continue;
      }
      if (depth == 0 and std::isspace(static_cast<unsigned char>(c))) {
        return i;
      }
    }
    return std::string_view::npos;
  }

  static auto unwrap_call(std::string_view text, std::string_view name)
    -> Option<std::string_view> {
    if (not text.starts_with(name)) {
      return None{};
    }
    if (text.size() <= name.size() + 1 or text[name.size()] != '('
        or text.back() != ')') {
      return None{};
    }
    return text.substr(name.size() + 1, text.size() - name.size() - 2);
  }

  static auto parse_size(std::string_view text, size_t& result) -> bool {
    auto value = uint64_t{0};
    auto [ptr, ec]
      = std::from_chars(text.data(), text.data() + text.size(), value);
    if (ec != std::errc{} or ptr != text.data() + text.size()) {
      return false;
    }
    result = detail::narrow_cast<size_t>(value);
    return true;
  }

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

  static auto format_scaled_integer(std::string digits, size_t scale)
    -> std::string {
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
  rescale_datetime64_to_nanos(int64_t value, size_t precision, value_path path,
                              diagnostic_handler& dh) -> failure_or<int64_t> {
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

  static auto is_scalar_kind(ParsedType::Kind kind) -> bool {
    return kind != ParsedType::Kind::nullable_
           and kind != ParsedType::Kind::record_
           and kind != ParsedType::Kind::list_
           and kind != ParsedType::Kind::blob_;
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
    if (auto inner = unwrap_call(text, "LowCardinality")) {
      return parse_clickhouse_type(*inner, path, dh);
    }
    if (auto inner = unwrap_call(text, "SimpleAggregateFunction")) {
      auto parts = split_top_level(*inner);
      if (parts.size() != 2) {
        return malformed();
      }
      return parse_clickhouse_type(parts[1], path, dh);
    }
    if (auto inner = unwrap_call(text, "Nullable")) {
      TRY(auto nested, parse_clickhouse_type(*inner, path, dh));
      auto result = ParsedType{.kind = ParsedType::Kind::nullable_};
      result.child = Box<ParsedType>{std::in_place, std::move(nested)};
      return result;
    }
    if (text == "Array(UInt8)") {
      return ParsedType{.kind = ParsedType::Kind::blob_};
    }
    if (auto inner = unwrap_call(text, "Array")) {
      TRY(auto nested, parse_clickhouse_type(*inner, path.list(), dh));
      auto result = ParsedType{.kind = ParsedType::Kind::list_};
      result.child = Box<ParsedType>{std::in_place, std::move(nested)};
      return result;
    }
    if (auto inner = unwrap_call(text, "Tuple")) {
      auto result = ParsedType{.kind = ParsedType::Kind::record_};
      auto parts = split_top_level(*inner);
      result.fields.reserve(parts.size());
      for (auto i = size_t{0}; i < parts.size(); ++i) {
        auto part = detail::trim(parts[i]);
        auto split = find_top_level_space(part);
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
        result.fields.push_back({
          .name = std::move(field_name),
          .value = Box<ParsedType>{std::in_place, std::move(nested)},
        });
      }
      make_unique_field_names(result.fields, "field");
      return result;
    }
    if (text == "Nothing" or text == "Null") {
      return ParsedType{.kind = ParsedType::Kind::null_};
    }
    if (text == "Bool") {
      return ParsedType{.kind = ParsedType::Kind::bool_};
    }
    if (text == "Int8" or text == "Int16" or text == "Int32"
        or text == "Int64") {
      return ParsedType{.kind = ParsedType::Kind::int64_};
    }
    if (text == "UInt8" or text == "UInt16" or text == "UInt32"
        or text == "UInt64") {
      return ParsedType{.kind = ParsedType::Kind::uint64_};
    }
    if (text == "Float32" or text == "Float64") {
      return ParsedType{.kind = ParsedType::Kind::double_};
    }
    if (text == "String" or text.starts_with("FixedString(")) {
      return ParsedType{.kind = ParsedType::Kind::string_};
    }
    if (text == "UUID") {
      return ParsedType{.kind = ParsedType::Kind::uuid_};
    }
    if (text == "Int128") {
      return ParsedType{.kind = ParsedType::Kind::int128_string_};
    }
    if (text.starts_with("Enum8(") or text.starts_with("Enum16(")) {
      return ParsedType{.kind = ParsedType::Kind::enum_name_};
    }
    if (text == "Date" or text == "Date32" or text == "DateTime"
        or text.starts_with("DateTime(")) {
      return ParsedType{.kind = ParsedType::Kind::time_};
    }
    if (text.starts_with("DateTime64(")) {
      auto inner = unwrap_call(text, "DateTime64");
      TENZIR_ASSERT(inner);
      auto parts = split_top_level(*inner);
      if (parts.empty()) {
        return malformed();
      }
      auto precision = size_t{0};
      if (not parse_size(parts[0], precision)) {
        return malformed();
      }
      return ParsedType{.kind = ParsedType::Kind::time_,
                        .time_precision = precision};
    }
    if (text == "IPv4" or text == "IPv6") {
      return ParsedType{.kind = ParsedType::Kind::ip_};
    }
    if (text.starts_with("Decimal(")) {
      auto inner = unwrap_call(text, "Decimal");
      TENZIR_ASSERT(inner);
      auto parts = split_top_level(*inner);
      if (parts.size() != 2) {
        return malformed();
      }
      auto precision = size_t{0};
      auto scale = size_t{0};
      if (not parse_size(parts[0], precision)
          or not parse_size(parts[1], scale)) {
        return malformed();
      }
      if (precision > 38) {
        unsupported_type_diagnostic(path, text)
          .hint("Decimal precisions above 38 are currently not supported")
          .emit(dh);
        return failure::promise();
      }
      return ParsedType{.kind = ParsedType::Kind::decimal_string_,
                        .decimal_scale = scale};
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
      auto inner = unwrap_call(text, prefix);
      TENZIR_ASSERT(inner);
      auto scale = size_t{0};
      if (not parse_size(*inner, scale)) {
        return malformed();
      }
      return ParsedType{.kind = ParsedType::Kind::decimal_string_,
                        .decimal_scale = scale};
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
  clickhouse_type_to_tenzir_type(ParsedType const& desc, value_path path,
                                 diagnostic_handler& dh) -> failure_or<type> {
    switch (desc.kind) {
      case ParsedType::Kind::null_:
        return type{null_type{}};
      case ParsedType::Kind::nullable_:
        TENZIR_ASSERT(desc.child);
        return clickhouse_type_to_tenzir_type(**desc.child, path, dh);
      case ParsedType::Kind::bool_:
        return type{bool_type{}};
      case ParsedType::Kind::int64_:
        return type{int64_type{}};
      case ParsedType::Kind::uint64_:
        return type{uint64_type{}};
      case ParsedType::Kind::double_:
        return type{double_type{}};
      case ParsedType::Kind::string_:
      case ParsedType::Kind::uuid_:
      case ParsedType::Kind::enum_name_:
      case ParsedType::Kind::int128_string_:
      case ParsedType::Kind::decimal_string_:
        return type{string_type{}};
      case ParsedType::Kind::time_:
        return type{time_type{}};
      case ParsedType::Kind::ip_:
        return type{ip_type{}};
      case ParsedType::Kind::blob_:
        return type{blob_type{}};
      case ParsedType::Kind::record_: {
        if (desc.fields.empty()) {
          diagnostic::error("ClickHouse column `{}` is an empty tuple, which "
                            "is not supported",
                            path)
            .emit(dh);
          return failure::promise();
        }
        auto fields = std::vector<struct record_type::field>{};
        fields.reserve(desc.fields.size());
        for (auto const& field : desc.fields) {
          TRY(auto nested, clickhouse_type_to_tenzir_type(
                             *field.value, path.field(field.name), dh));
          fields.emplace_back(field.name, std::move(nested));
        }
        return type{record_type{fields}};
      }
      case ParsedType::Kind::list_: {
        TENZIR_ASSERT(desc.child);
        TRY(auto nested,
            clickhouse_type_to_tenzir_type(**desc.child, path.list(), dh));
        return type{list_type{nested}};
      }
    }
    TENZIR_UNREACHABLE();
  }

  static auto
  make_query_schema(std::string name, std::vector<ParsedField> columns,
                    diagnostic_handler& dh) -> failure_or<QuerySchema> {
    if (columns.empty()) {
      diagnostic::error("ClickHouse query returned zero columns").emit(dh);
      return failure::promise();
    }
    make_unique_field_names(columns, "column");
    auto fields = std::vector<struct record_type::field>{};
    fields.reserve(columns.size());
    for (auto const& column : columns) {
      TRY(auto nested, clickhouse_type_to_tenzir_type(
                         *column.value, value_path{}.field(column.name), dh));
      fields.emplace_back(column.name, std::move(nested));
    }
    auto schema = type{name, record_type{fields}};
    return QuerySchema{
      .name = std::move(name),
      .columns = std::move(columns),
      .schema = std::move(schema),
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
    auto ok = true;
    auto query = ::clickhouse::Query{fmt::format(
      "DESCRIBE TABLE {} SETTINGS describe_compact_output=1", table)};
    query.OnData([&](::clickhouse::Block const& block) {
      if (not ok) {
        return;
      }
      if (block.GetColumnCount() == 0) {
        return;
      }
      if (block.GetColumnCount() < 2) {
        diagnostic::error("unexpected ClickHouse response to DESCRIBE TABLE")
          .emit(dh);
        ok = false;
        return;
      }
      auto names = block[0]->As<::clickhouse::ColumnString>();
      auto types = block[1]->As<::clickhouse::ColumnString>();
      if (not names or not types) {
        diagnostic::error("unexpected ClickHouse DESCRIBE TABLE column layout")
          .emit(dh);
        ok = false;
        return;
      }
      for (auto row = size_t{0}; row < block.GetRowCount(); ++row) {
        auto name = std::string{names->At(row)};
        auto type_name = remove_non_significant_whitespace(types->At(row));
        auto parsed
          = parse_clickhouse_type(type_name, value_path{}.field(name), dh);
        if (not parsed) {
          ok = false;
          return;
        }
        columns.push_back({
          .name = std::move(name),
          .value = Box<ParsedType>{std::in_place, std::move(*parsed)},
        });
      }
    });
    client.Select(query);
    if (not ok) {
      return failure::promise();
    }
    return make_query_schema(std::move(schema_name), std::move(columns), dh);
  }

  static auto
  append_scalar(builder_ref builder, ::clickhouse::ColumnRef const& column,
                size_t row, ParsedType const& desc, value_path path,
                diagnostic_handler& dh) -> failure_or<void> {
    switch (desc.kind) {
      case ParsedType::Kind::decimal_string_: {
        if (auto decimal = column->As<::clickhouse::ColumnDecimal>()) {
          builder.data(format_scaled_integer(format_int128(decimal->At(row)),
                                             desc.decimal_scale));
          return {};
        }
        auto bytes = column->GetItem(row).AsBinaryData();
        switch (bytes.size()) {
          case 4: {
            auto value = int32_t{0};
            std::memcpy(&value, bytes.data(), sizeof(value));
            builder.data(
              format_scaled_integer(std::to_string(value), desc.decimal_scale));
            return {};
          }
          case 8: {
            auto value = int64_t{0};
            std::memcpy(&value, bytes.data(), sizeof(value));
            builder.data(
              format_scaled_integer(std::to_string(value), desc.decimal_scale));
            return {};
          }
          case 16: {
            auto value = ::clickhouse::Int128{};
            std::memcpy(&value, bytes.data(), sizeof(value));
            builder.data(
              format_scaled_integer(format_int128(value), desc.decimal_scale));
            return {};
          }
          default:
            break;
        }
        diagnostic::error("expected decimal ClickHouse type for `{}`", path)
          .emit(dh);
        return failure::promise();
      }
      case ParsedType::Kind::ip_: {
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
      default:
        break;
    }
    auto item = column->GetItem(row);
    switch (desc.kind) {
      case ParsedType::Kind::null_:
        builder.null();
        return {};
      case ParsedType::Kind::bool_:
        if (item.type == ::clickhouse::Type::UInt8) {
          builder.data(item.get<uint8_t>() != 0);
          return {};
        }
        diagnostic::error("expected bool-compatible ClickHouse type for `{}`",
                          path)
          .emit(dh);
        return failure::promise();
      case ParsedType::Kind::int64_:
        switch (item.type) {
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
          case ::clickhouse::Type::Enum8:
            builder.data(int64_t{item.get<int8_t>()});
            return {};
          case ::clickhouse::Type::Enum16:
            builder.data(int64_t{item.get<int16_t>()});
            return {};
          default:
            break;
        }
        diagnostic::error("expected signed integer ClickHouse type for `{}`",
                          path)
          .emit(dh);
        return failure::promise();
      case ParsedType::Kind::uint64_:
        switch (item.type) {
          case ::clickhouse::Type::UInt8:
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
          default:
            break;
        }
        diagnostic::error("expected unsigned integer ClickHouse type for `{}`",
                          path)
          .emit(dh);
        return failure::promise();
      case ParsedType::Kind::double_:
        switch (item.type) {
          case ::clickhouse::Type::Float32:
            builder.data(double{item.get<float>()});
            return {};
          case ::clickhouse::Type::Float64:
            builder.data(item.get<double>());
            return {};
          default:
            break;
        }
        diagnostic::error("expected floating-point ClickHouse type for `{}`",
                          path)
          .emit(dh);
        return failure::promise();
      case ParsedType::Kind::string_:
        if (item.type == ::clickhouse::Type::String
            or item.type == ::clickhouse::Type::FixedString) {
          builder.data(std::string{item.get<std::string_view>()});
          return {};
        }
        diagnostic::error("expected string ClickHouse type for `{}`", path)
          .emit(dh);
        return failure::promise();
      case ParsedType::Kind::uuid_:
        if (item.type == ::clickhouse::Type::UUID) {
          auto data = item.AsBinaryData();
          auto first = uint64_t{0};
          auto second = uint64_t{0};
          std::memcpy(&first, data.data(), sizeof(first));
          std::memcpy(&second, data.data() + sizeof(first), sizeof(second));
          builder.data(format_uuid(::clickhouse::UUID{first, second}));
          return {};
        }
        diagnostic::error("expected UUID ClickHouse type for `{}`", path)
          .emit(dh);
        return failure::promise();
      case ParsedType::Kind::enum_name_:
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
      case ParsedType::Kind::int128_string_:
        if (item.type == ::clickhouse::Type::Int128) {
          builder.data(format_int128(item.get<::clickhouse::Int128>()));
          return {};
        }
        diagnostic::error("expected Int128 ClickHouse type for `{}`", path)
          .emit(dh);
        return failure::promise();
      case ParsedType::Kind::time_:
        switch (item.type) {
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
            TRY(auto value, time_from_unix_seconds(item.get<uint32_t>(),
                                                   "DateTime", path, dh));
            builder.data(value);
            return {};
          }
          case ::clickhouse::Type::DateTime64: {
            TRY(auto ns, rescale_datetime64_to_nanos(
                           item.get<int64_t>(), desc.time_precision, path, dh));
            builder.data(time{std::chrono::duration_cast<duration>(
              std::chrono::nanoseconds{ns})});
            return {};
          }
          default:
            break;
        }
        diagnostic::error("expected temporal ClickHouse type for `{}`", path)
          .emit(dh);
        return failure::promise();
      case ParsedType::Kind::ip_:
      case ParsedType::Kind::decimal_string_:
        TENZIR_UNREACHABLE();
      case ParsedType::Kind::nullable_:
      case ParsedType::Kind::blob_:
      case ParsedType::Kind::record_:
      case ParsedType::Kind::list_:
        break;
    }
    diagnostic::error("unexpected ClickHouse type mapping for `{}`", path)
      .emit(dh);
    return failure::promise();
  }

  static auto
  append_value(builder_ref builder, ::clickhouse::ColumnRef const& column,
               size_t row, ParsedType const& desc, value_path path,
               diagnostic_handler& dh) -> failure_or<void> {
    switch (desc.kind) {
      case ParsedType::Kind::nullable_:
        TENZIR_ASSERT(desc.child);
        if (auto nullable = column->As<::clickhouse::ColumnNullable>()) {
          if (nullable->IsNull(row)) {
            builder.null();
            return {};
          }
          return append_value(builder, nullable->Nested(), row, **desc.child,
                              path, dh);
        }
        if (is_scalar_kind((**desc.child).kind)) {
          auto item = column->GetItem(row);
          if (item.type == ::clickhouse::Type::Void) {
            builder.null();
            return {};
          }
        }
        return append_value(builder, column, row, **desc.child, path, dh);
      case ParsedType::Kind::record_: {
        auto tuple = column->As<::clickhouse::ColumnTuple>();
        if (not tuple) {
          diagnostic::error("expected tuple ClickHouse type for `{}`", path)
            .emit(dh);
          return failure::promise();
        }
        if (tuple->TupleSize() != desc.fields.size()) {
          diagnostic::error("unexpected tuple size for `{}`", path).emit(dh);
          return failure::promise();
        }
        auto record = builder.record();
        for (auto i = size_t{0}; i < desc.fields.size(); ++i) {
          TRY(append_value(record.field(desc.fields[i].name), tuple->At(i), row,
                           *desc.fields[i].value,
                           path.field(desc.fields[i].name), dh));
        }
        return {};
      }
      case ParsedType::Kind::list_: {
        TENZIR_ASSERT(desc.child);
        auto array = column->As<::clickhouse::ColumnArray>();
        if (not array) {
          diagnostic::error("expected array ClickHouse type for `{}`", path)
            .emit(dh);
          return failure::promise();
        }
        auto nested = array->GetAsColumn(row);
        auto list = builder.list();
        for (auto i = size_t{0}; i < nested->Size(); ++i) {
          TRY(append_value(list, nested, i, **desc.child, path.list(), dh));
        }
        return {};
      }
      case ParsedType::Kind::blob_: {
        auto array = column->As<::clickhouse::ColumnArray>();
        if (not array) {
          diagnostic::error("expected byte array ClickHouse type for `{}`",
                            path)
            .emit(dh);
          return failure::promise();
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
        builder.data(std::move(value));
        return {};
      }
      case ParsedType::Kind::null_:
      case ParsedType::Kind::bool_:
      case ParsedType::Kind::int64_:
      case ParsedType::Kind::uint64_:
      case ParsedType::Kind::double_:
      case ParsedType::Kind::string_:
      case ParsedType::Kind::time_:
      case ParsedType::Kind::ip_:
      case ParsedType::Kind::uuid_:
      case ParsedType::Kind::enum_name_:
      case ParsedType::Kind::int128_string_:
      case ParsedType::Kind::decimal_string_:
        return append_scalar(builder, column, row, desc, path, dh);
    }
    TENZIR_UNREACHABLE();
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
    auto builder = series_builder{schema.schema};
    for (auto row = size_t{0}; row < block.GetRowCount(); ++row) {
      auto event = builder.record();
      for (auto column = size_t{0}; column < schema.columns.size(); ++column) {
        TRY(append_value(event.field(schema.columns[column].name),
                         block[column], row, *schema.columns[column].value,
                         value_path{}.field(schema.columns[column].name), dh));
      }
    }
    return builder.finish_as_table_slice();
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

  auto run_query(::clickhouse::ClientOptions options, QueryPlan plan)
    -> Task<void> {
    auto terminal_error = Option<ErrorMessage>{};
    try {
      auto client = ::clickhouse::Client{std::move(options)};
      auto schema = Option<QuerySchema>{};
      if (plan.described_table) {
        auto dh = collecting_diagnostic_handler{};
        auto described = describe_table_schema(client, *plan.described_table,
                                               plan.schema_name, dh);
        if (not described) {
          terminal_error = make_error_message(
            std::move(dh), "failed to describe ClickHouse table");
        } else {
          schema = std::move(*described);
        }
      }
      if (terminal_error) {
        co_await enqueue_message(*runtime_,
                                 Message{std::move(*terminal_error)});
        co_return;
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
          auto dh = collecting_diagnostic_handler{};
          auto inferred = infer_schema_from_block(block, plan.schema_name, dh);
          if (not inferred) {
            failed = true;
            std::ignore = folly::coro::blockingWait(enqueue_message(
              *runtime_, Message{make_error_message(
                           std::move(dh), "failed to infer ClickHouse query "
                                          "schema")}));
            runtime_->stop_requested.store(true, std::memory_order_release);
            return false;
          }
          schema = std::move(*inferred);
        }
        if (block.GetRowCount() == 0) {
          return not runtime_->stop_requested.load(std::memory_order_acquire);
        }
        auto dh = collecting_diagnostic_handler{};
        auto slices = block_to_slices(block, *schema, dh);
        if (not slices) {
          failed = true;
          std::ignore = folly::coro::blockingWait(enqueue_message(
            *runtime_, Message{make_error_message(
                         std::move(dh), "failed to decode ClickHouse block")}));
          runtime_->stop_requested.store(true, std::memory_order_release);
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
        std::ignore = runtime_->queue.try_enqueue(Message{DoneMessage{}});
        co_return;
      }
      co_await enqueue_message(*runtime_, Message{DoneMessage{}});
    } catch (const panic_exception&) {
      throw;
    } catch (const ::clickhouse::ServerError& e) {
      terminal_error = ErrorMessage{
        .diagnostics
        = {diagnostic::error("ClickHouse error {}: {}", e.GetCode(), e.what())
             .done()},
        .add_tls_hints = true,
      };
    } catch (const std::exception& e) {
      terminal_error = ErrorMessage{
        .diagnostics
        = {diagnostic::error("ClickHouse error: {}", e.what()).done()},
        .add_tls_hints = true,
      };
    }
    if (terminal_error) {
      co_await enqueue_message(*runtime_, Message{std::move(*terminal_error)});
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
    auto table_arg = d.named("table", &FromClickhouseArgs::table);
    d.named_optional("host", &FromClickhouseArgs::host);
    auto port_arg = d.named("port", &FromClickhouseArgs::port);
    d.named_optional("user", &FromClickhouseArgs::user);
    d.named_optional("password", &FromClickhouseArgs::password);
    auto sql_arg = d.named("sql", &FromClickhouseArgs::sql);
    auto tls_validate
      = tls_options{}.add_to_describer(d, &FromClickhouseArgs::tls);
    d.operator_location(&FromClickhouseArgs::operator_location);
    d.validate([=, tls_validate
                   = std::move(tls_validate)](DescribeCtx& ctx) -> Empty {
      tls_validate(ctx);
      auto has_table = ctx.get(table_arg).has_value();
      auto has_sql = ctx.get(sql_arg).has_value();
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
