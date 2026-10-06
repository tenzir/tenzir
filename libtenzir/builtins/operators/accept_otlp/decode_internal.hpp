//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/narrow.hpp"
#include "tenzir/nova/array_builder.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/try.hpp"
#include "tenzir/type.hpp"

#include <opentelemetry/proto/common/v1/common.pb.h>
#include <opentelemetry/proto/resource/v1/resource.pb.h>

#include <initializer_list>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "decode.hpp"

namespace tenzir::plugins::accept_otlp::detail {

// Both output representations share protobuf validation and record construction.
// The event builder never constructs an Arrow array or a table slice.
class EventsBuilder {
public:
  explicit EventsBuilder(type schema) : name_{schema.name()} {
  }

  auto data(record const& value) -> void {
    auto row = builder_.record();
    for (auto const& [key, field] : value) {
      nova::append_legacy_data(row.field(key), field, dh_);
    }
  }

  auto length() const -> int64_t {
    return builder_.length();
  }

  auto finish() -> std::vector<nova::Events> {
    if (builder_.length() == 0) {
      return {};
    }
    auto data = builder_.finish();
    builder_ = nova::ArrayBuilder<nova::Record>{};
    auto const length = data.length();
    auto result = std::vector<nova::Events>{};
    result.emplace_back(std::move(data), nova::storage::BitMap{length, true},
                        nova::Events::Meta::make_empty(length, name_));
    return result;
  }

private:
  std::string name_;
  nova::ArrayBuilder<nova::Record> builder_;
  null_diagnostic_handler dh_;
};

inline auto finish_batches(series_builder& builder)
  -> std::vector<table_slice> {
  return builder.finish_as_table_slice();
}

inline auto finish_batches(EventsBuilder& builder)
  -> std::vector<nova::Events> {
  return builder.finish();
}

template <class Builder>
using Batch =
  typename decltype(finish_batches(std::declval<Builder&>()))::value_type;

template <class Builder>
using DecodedBatch = Result<Batch<Builder>, std::string>;

template <class Builder>
using DecodedBatches = generator<DecodedBatch<Builder>>;

template <class Builder>
using BatchDecodeResult = Result<DecodedBatches<Builder>, std::string>;

namespace resource = ::opentelemetry::proto::resource::v1;

auto bytes_data(std::string_view bytes) -> data;
auto id_string(std::string_view bytes) -> data;
auto timestamp(uint64_t nanos) -> data;
auto nullable_string(std::string const& value) -> data;
auto make_tagged_any_value(common::AnyValue const& value,
                           DecodeContext const& ctx)
  -> Result<data, std::string>;
auto make_native_any_value(common::AnyValue const& value,
                           DecodeContext const& ctx)
  -> Result<data, std::string>;

template <class Attributes>
auto make_attributes(Attributes const& attributes, DecodeContext const& ctx)
  -> Result<data, std::string> {
  if (ctx.attribute_mode == AttributeMode::record) {
    auto result = record{};
    for (auto const& attribute : attributes) {
      if (ctx.is_cancelled()) {
        return Err{std::string{cancelled_error}};
      }
      TRY(auto value, make_native_any_value(attribute.value(), ctx));
      result[attribute.key()] = std::move(value);
    }
    return data{std::move(result)};
  }
  auto result = list{};
  result.reserve(tenzir::detail::narrow<size_t>(attributes.size()));
  for (auto const& attribute : attributes) {
    if (ctx.is_cancelled()) {
      return Err{std::string{cancelled_error}};
    }
    TRY(auto value, make_tagged_any_value(attribute.value(), ctx));
    result.emplace_back(
      record{{"key", attribute.key()}, {"value", std::move(value)}});
  }
  return data{std::move(result)};
}

auto with_context(resource::Resource const& resource_value,
                  std::string const& resource_schema_url,
                  common::InstrumentationScope const& scope_value,
                  std::string const& scope_schema_url, DecodeContext const& ctx)
  -> Result<record, std::string>;
auto batch_row_limit(resource::Resource const& resource_value,
                     std::string const& resource_schema_url,
                     common::InstrumentationScope const& scope_value,
                     std::string const& scope_schema_url,
                     DecodeContext const& ctx, size_t additional_size = 0)
  -> int64_t;
auto attributes_type(AttributeMode mode) -> type;
auto common_fields(AttributeMode mode)
  -> std::vector<struct record_type::field>;
auto append_fields(std::vector<struct record_type::field> fields,
                   std::initializer_list<record_type::field_view> extra)
  -> record_type;
auto optional_id_string(std::string_view id, size_t size) -> data;
auto validate_id(std::string_view id, size_t size, bool required,
                 std::string_view field) -> Result<Empty, std::string>;
auto validate_time(uint64_t value, bool required, std::string_view field)
  -> Result<Empty, std::string>;
auto validate_any_value(common::AnyValue const& value, DecodeContext const* ctx
                                                       = nullptr)
  -> Result<Empty, std::string>;

template <class Attributes>
auto validate_attributes(Attributes const& attributes, bool unique,
                         DecodeContext const* ctx = nullptr)
  -> Result<Empty, std::string> {
  auto keys = std::unordered_map<std::string_view, common::AnyValue const*>{};
  for (auto const& attribute : attributes) {
    if (ctx and ctx->is_cancelled()) {
      return Err{std::string{cancelled_error}};
    }
    if (attribute.key().empty()) {
      return Err{std::string{"attribute keys must not be empty"}};
    }
    if (unique) {
      auto [it, inserted] = keys.emplace(attribute.key(), &attribute.value());
      if (not inserted) {
        if (ctx) {
          ctx->warn_about_duplicate_attribute(attribute.key(), *it->second,
                                              attribute.value());
        }
        it->second = &attribute.value();
      }
    }
    auto valid = validate_any_value(attribute.value(), ctx);
    if (valid.is_err()) {
      return valid;
    }
  }
  return Empty{};
}

auto validate_resource(resource::Resource const& value, bool unique,
                       DecodeContext const* ctx = nullptr)
  -> Result<Empty, std::string>;

auto decode_logs(collector_logs::ExportLogsServiceRequest request,
                 DecodeContext ctx) -> DecodeResult;
auto decode_logs_events(collector_logs::ExportLogsServiceRequest request,
                        DecodeContext ctx) -> EventsDecodeResult;
auto decode_metrics(collector_metrics::ExportMetricsServiceRequest request,
                    DecodeContext ctx) -> DecodeResult;
auto decode_metrics_events(
  collector_metrics::ExportMetricsServiceRequest request, DecodeContext ctx)
  -> EventsDecodeResult;
auto decode_traces(collector_trace::ExportTraceServiceRequest request,
                   DecodeContext ctx) -> DecodeResult;
auto decode_traces_events(collector_trace::ExportTraceServiceRequest request,
                          DecodeContext ctx) -> EventsDecodeResult;

} // namespace tenzir::plugins::accept_otlp::detail
