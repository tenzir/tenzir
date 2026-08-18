//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/narrow.hpp"
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
auto decode_metrics(collector_metrics::ExportMetricsServiceRequest request,
                    DecodeContext ctx) -> DecodeResult;
auto decode_traces(collector_trace::ExportTraceServiceRequest request,
                   DecodeContext ctx) -> DecodeResult;

} // namespace tenzir::plugins::accept_otlp::detail
