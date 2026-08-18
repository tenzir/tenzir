//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/narrow.hpp"
#include "tenzir/series_builder.hpp"

#include <opentelemetry/proto/logs/v1/logs.pb.h>

#include "decode_internal.hpp"

namespace tenzir::plugins::accept_otlp::detail {

namespace logs = ::opentelemetry::proto::logs::v1;

auto log_type(AttributeMode mode) -> type {
  auto const tagged = record_type{
    {"kind", string_type{}},         {"string_value", string_type{}},
    {"bool_value", bool_type{}},     {"int_value", int64_type{}},
    {"double_value", double_type{}}, {"bytes_value", blob_type{}},
    {"json_value", string_type{}},
  };
  return type{"otel.log",
              append_fields(common_fields(mode),
                            {{"time", time_type{}},
                             {"observed_time", time_type{}},
                             {"severity_number", int64_type{}},
                             {"severity_text", string_type{}},
                             {"body", tagged},
                             {"attributes", attributes_type(mode)},
                             {"dropped_attributes_count", uint64_type{}},
                             {"flags", uint64_type{}},
                             {"trace_id", string_type{}},
                             {"span_id", string_type{}},
                             {"event_name", string_type{}}})};
}

auto materialize_logs(collector_logs::ExportLogsServiceRequest request,
                      DecodeContext ctx) -> DecodedSlices {
  auto builder = series_builder{log_type(ctx.attribute_mode)};
  for (auto const& resource_logs : request.resource_logs()) {
    for (auto const& scope_logs : resource_logs.scope_logs()) {
      auto const batch_rows
        = batch_row_limit(resource_logs.resource(), resource_logs.schema_url(),
                          scope_logs.scope(), scope_logs.schema_url(), ctx);
      for (auto const& log : scope_logs.log_records()) {
        if (ctx.is_cancelled()) {
          co_yield DecodedSlice{Err{std::string{cancelled_error}}};
          co_return;
        }
        auto event
          = with_context(resource_logs.resource(), resource_logs.schema_url(),
                         scope_logs.scope(), scope_logs.schema_url(), ctx);
        if (event.is_err()) {
          co_yield DecodedSlice{Err{std::move(event).unwrap_err()}};
          co_return;
        }
        auto materialized = std::move(event).unwrap();
        materialized["time"] = timestamp(log.time_unix_nano());
        materialized["observed_time"]
          = timestamp(log.observed_time_unix_nano());
        materialized["severity_number"]
          = static_cast<int64_t>(log.severity_number());
        materialized["severity_text"] = nullable_string(log.severity_text());
        auto body = make_tagged_any_value(log.body(), ctx);
        if (body.is_err()) {
          co_yield DecodedSlice{Err{std::move(body).unwrap_err()}};
          co_return;
        }
        materialized["body"] = std::move(body).unwrap();
        auto attributes = make_attributes(log.attributes(), ctx);
        if (attributes.is_err()) {
          co_yield DecodedSlice{Err{std::move(attributes).unwrap_err()}};
          co_return;
        }
        materialized["attributes"] = std::move(attributes).unwrap();
        materialized["dropped_attributes_count"]
          = static_cast<uint64_t>(log.dropped_attributes_count());
        materialized["flags"] = static_cast<uint64_t>(log.flags());
        materialized["trace_id"] = optional_id_string(log.trace_id(), 16);
        materialized["span_id"] = optional_id_string(log.span_id(), 8);
        materialized["event_name"] = nullable_string(log.event_name());
        builder.data(materialized);
        if (builder.length() >= batch_rows) {
          for (auto& slice : builder.finish_as_table_slice("otel.log")) {
            co_yield DecodedSlice{std::move(slice)};
          }
        }
      }
    }
  }
  if (ctx.is_cancelled()) {
    co_yield DecodedSlice{Err{std::string{cancelled_error}}};
    co_return;
  }
  for (auto& slice : builder.finish_as_table_slice("otel.log")) {
    co_yield DecodedSlice{std::move(slice)};
  }
}

auto decode_logs(collector_logs::ExportLogsServiceRequest request,
                 DecodeContext ctx) -> DecodeResult {
  auto const unique = ctx.attribute_mode == AttributeMode::record;
  for (auto const& resource_logs : request.resource_logs()) {
    if (ctx.is_cancelled()) {
      return Err{std::string{cancelled_error}};
    }
    auto valid = validate_resource(resource_logs.resource(), unique, &ctx);
    if (valid.is_err()) {
      return Err{std::move(valid).unwrap_err()};
    }
    for (auto const& scope_logs : resource_logs.scope_logs()) {
      if (ctx.is_cancelled()) {
        return Err{std::string{cancelled_error}};
      }
      valid
        = validate_attributes(scope_logs.scope().attributes(), unique, &ctx);
      if (valid.is_err()) {
        return Err{std::move(valid).unwrap_err()};
      }
      for (auto const& log : scope_logs.log_records()) {
        if (ctx.is_cancelled()) {
          return Err{std::string{cancelled_error}};
        }
        valid = validate_time(log.time_unix_nano(), false, "time");
        if (valid.is_err()) {
          return Err{std::move(valid).unwrap_err()};
        }
        valid = validate_time(log.observed_time_unix_nano(), false,
                              "observed_time");
        if (valid.is_err()) {
          return Err{std::move(valid).unwrap_err()};
        }
        valid = validate_any_value(log.body(), &ctx);
        if (valid.is_err()) {
          return Err{std::move(valid).unwrap_err()};
        }
        valid = validate_attributes(log.attributes(), unique, &ctx);
        if (valid.is_err()) {
          return Err{std::move(valid).unwrap_err()};
        }
      }
    }
  }
  return materialize_logs(std::move(request), std::move(ctx));
}

} // namespace tenzir::plugins::accept_otlp::detail
