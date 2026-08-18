//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/narrow.hpp"
#include "tenzir/series_builder.hpp"

#include <opentelemetry/proto/trace/v1/trace.pb.h>

#include <array>

#include "decode_internal.hpp"

namespace tenzir::plugins::accept_otlp::detail {

namespace trace = ::opentelemetry::proto::trace::v1;

auto span_type(AttributeMode mode) -> type {
  auto const attrs = attributes_type(mode);
  auto const event = record_type{
    {"time", time_type{}},
    {"name", string_type{}},
    {"attributes", attrs},
    {"dropped_attributes_count", uint64_type{}},
  };
  auto const link = record_type{
    {"trace_id", string_type{}},
    {"span_id", string_type{}},
    {"trace_state", string_type{}},
    {"attributes", attrs},
    {"dropped_attributes_count", uint64_type{}},
    {"flags", uint64_type{}},
  };
  return type{"otel.span",
              append_fields(common_fields(mode),
                            {{"trace_id", string_type{}},
                             {"span_id", string_type{}},
                             {"parent_span_id", string_type{}},
                             {"trace_state", string_type{}},
                             {"name", string_type{}},
                             {"kind_id", int64_type{}},
                             {"kind", string_type{}},
                             {"start_time", time_type{}},
                             {"end_time", time_type{}},
                             {"attributes", attrs},
                             {"dropped_attributes_count", uint64_type{}},
                             {"events", list_type{event}},
                             {"dropped_events_count", uint64_type{}},
                             {"links", list_type{link}},
                             {"dropped_links_count", uint64_type{}},
                             {"status",
                              record_type{
                                {"code_id", int64_type{}},
                                {"code", string_type{}},
                                {"message", string_type{}},
                              }},
                             {"flags", uint64_type{}}})};
}

auto span_kind_name(int value) -> data {
  static constexpr auto names = std::array<std::string_view, 6>{
    "unspecified", "internal", "server", "client", "producer", "consumer"};
  if (value < 0 or value >= tenzir::detail::narrow<int>(names.size())) {
    return {};
  }
  return std::string{names[tenzir::detail::narrow<size_t>(value)]};
}

auto status_name(int value) -> data {
  static constexpr auto names
    = std::array<std::string_view, 3>{"unset", "ok", "error"};
  if (value < 0 or value >= tenzir::detail::narrow<int>(names.size())) {
    return {};
  }
  return std::string{names[tenzir::detail::narrow<size_t>(value)]};
}

auto make_span_events(trace::Span const& span, DecodeContext const& ctx)
  -> Result<list, std::string> {
  auto result = list{};
  result.reserve(tenzir::detail::narrow<size_t>(span.events_size()));
  for (auto const& event : span.events()) {
    if (ctx.is_cancelled()) {
      return Err{std::string{cancelled_error}};
    }
    TRY(auto attributes, make_attributes(event.attributes(), ctx));
    result.emplace_back(record{
      {"time", timestamp(event.time_unix_nano())},
      {"name", event.name()},
      {"attributes", std::move(attributes)},
      {"dropped_attributes_count",
       static_cast<uint64_t>(event.dropped_attributes_count())},
    });
  }
  return result;
}

auto make_span_links(trace::Span const& span, DecodeContext const& ctx)
  -> Result<list, std::string> {
  auto result = list{};
  result.reserve(tenzir::detail::narrow<size_t>(span.links_size()));
  for (auto const& link : span.links()) {
    if (ctx.is_cancelled()) {
      return Err{std::string{cancelled_error}};
    }
    TRY(auto attributes, make_attributes(link.attributes(), ctx));
    result.emplace_back(record{
      {"trace_id", id_string(link.trace_id())},
      {"span_id", id_string(link.span_id())},
      {"trace_state", nullable_string(link.trace_state())},
      {"attributes", std::move(attributes)},
      {"dropped_attributes_count",
       static_cast<uint64_t>(link.dropped_attributes_count())},
      {"flags", static_cast<uint64_t>(link.flags())},
    });
  }
  return result;
}

auto validate_span(trace::Span const& span, DecodeContext const& ctx)
  -> Result<Empty, std::string> {
  auto const unique = ctx.attribute_mode == AttributeMode::record;
  auto result = validate_id(span.trace_id(), 16, true, "trace_id");
  if (result.is_err()) {
    return result;
  }
  result = validate_id(span.span_id(), 8, true, "span_id");
  if (result.is_err()) {
    return result;
  }
  result = validate_id(span.parent_span_id(), 8, false, "parent_span_id");
  if (result.is_err()) {
    return result;
  }
  if (span.name().empty()) {
    return Err{std::string{"span name must not be empty"}};
  }
  result = validate_time(span.start_time_unix_nano(), true, "start_time");
  if (result.is_err()) {
    return result;
  }
  result = validate_time(span.end_time_unix_nano(), true, "end_time");
  if (result.is_err()) {
    return result;
  }
  if (span.end_time_unix_nano() < span.start_time_unix_nano()) {
    return Err{std::string{"span end time precedes its start time"}};
  }
  result = validate_attributes(span.attributes(), unique, &ctx);
  if (result.is_err()) {
    return result;
  }
  for (auto const& event : span.events()) {
    if (ctx.is_cancelled()) {
      return Err{std::string{cancelled_error}};
    }
    if (event.name().empty()) {
      return Err{std::string{"span event name must not be empty"}};
    }
    result = validate_time(event.time_unix_nano(), true, "event.time");
    if (result.is_err()) {
      return result;
    }
    result = validate_attributes(event.attributes(), unique, &ctx);
    if (result.is_err()) {
      return result;
    }
  }
  for (auto const& link : span.links()) {
    if (ctx.is_cancelled()) {
      return Err{std::string{cancelled_error}};
    }
    result = validate_id(link.trace_id(), 16, true, "link.trace_id");
    if (result.is_err()) {
      return result;
    }
    result = validate_id(link.span_id(), 8, true, "link.span_id");
    if (result.is_err()) {
      return result;
    }
    result = validate_attributes(link.attributes(), unique, &ctx);
    if (result.is_err()) {
      return result;
    }
  }
  return Empty{};
}

auto materialize_traces(collector_trace::ExportTraceServiceRequest request,
                        DecodeContext ctx) -> DecodedSlices {
  auto builder = series_builder{span_type(ctx.attribute_mode)};
  for (auto const& resource_spans : request.resource_spans()) {
    for (auto const& scope_spans : resource_spans.scope_spans()) {
      auto const batch_rows
        = batch_row_limit(resource_spans.resource(),
                          resource_spans.schema_url(), scope_spans.scope(),
                          scope_spans.schema_url(), ctx);
      for (auto const& span : scope_spans.spans()) {
        if (ctx.is_cancelled()) {
          co_yield DecodedSlice{Err{std::string{cancelled_error}}};
          co_return;
        }
        auto event
          = with_context(resource_spans.resource(), resource_spans.schema_url(),
                         scope_spans.scope(), scope_spans.schema_url(), ctx);
        if (event.is_err()) {
          co_yield DecodedSlice{Err{std::move(event).unwrap_err()}};
          co_return;
        }
        auto materialized = std::move(event).unwrap();
        materialized["trace_id"] = id_string(span.trace_id());
        materialized["span_id"] = id_string(span.span_id());
        materialized["parent_span_id"] = id_string(span.parent_span_id());
        materialized["trace_state"] = nullable_string(span.trace_state());
        materialized["name"] = span.name();
        materialized["kind_id"] = static_cast<int64_t>(span.kind());
        materialized["kind"] = span_kind_name(span.kind());
        materialized["start_time"] = timestamp(span.start_time_unix_nano());
        materialized["end_time"] = timestamp(span.end_time_unix_nano());
        auto attributes = make_attributes(span.attributes(), ctx);
        if (attributes.is_err()) {
          co_yield DecodedSlice{Err{std::move(attributes).unwrap_err()}};
          co_return;
        }
        materialized["attributes"] = std::move(attributes).unwrap();
        materialized["dropped_attributes_count"]
          = static_cast<uint64_t>(span.dropped_attributes_count());
        auto events = make_span_events(span, ctx);
        if (events.is_err()) {
          co_yield DecodedSlice{Err{std::move(events).unwrap_err()}};
          co_return;
        }
        materialized["events"] = std::move(events).unwrap();
        materialized["dropped_events_count"]
          = static_cast<uint64_t>(span.dropped_events_count());
        auto links = make_span_links(span, ctx);
        if (links.is_err()) {
          co_yield DecodedSlice{Err{std::move(links).unwrap_err()}};
          co_return;
        }
        materialized["links"] = std::move(links).unwrap();
        materialized["dropped_links_count"]
          = static_cast<uint64_t>(span.dropped_links_count());
        materialized["status"]
          = record{{"code_id", static_cast<int64_t>(span.status().code())},
                   {"code", status_name(span.status().code())},
                   {"message", nullable_string(span.status().message())}};
        materialized["flags"] = static_cast<uint64_t>(span.flags());
        builder.data(materialized);
        if (builder.length() >= batch_rows) {
          for (auto& slice : builder.finish_as_table_slice("otel.span")) {
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
  for (auto& slice : builder.finish_as_table_slice("otel.span")) {
    co_yield DecodedSlice{std::move(slice)};
  }
}

auto decode_traces(collector_trace::ExportTraceServiceRequest request,
                   DecodeContext ctx) -> DecodeResult {
  auto const unique = ctx.attribute_mode == AttributeMode::record;
  for (auto const& resource_spans : request.resource_spans()) {
    if (ctx.is_cancelled()) {
      return Err{std::string{cancelled_error}};
    }
    auto valid = validate_resource(resource_spans.resource(), unique, &ctx);
    if (valid.is_err()) {
      return Err{std::move(valid).unwrap_err()};
    }
    for (auto const& scope_spans : resource_spans.scope_spans()) {
      if (ctx.is_cancelled()) {
        return Err{std::string{cancelled_error}};
      }
      valid
        = validate_attributes(scope_spans.scope().attributes(), unique, &ctx);
      if (valid.is_err()) {
        return Err{std::move(valid).unwrap_err()};
      }
      for (auto const& span : scope_spans.spans()) {
        valid = validate_span(span, ctx);
        if (valid.is_err()) {
          return Err{std::move(valid).unwrap_err()};
        }
      }
    }
  }
  return materialize_traces(std::move(request), std::move(ctx));
}

} // namespace tenzir::plugins::accept_otlp::detail
