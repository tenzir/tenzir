//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/narrow.hpp"
#include "tenzir/series_builder.hpp"

#include <opentelemetry/proto/metrics/v1/metrics.pb.h>

#include <array>
#include <cmath>
#include <limits>

#include "decode_internal.hpp"

namespace tenzir::plugins::accept_otlp::detail {

namespace metrics = ::opentelemetry::proto::metrics::v1;

auto temporality_name(int value) -> data {
  static constexpr auto names
    = std::array<std::string_view, 3>{"unspecified", "delta", "cumulative"};
  if (value < 0 or value >= tenzir::detail::narrow<int>(names.size())) {
    return {};
  }
  return std::string{names[tenzir::detail::narrow<size_t>(value)]};
}

auto exemplar_type(AttributeMode mode) -> record_type {
  return record_type{
    {"filtered_attributes", attributes_type(mode)},
    {"time", time_type{}},
    {"value",
     record_type{{"int_value", int64_type{}}, {"double_value", double_type{}}}},
    {"trace_id", string_type{}},
    {"span_id", string_type{}},
  };
}

auto metric_common_fields(AttributeMode mode)
  -> std::vector<struct record_type::field> {
  auto result = common_fields(mode);
  result.emplace_back("metric",
                      record_type{{"name", string_type{}},
                                  {"description", string_type{}},
                                  {"unit", string_type{}},
                                  {"metadata", attributes_type(mode)}});
  result.emplace_back("attributes", attributes_type(mode));
  result.emplace_back("start_time", time_type{});
  result.emplace_back("time", time_type{});
  return result;
}

auto metric_type(std::string_view name, AttributeMode mode,
                 std::initializer_list<record_type::field_view> extra) -> type {
  return type{name, append_fields(metric_common_fields(mode), extra)};
}

auto gauge_type(AttributeMode mode) -> type {
  return metric_type("otel.metric.gauge", mode,
                     {{"value", record_type{{"int_value", int64_type{}},
                                            {"double_value", double_type{}}}},
                      {"exemplars", list_type{exemplar_type(mode)}},
                      {"flags", uint64_type{}}});
}

auto sum_type(AttributeMode mode) -> type {
  return metric_type("otel.metric.sum", mode,
                     {{"value", record_type{{"int_value", int64_type{}},
                                            {"double_value", double_type{}}}},
                      {"exemplars", list_type{exemplar_type(mode)}},
                      {"flags", uint64_type{}},
                      {"aggregation_temporality_id", int64_type{}},
                      {"aggregation_temporality", string_type{}},
                      {"monotonic", bool_type{}}});
}

auto histogram_type(AttributeMode mode) -> type {
  return metric_type("otel.metric.histogram", mode,
                     {{"aggregation_temporality_id", int64_type{}},
                      {"aggregation_temporality", string_type{}},
                      {"count", uint64_type{}},
                      {"sum", double_type{}},
                      {"min", double_type{}},
                      {"max", double_type{}},
                      {"bucket_counts", list_type{uint64_type{}}},
                      {"explicit_bounds", list_type{double_type{}}},
                      {"exemplars", list_type{exemplar_type(mode)}},
                      {"flags", uint64_type{}}});
}

auto exponential_histogram_type(AttributeMode mode) -> type {
  auto const buckets = record_type{{"offset", int64_type{}},
                                   {"bucket_counts", list_type{uint64_type{}}}};
  return metric_type("otel.metric.exponential_histogram", mode,
                     {{"aggregation_temporality_id", int64_type{}},
                      {"aggregation_temporality", string_type{}},
                      {"count", uint64_type{}},
                      {"sum", double_type{}},
                      {"min", double_type{}},
                      {"max", double_type{}},
                      {"scale", int64_type{}},
                      {"zero_count", uint64_type{}},
                      {"zero_threshold", double_type{}},
                      {"positive", buckets},
                      {"negative", buckets},
                      {"exemplars", list_type{exemplar_type(mode)}},
                      {"flags", uint64_type{}}});
}

auto summary_type(AttributeMode mode) -> type {
  return metric_type("otel.metric.summary", mode,
                     {{"count", uint64_type{}},
                      {"sum", double_type{}},
                      {"quantile_values", list_type{record_type{
                                            {"quantile", double_type{}},
                                            {"value", double_type{}},
                                          }}},
                      {"flags", uint64_type{}}});
}

auto make_metric_metadata(metrics::Metric const& metric,
                          DecodeContext const& ctx)
  -> Result<data, std::string> {
  TRY(auto metadata, make_attributes(metric.metadata(), ctx));
  return data{record{{"name", metric.name()},
                     {"description", nullable_string(metric.description())},
                     {"unit", nullable_string(metric.unit())},
                     {"metadata", std::move(metadata)}}};
}

auto has_no_recorded_value(auto const& point) -> bool {
  constexpr auto mask
    = static_cast<uint32_t>(metrics::DATA_POINT_FLAGS_NO_RECORDED_VALUE_MASK);
  return (point.flags() & mask) == mask;
}

auto make_number_value(metrics::NumberDataPoint const& point) -> data {
  auto result = record{{"int_value", data{}}, {"double_value", data{}}};
  if (has_no_recorded_value(point)) {
    return result;
  }
  if (point.value_case() == metrics::NumberDataPoint::kAsInt) {
    result["int_value"] = point.as_int();
  } else if (point.value_case() == metrics::NumberDataPoint::kAsDouble) {
    result["double_value"] = point.as_double();
  }
  return result;
}

auto make_exemplar_value(metrics::Exemplar const& exemplar) -> data {
  auto result = record{{"int_value", data{}}, {"double_value", data{}}};
  if (exemplar.value_case() == metrics::Exemplar::kAsInt) {
    result["int_value"] = exemplar.as_int();
  } else if (exemplar.value_case() == metrics::Exemplar::kAsDouble) {
    result["double_value"] = exemplar.as_double();
  }
  return result;
}

auto make_exemplars(
  google::protobuf::RepeatedPtrField<metrics::Exemplar> const& xs,
  DecodeContext const& ctx) -> Result<list, std::string> {
  auto result = list{};
  result.reserve(tenzir::detail::narrow<size_t>(xs.size()));
  for (auto const& exemplar : xs) {
    if (ctx.is_cancelled()) {
      return Err{std::string{cancelled_error}};
    }
    TRY(auto attributes, make_attributes(exemplar.filtered_attributes(), ctx));
    result.emplace_back(record{
      {"filtered_attributes", std::move(attributes)},
      {"time", timestamp(exemplar.time_unix_nano())},
      {"value", make_exemplar_value(exemplar)},
      {"trace_id", id_string(exemplar.trace_id())},
      {"span_id", id_string(exemplar.span_id())},
    });
  }
  return result;
}

template <class Point>
auto validate_point_common(Point const& point, DecodeContext const& ctx,
                           bool ignore_start_time = false)
  -> Result<Empty, std::string> {
  auto const unique = ctx.attribute_mode == AttributeMode::record;
  if (ctx.is_cancelled()) {
    return Err{std::string{cancelled_error}};
  }
  if (not ignore_start_time) {
    auto result
      = validate_time(point.start_time_unix_nano(), false, "start_time");
    if (result.is_err()) {
      return result;
    }
  }
  auto result = validate_time(point.time_unix_nano(), true, "time");
  if (result.is_err()) {
    return result;
  }
  if (not ignore_start_time and point.start_time_unix_nano() != 0
      and point.start_time_unix_nano() > point.time_unix_nano()) {
    return Err{std::string{"metric start time follows its sample time"}};
  }
  return validate_attributes(point.attributes(), unique, &ctx);
}

auto validate_exemplars(
  google::protobuf::RepeatedPtrField<metrics::Exemplar> const& exemplars,
  DecodeContext const& ctx) -> Result<Empty, std::string> {
  auto const unique = ctx.attribute_mode == AttributeMode::record;
  for (auto const& exemplar : exemplars) {
    if (ctx.is_cancelled()) {
      return Err{std::string{cancelled_error}};
    }
    auto result
      = validate_time(exemplar.time_unix_nano(), true, "exemplar.time");
    if (result.is_err()) {
      return result;
    }
    if (exemplar.value_case() == metrics::Exemplar::VALUE_NOT_SET) {
      return Err{std::string{"exemplar has no value"}};
    }
    result = validate_id(exemplar.trace_id(), 16, false, "exemplar.trace_id");
    if (result.is_err()) {
      return result;
    }
    result = validate_id(exemplar.span_id(), 8, false, "exemplar.span_id");
    if (result.is_err()) {
      return result;
    }
    result = validate_attributes(exemplar.filtered_attributes(), unique, &ctx);
    if (result.is_err()) {
      return result;
    }
  }
  return Empty{};
}

auto add_metric_common(record& event, metrics::Metric const& metric,
                       auto const& point, DecodeContext const& ctx,
                       bool ignore_start_time = false)
  -> Result<Empty, std::string> {
  TRY(auto metadata, make_metric_metadata(metric, ctx));
  TRY(auto attributes, make_attributes(point.attributes(), ctx));
  event["metric"] = std::move(metadata);
  event["attributes"] = std::move(attributes);
  event["start_time"]
    = ignore_start_time ? data{} : timestamp(point.start_time_unix_nano());
  event["time"] = timestamp(point.time_unix_nano());
  return Empty{};
}

auto make_u64_list(auto const& xs, DecodeContext const& ctx)
  -> Result<list, std::string> {
  auto result = list{};
  result.reserve(tenzir::detail::narrow<size_t>(xs.size()));
  for (auto x : xs) {
    if (ctx.is_cancelled()) {
      return Err{std::string{cancelled_error}};
    }
    result.emplace_back(static_cast<uint64_t>(x));
  }
  return result;
}

auto make_double_list(auto const& xs, DecodeContext const& ctx)
  -> Result<list, std::string> {
  auto result = list{};
  result.reserve(tenzir::detail::narrow<size_t>(xs.size()));
  for (auto x : xs) {
    if (ctx.is_cancelled()) {
      return Err{std::string{cancelled_error}};
    }
    result.emplace_back(static_cast<double>(x));
  }
  return result;
}

auto make_buckets(metrics::ExponentialHistogramDataPoint::Buckets const& xs,
                  DecodeContext const& ctx) -> Result<data, std::string> {
  TRY(auto counts, make_u64_list(xs.bucket_counts(), ctx));
  return data{record{{"offset", static_cast<int64_t>(xs.offset())},
                     {"bucket_counts", std::move(counts)}}};
}

auto sum_bucket_counts(auto const& counts, DecodeContext const& ctx)
  -> Option<uint64_t> {
  auto result = uint64_t{};
  for (auto count : counts) {
    if (ctx.is_cancelled()
        or count > std::numeric_limits<uint64_t>::max() - result) {
      return None{};
    }
    result += count;
  }
  return result;
}

auto validate_aggregation_temporality(metrics::AggregationTemporality value)
  -> Result<Empty, std::string> {
  if (value != metrics::AGGREGATION_TEMPORALITY_DELTA
      and value != metrics::AGGREGATION_TEMPORALITY_CUMULATIVE) {
    return Err{
      std::string{"aggregation temporality must be `delta` or `cumulative`"}};
  }
  return Empty{};
}

auto validate_aggregate_sum(uint64_t count, double sum)
  -> Result<Empty, std::string> {
  if (count == 0 and sum != 0.0) {
    return Err{std::string{"aggregate sum must be zero when count is zero"}};
  }
  return Empty{};
}

auto validate_metric(metrics::Metric const& metric, DecodeContext const& ctx)
  -> Result<Empty, std::string> {
  auto const unique = ctx.attribute_mode == AttributeMode::record;
  if (ctx.is_cancelled()) {
    return Err{std::string{cancelled_error}};
  }
  if (metric.name().empty()) {
    return Err{std::string{"metric name must not be empty"}};
  }
  auto result = validate_attributes(metric.metadata(), unique, &ctx);
  if (result.is_err()) {
    return result;
  }
  if (metric.data_case() == metrics::Metric::DATA_NOT_SET) {
    return Err{fmt::format("metric `{}` has no data", metric.name())};
  }
  auto validate_number = [&](metrics::NumberDataPoint const& point,
                             bool ignore_start_time = false) {
    auto valid = validate_point_common(point, ctx, ignore_start_time);
    if (valid.is_err() or has_no_recorded_value(point)) {
      return valid;
    }
    if (point.value_case() == metrics::NumberDataPoint::VALUE_NOT_SET) {
      return Result<Empty, std::string>{
        Err{std::string{"number data point has no value"}}};
    }
    return validate_exemplars(point.exemplars(), ctx);
  };
  if (metric.has_gauge()) {
    for (auto const& point : metric.gauge().data_points()) {
      result = validate_number(point, true);
      if (result.is_err()) {
        return result;
      }
    }
  } else if (metric.has_sum()) {
    result = validate_aggregation_temporality(
      metric.sum().aggregation_temporality());
    if (result.is_err()) {
      return result;
    }
    for (auto const& point : metric.sum().data_points()) {
      result = validate_number(point);
      if (result.is_err()) {
        return result;
      }
    }
  } else if (metric.has_histogram()) {
    result = validate_aggregation_temporality(
      metric.histogram().aggregation_temporality());
    if (result.is_err()) {
      return result;
    }
    for (auto const& point : metric.histogram().data_points()) {
      result = validate_point_common(point, ctx);
      if (result.is_err()) {
        return result;
      }
      if (has_no_recorded_value(point)) {
        continue;
      }
      if (point.has_sum()) {
        result = validate_aggregate_sum(point.count(), point.sum());
        if (result.is_err()) {
          return result;
        }
      }
      auto const counts = point.bucket_counts_size();
      auto const bounds = point.explicit_bounds_size();
      if ((counts == 0 and bounds != 0)
          or (counts != 0 and counts != bounds + 1)) {
        return Err{std::string{"histogram bucket counts and bounds disagree"}};
      }
      if (counts != 0) {
        auto total = sum_bucket_counts(point.bucket_counts(), ctx);
        if (not total or *total != point.count()) {
          return Err{std::string{"histogram bucket counts must add up to the "
                                 "total count"}};
        }
      }
      for (auto i = 0; i < bounds; ++i) {
        if (ctx.is_cancelled()) {
          return Err{std::string{cancelled_error}};
        }
        auto const bound = point.explicit_bounds(i);
        if (std::isnan(bound)
            or (i > 0 and bound <= point.explicit_bounds(i - 1))) {
          return Err{
            std::string{"histogram bounds must be strictly increasing"}};
        }
      }
      result = validate_exemplars(point.exemplars(), ctx);
      if (result.is_err()) {
        return result;
      }
    }
  } else if (metric.has_exponential_histogram()) {
    result = validate_aggregation_temporality(
      metric.exponential_histogram().aggregation_temporality());
    if (result.is_err()) {
      return result;
    }
    for (auto const& point : metric.exponential_histogram().data_points()) {
      result = validate_point_common(point, ctx);
      if (result.is_err()) {
        return result;
      }
      if (has_no_recorded_value(point)) {
        continue;
      }
      if (point.has_sum()) {
        result = validate_aggregate_sum(point.count(), point.sum());
        if (result.is_err()) {
          return result;
        }
      }
      if (not(point.zero_threshold() >= 0.0)) {
        return Err{std::string{"exponential histogram zero threshold must be "
                               "non-negative"}};
      }
      auto positive = sum_bucket_counts(point.positive().bucket_counts(), ctx);
      auto negative = sum_bucket_counts(point.negative().bucket_counts(), ctx);
      if (not positive or not negative
          or *positive > std::numeric_limits<uint64_t>::max() - *negative
          or *positive + *negative
               > std::numeric_limits<uint64_t>::max() - point.zero_count()
          or *positive + *negative + point.zero_count() != point.count()) {
        return Err{std::string{"exponential histogram buckets must add up to "
                               "the total count"}};
      }
      result = validate_exemplars(point.exemplars(), ctx);
      if (result.is_err()) {
        return result;
      }
    }
  } else if (metric.has_summary()) {
    for (auto const& point : metric.summary().data_points()) {
      result = validate_point_common(point, ctx);
      if (result.is_err()) {
        return result;
      }
      if (has_no_recorded_value(point)) {
        continue;
      }
      result = validate_aggregate_sum(point.count(), point.sum());
      if (result.is_err()) {
        return result;
      }
      auto previous = -1.0;
      for (auto const& quantile : point.quantile_values()) {
        if (ctx.is_cancelled()) {
          return Err{std::string{cancelled_error}};
        }
        auto const coordinate = quantile.quantile();
        if (not(coordinate >= 0.0 and coordinate <= 1.0
                and coordinate > previous)) {
          return Err{std::string{"summary quantiles must be unique, sorted, "
                                 "and between 0 and 1"}};
        }
        if (not(quantile.value() >= 0.0)) {
          return Err{
            std::string{"summary quantile values must be non-negative"}};
        }
        previous = coordinate;
      }
    }
  }
  return Empty{};
}

auto metric_repeated_size(metrics::Metric const& metric) -> size_t {
  auto result
    = metric.name().size() + metric.description().size() + metric.unit().size();
  for (auto const& attribute : metric.metadata()) {
    result += attribute.ByteSizeLong();
  }
  return result;
}

auto materialize_metrics(collector_metrics::ExportMetricsServiceRequest request,
                         DecodeContext ctx) -> DecodedSlices {
  auto builder = Option<series_builder>{None{}};
  auto active_kind = metrics::Metric::DATA_NOT_SET;
  for (auto const& resource_metrics : request.resource_metrics()) {
    for (auto const& scope_metrics : resource_metrics.scope_metrics()) {
      for (auto const& metric : scope_metrics.metrics()) {
        if (ctx.is_cancelled()) {
          co_yield DecodedSlice{Err{std::string{cancelled_error}}};
          co_return;
        }
        auto const kind = metric.data_case();
        if (kind != active_kind) {
          if (builder) {
            for (auto& slice : builder->finish_as_table_slice()) {
              co_yield DecodedSlice{std::move(slice)};
            }
            builder.reset();
          }
          active_kind = kind;
          switch (kind) {
            case metrics::Metric::kGauge:
              builder.emplace(gauge_type(ctx.attribute_mode));
              break;
            case metrics::Metric::kSum:
              builder.emplace(sum_type(ctx.attribute_mode));
              break;
            case metrics::Metric::kHistogram:
              builder.emplace(histogram_type(ctx.attribute_mode));
              break;
            case metrics::Metric::kExponentialHistogram:
              builder.emplace(exponential_histogram_type(ctx.attribute_mode));
              break;
            case metrics::Metric::kSummary:
              builder.emplace(summary_type(ctx.attribute_mode));
              break;
            case metrics::Metric::DATA_NOT_SET:
              co_yield DecodedSlice{
                Err{fmt::format("metric `{}` has no data", metric.name())}};
              co_return;
          }
        }
        auto const batch_rows
          = batch_row_limit(resource_metrics.resource(),
                            resource_metrics.schema_url(),
                            scope_metrics.scope(), scope_metrics.schema_url(),
                            ctx, metric_repeated_size(metric));
        auto make_event = [&]() -> Result<record, std::string> {
          return with_context(resource_metrics.resource(),
                              resource_metrics.schema_url(),
                              scope_metrics.scope(), scope_metrics.schema_url(),
                              ctx);
        };
        auto make_point_event
          = [&](auto const& point, bool ignore_start_time
                                   = false) -> Result<record, std::string> {
          TRY(auto event, make_event());
          TRY(add_metric_common(event, metric, point, ctx, ignore_start_time));
          return event;
        };
        auto add_exemplars
          = [&](record& event, auto const& point) -> Result<void, std::string> {
          if (has_no_recorded_value(point)) {
            event["exemplars"] = data{};
          } else {
            TRY(auto exemplars, make_exemplars(point.exemplars(), ctx));
            event["exemplars"] = std::move(exemplars);
          }
          return {};
        };
        auto flush_if_ready = [&]() -> std::vector<table_slice> {
          if (builder->length() < batch_rows) {
            return {};
          }
          return builder->finish_as_table_slice();
        };
        if (metric.has_gauge()) {
          for (auto const& point : metric.gauge().data_points()) {
            if (ctx.is_cancelled()) {
              co_yield DecodedSlice{Err{std::string{cancelled_error}}};
              co_return;
            }
            auto event = make_point_event(point, true);
            if (event.is_err()) {
              co_yield DecodedSlice{Err{std::move(event).unwrap_err()}};
              co_return;
            }
            auto materialized = std::move(event).unwrap();
            materialized["value"] = make_number_value(point);
            auto exemplars = add_exemplars(materialized, point);
            if (exemplars.is_err()) {
              co_yield DecodedSlice{Err{std::move(exemplars).unwrap_err()}};
              co_return;
            }
            materialized["flags"] = static_cast<uint64_t>(point.flags());
            builder->data(materialized);
            for (auto& slice : flush_if_ready()) {
              co_yield DecodedSlice{std::move(slice)};
            }
          }
        } else if (metric.has_sum()) {
          for (auto const& point : metric.sum().data_points()) {
            if (ctx.is_cancelled()) {
              co_yield DecodedSlice{Err{std::string{cancelled_error}}};
              co_return;
            }
            auto event = make_point_event(point);
            if (event.is_err()) {
              co_yield DecodedSlice{Err{std::move(event).unwrap_err()}};
              co_return;
            }
            auto materialized = std::move(event).unwrap();
            materialized["value"] = make_number_value(point);
            auto exemplars = add_exemplars(materialized, point);
            if (exemplars.is_err()) {
              co_yield DecodedSlice{Err{std::move(exemplars).unwrap_err()}};
              co_return;
            }
            materialized["flags"] = static_cast<uint64_t>(point.flags());
            materialized["aggregation_temporality_id"]
              = static_cast<int64_t>(metric.sum().aggregation_temporality());
            materialized["aggregation_temporality"]
              = temporality_name(metric.sum().aggregation_temporality());
            materialized["monotonic"] = metric.sum().is_monotonic();
            builder->data(materialized);
            for (auto& slice : flush_if_ready()) {
              co_yield DecodedSlice{std::move(slice)};
            }
          }
        } else if (metric.has_histogram()) {
          for (auto const& point : metric.histogram().data_points()) {
            if (ctx.is_cancelled()) {
              co_yield DecodedSlice{Err{std::string{cancelled_error}}};
              co_return;
            }
            auto event = make_point_event(point);
            if (event.is_err()) {
              co_yield DecodedSlice{Err{std::move(event).unwrap_err()}};
              co_return;
            }
            auto materialized = std::move(event).unwrap();
            materialized["aggregation_temporality_id"] = static_cast<int64_t>(
              metric.histogram().aggregation_temporality());
            materialized["aggregation_temporality"]
              = temporality_name(metric.histogram().aggregation_temporality());
            if (has_no_recorded_value(point)) {
              materialized["count"] = data{};
              materialized["sum"] = data{};
              materialized["min"] = data{};
              materialized["max"] = data{};
              materialized["bucket_counts"] = data{};
              materialized["explicit_bounds"] = data{};
              materialized["exemplars"] = data{};
            } else {
              materialized["count"] = point.count();
              materialized["sum"]
                = point.has_sum() ? data{point.sum()} : data{};
              materialized["min"]
                = point.has_min() ? data{point.min()} : data{};
              materialized["max"]
                = point.has_max() ? data{point.max()} : data{};
              auto counts = make_u64_list(point.bucket_counts(), ctx);
              if (counts.is_err()) {
                co_yield DecodedSlice{Err{std::move(counts).unwrap_err()}};
                co_return;
              }
              materialized["bucket_counts"] = std::move(counts).unwrap();
              auto bounds = make_double_list(point.explicit_bounds(), ctx);
              if (bounds.is_err()) {
                co_yield DecodedSlice{Err{std::move(bounds).unwrap_err()}};
                co_return;
              }
              materialized["explicit_bounds"] = std::move(bounds).unwrap();
              auto exemplars = make_exemplars(point.exemplars(), ctx);
              if (exemplars.is_err()) {
                co_yield DecodedSlice{Err{std::move(exemplars).unwrap_err()}};
                co_return;
              }
              materialized["exemplars"] = std::move(exemplars).unwrap();
            }
            materialized["flags"] = static_cast<uint64_t>(point.flags());
            builder->data(materialized);
            for (auto& slice : flush_if_ready()) {
              co_yield DecodedSlice{std::move(slice)};
            }
          }
        } else if (metric.has_exponential_histogram()) {
          for (auto const& point :
               metric.exponential_histogram().data_points()) {
            if (ctx.is_cancelled()) {
              co_yield DecodedSlice{Err{std::string{cancelled_error}}};
              co_return;
            }
            auto event = make_point_event(point);
            if (event.is_err()) {
              co_yield DecodedSlice{Err{std::move(event).unwrap_err()}};
              co_return;
            }
            auto materialized = std::move(event).unwrap();
            materialized["aggregation_temporality_id"] = static_cast<int64_t>(
              metric.exponential_histogram().aggregation_temporality());
            materialized["aggregation_temporality"] = temporality_name(
              metric.exponential_histogram().aggregation_temporality());
            if (has_no_recorded_value(point)) {
              materialized["count"] = data{};
              materialized["sum"] = data{};
              materialized["min"] = data{};
              materialized["max"] = data{};
              materialized["scale"] = data{};
              materialized["zero_count"] = data{};
              materialized["zero_threshold"] = data{};
              materialized["positive"] = data{};
              materialized["negative"] = data{};
              materialized["exemplars"] = data{};
            } else {
              materialized["count"] = point.count();
              materialized["sum"]
                = point.has_sum() ? data{point.sum()} : data{};
              materialized["min"]
                = point.has_min() ? data{point.min()} : data{};
              materialized["max"]
                = point.has_max() ? data{point.max()} : data{};
              materialized["scale"] = static_cast<int64_t>(point.scale());
              materialized["zero_count"] = point.zero_count();
              materialized["zero_threshold"] = point.zero_threshold();
              auto positive = make_buckets(point.positive(), ctx);
              if (positive.is_err()) {
                co_yield DecodedSlice{Err{std::move(positive).unwrap_err()}};
                co_return;
              }
              materialized["positive"] = std::move(positive).unwrap();
              auto negative = make_buckets(point.negative(), ctx);
              if (negative.is_err()) {
                co_yield DecodedSlice{Err{std::move(negative).unwrap_err()}};
                co_return;
              }
              materialized["negative"] = std::move(negative).unwrap();
              auto exemplars = make_exemplars(point.exemplars(), ctx);
              if (exemplars.is_err()) {
                co_yield DecodedSlice{Err{std::move(exemplars).unwrap_err()}};
                co_return;
              }
              materialized["exemplars"] = std::move(exemplars).unwrap();
            }
            materialized["flags"] = static_cast<uint64_t>(point.flags());
            builder->data(materialized);
            for (auto& slice : flush_if_ready()) {
              co_yield DecodedSlice{std::move(slice)};
            }
          }
        } else if (metric.has_summary()) {
          for (auto const& point : metric.summary().data_points()) {
            if (ctx.is_cancelled()) {
              co_yield DecodedSlice{Err{std::string{cancelled_error}}};
              co_return;
            }
            auto event = make_point_event(point);
            if (event.is_err()) {
              co_yield DecodedSlice{Err{std::move(event).unwrap_err()}};
              co_return;
            }
            auto materialized = std::move(event).unwrap();
            if (has_no_recorded_value(point)) {
              materialized["count"] = data{};
              materialized["sum"] = data{};
              materialized["quantile_values"] = data{};
            } else {
              materialized["count"] = point.count();
              materialized["sum"] = point.sum();
              auto quantiles = list{};
              for (auto const& quantile : point.quantile_values()) {
                if (ctx.is_cancelled()) {
                  co_yield DecodedSlice{Err{std::string{cancelled_error}}};
                  co_return;
                }
                quantiles.emplace_back(record{{"quantile", quantile.quantile()},
                                              {"value", quantile.value()}});
              }
              materialized["quantile_values"] = std::move(quantiles);
            }
            materialized["flags"] = static_cast<uint64_t>(point.flags());
            builder->data(materialized);
            for (auto& slice : flush_if_ready()) {
              co_yield DecodedSlice{std::move(slice)};
            }
          }
        }
      }
    }
  }
  if (ctx.is_cancelled()) {
    co_yield DecodedSlice{Err{std::string{cancelled_error}}};
    co_return;
  }
  if (builder) {
    for (auto& slice : builder->finish_as_table_slice()) {
      co_yield DecodedSlice{std::move(slice)};
    }
  }
}

auto decode_metrics(collector_metrics::ExportMetricsServiceRequest request,
                    DecodeContext ctx) -> DecodeResult {
  auto const unique = ctx.attribute_mode == AttributeMode::record;
  for (auto const& resource_metrics : request.resource_metrics()) {
    if (ctx.is_cancelled()) {
      return Err{std::string{cancelled_error}};
    }
    auto valid = validate_resource(resource_metrics.resource(), unique, &ctx);
    if (valid.is_err()) {
      return Err{std::move(valid).unwrap_err()};
    }
    for (auto const& scope_metrics : resource_metrics.scope_metrics()) {
      if (ctx.is_cancelled()) {
        return Err{std::string{cancelled_error}};
      }
      valid
        = validate_attributes(scope_metrics.scope().attributes(), unique, &ctx);
      if (valid.is_err()) {
        return Err{std::move(valid).unwrap_err()};
      }
      for (auto const& metric : scope_metrics.metrics()) {
        valid = validate_metric(metric, ctx);
        if (valid.is_err()) {
          return Err{std::move(valid).unwrap_err()};
        }
      }
    }
  }
  return materialize_metrics(std::move(request), std::move(ctx));
}

} // namespace tenzir::plugins::accept_otlp::detail
