//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/prometheus_metric_shaper.hpp"

#include "tenzir/arrow_memory_pool.hpp"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/option.hpp"
#include "tenzir/plugin/metrics.hpp"
#include "tenzir/series.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/time.hpp"
#include "tenzir/type.hpp"
#include "tenzir/value_path.hpp"
#include "tenzir/view3.hpp"

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/compute/api_scalar.h>
#include <arrow/compute/api_vector.h>
#include <arrow/record_batch.h>
#include <fmt/format.h>

#include <chrono>
#include <string>
#include <string_view>
#include <vector>

namespace tenzir::detail {

namespace {

using namespace std::string_view_literals;

enum class prometheus_role {
  none,
  ignore,
  label,
  metric,
};

struct metric_descriptor {
  std::string type = "gauge";
  std::string unit = "";
  bool suffix_seconds = false;
};

struct sample_label {
  std::string key;
  std::string value;

  auto operator==(sample_label const&) const -> bool = default;
};

using sample_labels = std::vector<sample_label>;

struct label_column {
  std::string key;
  series data;
};

using label_columns = std::vector<label_column>;

struct metric_sample {
  std::string metric;
  double value = {};
  Option<time> timestamp = None{};
  sample_labels metric_labels;
  std::string type = "gauge";
  std::string unit = "";
};

struct shape_context {
  series timestamp;
  label_columns labels;
};

auto prometheus_schema(record_type labels_type) -> type {
  return type{
    "tenzir.metrics.prometheus",
    record_type{
      {"metric", string_type{}},
      {"value", double_type{}},
      {"timestamp", time_type{}},
      {"labels", std::move(labels_type)},
      {"type", string_type{}},
      {"unit", string_type{}},
    },
    {{"internal", ""}},
  };
}

auto metric_source(std::string_view schema) -> std::string_view {
  constexpr auto prefix = "tenzir.metrics."sv;
  if (schema.starts_with(prefix)) {
    return schema.substr(prefix.size());
  }
  return schema;
}

template <class Predicate>
auto sanitize_identifier(std::string_view input, Predicate is_valid,
                         bool allow_colon) -> std::string {
  auto result = std::string{};
  result.reserve(input.size());
  auto last_was_underscore = false;
  for (auto c : input) {
    auto const uc = static_cast<unsigned char>(c);
    auto const next = is_valid(uc) and (allow_colon or c != ':') ? c : '_';
    if (next == '_' and last_was_underscore) {
      continue;
    }
    result.push_back(next);
    last_was_underscore = next == '_';
  }
  while (not result.empty() and result.back() == '_') {
    result.pop_back();
  }
  if (result.empty()) {
    return "_";
  }
  auto const first = static_cast<unsigned char>(result.front());
  if (ascii_isdigit(first) or (not allow_colon and result.front() == ':')) {
    result.insert(result.begin(), '_');
  }
  return result;
}

auto sanitize_label_key(std::string_view key) -> std::string {
  return sanitize_identifier(
    key,
    [](unsigned char c) {
      return ascii_isalnum(c) or c == '_';
    },
    false);
}

auto prometheus_role_of(type const& field_type) -> prometheus_role {
  auto const role = field_type.attribute(metrics::prometheus_role_attribute);
  if (not role) {
    return prometheus_role::none;
  }
  if (*role == "ignore") {
    return prometheus_role::ignore;
  }
  if (*role == "label") {
    return prometheus_role::label;
  }
  if (*role == "metric") {
    return prometheus_role::metric;
  }
  return prometheus_role::none;
}

auto descriptor_of(type const& field_type) -> metric_descriptor {
  auto result = metric_descriptor{};
  if (auto prometheus_type
      = field_type.attribute(metrics::prometheus_type_attribute)) {
    result.type = *prometheus_type;
  }
  if (auto unit = field_type.attribute(metrics::prometheus_unit_attribute)) {
    result.unit = *unit;
    result.suffix_seconds = *unit == "seconds";
  }
  return result;
}

auto stringify_label_value(data_view3 value) -> Option<std::string> {
  return match(
    value,
    [](caf::none_t) -> Option<std::string> {
      return None{};
    },
    [](bool value) -> Option<std::string> {
      return value ? std::string{"true"} : std::string{"false"};
    },
    [](int64_t value) -> Option<std::string> {
      return fmt::format("{}", value);
    },
    [](uint64_t value) -> Option<std::string> {
      return fmt::format("{}", value);
    },
    [](double value) -> Option<std::string> {
      return fmt::format("{}", value);
    },
    [](duration value) -> Option<std::string> {
      return fmt::format("{}", value);
    },
    [](time value) -> Option<std::string> {
      return fmt::format("{}", value);
    },
    [](std::string_view value) -> Option<std::string> {
      return std::string{value};
    },
    [](enumeration value) -> Option<std::string> {
      return fmt::format("{}", value);
    },
    [](ip value) -> Option<std::string> {
      return fmt::format("{}", value);
    },
    [](subnet value) -> Option<std::string> {
      return fmt::format("{}", value);
    },
    [](auto) -> Option<std::string> {
      return None{};
    });
}

auto add_label(sample_labels& result, std::string_view key, std::string value)
  -> void {
  auto sanitized = sanitize_label_key(key);
  for (auto& item : result) {
    if (item.key == sanitized) {
      item.value = std::move(value);
      return;
    }
  }
  result.push_back({
    .key = std::move(sanitized),
    .value = std::move(value),
  });
}

auto make_metric_name(std::string_view source, value_path const& path,
                      metric_descriptor const& descriptor) -> std::string {
  auto formatted_path = fmt::format("{}", path);
  if (formatted_path.starts_with("this.")) {
    formatted_path.erase(0, "this."sv.size());
  }
  auto raw = fmt::format("tenzir_{}_{}", source, formatted_path);
  auto result = sanitize_identifier(
    raw,
    [](unsigned char c) {
      return ascii_isalnum(c) or c == '_' or c == ':';
    },
    true);
  if (descriptor.suffix_seconds and not result.ends_with("_seconds")) {
    result += "_seconds";
  }
  return result;
}

auto make_string_array(std::string_view value, int64_t length)
  -> std::shared_ptr<arrow::Array> {
  return check(arrow::MakeArrayFromScalar(
    arrow::StringScalar{std::string{value}}, length, arrow_memory_pool()));
}

auto make_labels_array(label_columns const& labels, int64_t length)
  -> std::pair<record_type, std::shared_ptr<arrow::StructArray>> {
  auto fields = std::vector<record_type::field_view>{};
  auto arrow_fields = arrow::FieldVector{};
  auto children = arrow::ArrayVector{};
  fields.reserve(labels.size());
  arrow_fields.reserve(labels.size());
  children.reserve(labels.size());
  for (auto const& label : labels) {
    TENZIR_ASSERT_EQ(label.data.length(), length);
    fields.emplace_back(label.key, string_type{});
    arrow_fields.push_back(type{string_type{}}.to_arrow_field(label.key));
    children.push_back(label.data.array);
  }
  auto labels_type = record_type{std::move(fields)};
  auto labels_array = std::make_shared<arrow::StructArray>(
    arrow::struct_(std::move(arrow_fields)), length, std::move(children));
  return {std::move(labels_type), std::move(labels_array)};
}

auto make_timestamp_series(basic_series<record_type> const& input) -> series {
  auto timestamp = input.field("timestamp");
  if (timestamp and is<time_type>(timestamp->type)) {
    return *timestamp;
  }
  return series::null(time_type{}, input.length());
}

auto stringify_label_series(series const& input) -> Option<series> {
  if (is<string_type>(input.type)) {
    return input;
  }
  auto result = arrow::compute::Cast(
    *input.array, arrow::TypeHolder{string_type{}.to_arrow_type()});
  if (not result.ok()) {
    return None{};
  }
  return series{string_type{}, result.MoveValueUnsafe()};
}

auto add_label(label_columns& result, std::string_view key, series data)
  -> void {
  auto strings = stringify_label_series(data);
  if (not strings) {
    return;
  }
  auto sanitized = sanitize_label_key(key);
  for (auto& item : result) {
    if (item.key == sanitized) {
      item.data = std::move(*strings);
      return;
    }
  }
  result.push_back({
    .key = std::move(sanitized),
    .data = std::move(*strings),
  });
}

auto value_to_double_series(series const& input) -> Option<series> {
  auto cast_to_double = [&]() -> Option<series> {
    auto result = arrow::compute::Cast(
      *input.array, arrow::TypeHolder{double_type{}.to_arrow_type()});
    if (not result.ok()) {
      return None{};
    }
    return series{double_type{}, result.MoveValueUnsafe()};
  };
  if (is<int64_type>(input.type) or is<uint64_type>(input.type)
      or is<double_type>(input.type)) {
    return cast_to_double();
  }
  if (not is<duration_type>(input.type)) {
    return None{};
  }
  auto builder = double_type::make_arrow_builder(arrow_memory_pool());
  check(builder->Reserve(input.length()));
  for (auto value : input.values<duration_type>()) {
    if (not value) {
      check(builder->AppendNull());
      continue;
    }
    check(builder->Append(
      std::chrono::duration_cast<double_seconds>(*value).count()));
  }
  return series{double_type{}, finish(*builder)};
}

auto filter_null_values(table_slice const& input) -> table_slice {
  if (input.rows() == 0) {
    return {};
  }
  auto batch = to_record_batch(input);
  auto value = batch->GetColumnByName("value");
  TENZIR_ASSERT(value);
  if (value->null_count() == 0) {
    return input;
  }
  auto mask = check(arrow::compute::IsValid(value)).make_array();
  auto const* bool_mask = try_as<arrow::BooleanArray>(&*mask);
  TENZIR_ASSERT(bool_mask);
  return filter(input, *bool_mask);
}

auto same_series(metric_sample const& lhs, metric_sample const& rhs) -> bool {
  if (lhs.metric != rhs.metric or lhs.type != rhs.type or lhs.unit != rhs.unit
      or static_cast<bool>(lhs.timestamp) != static_cast<bool>(rhs.timestamp)) {
    return false;
  }
  if (lhs.timestamp and *lhs.timestamp != *rhs.timestamp) {
    return false;
  }
  return lhs.metric_labels == rhs.metric_labels;
}

auto add_metric_sample(std::vector<metric_sample>& samples,
                       metric_sample sample) -> bool {
  for (auto& existing : samples) {
    if (same_series(existing, sample)) {
      existing.value += sample.value;
      return true;
    }
  }
  samples.push_back(std::move(sample));
  return false;
}

auto append_metric(series_builder& builder, metric_sample const& sample)
  -> void {
  auto metric = builder.record();
  metric.field("metric", sample.metric);
  metric.field("value", sample.value);
  if (sample.timestamp) {
    metric.field("timestamp", *sample.timestamp);
  } else {
    metric.field("timestamp", caf::none);
  }
  auto labels_record = metric.field("labels").record();
  for (auto const& [key, label_value] : sample.metric_labels) {
    labels_record.field(key, label_value);
  }
  metric.field("type", sample.type);
  metric.field("unit", sample.unit);
}

auto aggregate_duplicate_series(table_slice const& input) -> table_slice {
  auto samples = std::vector<metric_sample>{};
  auto duplicate = false;
  for (auto row : input.values()) {
    auto sample = metric_sample{};
    for (auto [key, value] : row) {
      if (key == "metric") {
        if (auto const* metric = try_as<std::string_view>(value)) {
          sample.metric = std::string{*metric};
        }
      } else if (key == "value") {
        if (auto const* metric_value = try_as<double>(value)) {
          sample.value = *metric_value;
        }
      } else if (key == "timestamp") {
        if (auto const* timestamp = try_as<time>(value)) {
          sample.timestamp = *timestamp;
        }
      } else if (key == "labels") {
        if (auto const* labels_record = try_as<view3<record>>(value)) {
          for (auto [label_key, label_value] : *labels_record) {
            if (auto string_value = stringify_label_value(label_value)) {
              add_label(sample.metric_labels, label_key,
                        std::move(*string_value));
            }
          }
        }
      } else if (key == "type") {
        if (auto const* type = try_as<std::string_view>(value)) {
          sample.type = std::string{*type};
        }
      } else if (key == "unit") {
        if (auto const* unit = try_as<std::string_view>(value)) {
          sample.unit = std::string{*unit};
        }
      }
    }
    duplicate |= add_metric_sample(samples, std::move(sample));
  }
  if (not duplicate) {
    return input;
  }
  auto builder = series_builder{input.schema()};
  for (auto const& sample : samples) {
    append_metric(builder, sample);
  }
  return builder.finish_assert_one_slice();
}

auto make_metric_slice(std::string_view source, value_path const& path,
                       series const& value, type const& field_type,
                       shape_context const& ctx) -> table_slice {
  auto values = value_to_double_series(value);
  if (not values) {
    return {};
  }
  auto const length = values->length();
  TENZIR_ASSERT_EQ(ctx.timestamp.length(), length);
  auto const descriptor = descriptor_of(field_type);
  auto [labels_type, labels_array] = make_labels_array(ctx.labels, length);
  auto schema = prometheus_schema(labels_type);
  auto columns = arrow::ArrayVector{
    make_string_array(make_metric_name(source, path, descriptor), length),
    values->array,
    ctx.timestamp.array,
    std::move(labels_array),
    make_string_array(descriptor.type, length),
    make_string_array(descriptor.unit, length),
  };
  auto batch = arrow::RecordBatch::Make(schema.to_arrow_schema(), length,
                                        std::move(columns));
  auto result = filter_null_values(table_slice{batch, std::move(schema)});
  if (result.rows() == 0) {
    return {};
  }
  return aggregate_duplicate_series(result);
}

auto take_series(series const& input, arrow::Array const& indices) -> series {
  auto taken = check(arrow::compute::Take(input.array, indices)).make_array();
  return series{input.type, std::move(taken)};
}

auto make_list_indices(arrow::ListArray const& list)
  -> std::pair<std::shared_ptr<arrow::Array>, std::shared_ptr<arrow::Array>> {
  auto parent_builder = arrow::UInt64Builder{arrow_memory_pool()};
  auto value_builder = arrow::UInt64Builder{arrow_memory_pool()};
  auto value_count = int64_t{0};
  for (auto row = int64_t{0}; row < list.length(); ++row) {
    if (list.IsNull(row)) {
      continue;
    }
    value_count += list.value_length(row);
  }
  check(parent_builder.Reserve(value_count));
  check(value_builder.Reserve(value_count));
  for (auto row = int64_t{0}; row < list.length(); ++row) {
    if (list.IsNull(row)) {
      continue;
    }
    auto const begin = list.value_offset(row);
    auto const end = begin + list.value_length(row);
    for (auto value = begin; value < end; ++value) {
      check(parent_builder.Append(detail::narrow<uint64_t>(row)));
      check(value_builder.Append(detail::narrow<uint64_t>(value)));
    }
  }
  return {finish(parent_builder), finish(value_builder)};
}

auto enter_list(shape_context const& ctx, arrow::Array const& parent_indices)
  -> shape_context {
  auto result = shape_context{
    .timestamp = take_series(ctx.timestamp, parent_indices),
    .labels = {},
  };
  result.labels.reserve(ctx.labels.size());
  for (auto const& label : ctx.labels) {
    result.labels.push_back({
      .key = label.key,
      .data = take_series(label.data, parent_indices),
    });
  }
  return result;
}

auto take_list_values(series const& input, arrow::Array const& value_indices)
  -> series {
  auto const* list = try_as<list_type>(&input.type);
  TENZIR_ASSERT(list);
  auto list_array = std::static_pointer_cast<arrow::ListArray>(input.array);
  auto values = check(arrow::compute::Take(list_array->values(), value_indices))
                  .make_array();
  return series{list->value_type(), std::move(values)};
}

auto shape_value(std::vector<table_slice>& result, std::string_view source,
                 series const& value, type const& field_type,
                 shape_context const& ctx, value_path path) -> void;

auto shape_record(std::vector<table_slice>& result, std::string_view source,
                  basic_series<record_type> const& record,
                  shape_context const& ctx, value_path path) -> void {
  auto scoped_ctx = ctx;
  for (auto const& field : record.type.fields()) {
    if (prometheus_role_of(field.type) != prometheus_role::label) {
      continue;
    }
    if (auto field_series = record.field(field.name)) {
      add_label(scoped_ctx.labels, field.name, std::move(*field_series));
    }
  }
  for (auto const& field : record.type.fields()) {
    auto const role = prometheus_role_of(field.type);
    if (role == prometheus_role::ignore or role == prometheus_role::label) {
      continue;
    }
    if (auto field_series = record.field(field.name)) {
      shape_value(result, source, *field_series, field.type, scoped_ctx,
                  path.field(field.name));
    }
  }
}

auto shape_value(std::vector<table_slice>& result, std::string_view source,
                 series const& value, type const& field_type,
                 shape_context const& ctx, value_path path) -> void {
  auto const role = prometheus_role_of(field_type);
  if (role == prometheus_role::ignore) {
    return;
  }
  if (auto record = value.as<record_type>()) {
    shape_record(result, source, *record, ctx, std::move(path));
    return;
  }
  if (auto list = value.as<list_type>()) {
    auto const value_type = list->type.value_type();
    if (not try_as<record_type>(&value_type)) {
      return;
    }
    auto list_array = std::static_pointer_cast<arrow::ListArray>(list->array);
    auto [parent_indices, value_indices] = make_list_indices(*list_array);
    auto list_ctx = enter_list(ctx, *parent_indices);
    auto list_values = take_list_values(value, *value_indices);
    auto records = list_values.as<record_type>();
    TENZIR_ASSERT(records);
    shape_record(result, source, *records, list_ctx, path.list());
    return;
  }
  if (role != prometheus_role::metric) {
    return;
  }
  auto output = make_metric_slice(source, path, value, field_type, ctx);
  if (output.rows() > 0) {
    result.push_back(std::move(output));
  }
}

} // namespace

prometheus_metric_shaper::prometheus_metric_shaper(type schema)
  : schema_{std::move(schema)} {
}

auto prometheus_metric_shaper::shape(table_slice const& input) const
  -> std::vector<table_slice> {
  if (input.rows() == 0 or not is<record_type>(schema_)) {
    return {};
  }
  auto record = basic_series<record_type>{input};
  auto result = std::vector<table_slice>{};
  auto ctx = shape_context{
    .timestamp = make_timestamp_series(record),
    .labels = {},
  };
  shape_record(result, metric_source(schema_.name()), record, ctx,
               value_path{});
  return result;
}

auto shape_metrics_for_prometheus(table_slice const& input)
  -> std::vector<table_slice> {
  return prometheus_metric_shaper{input.schema()}.shape(input);
}

} // namespace tenzir::detail
