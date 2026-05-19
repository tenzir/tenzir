//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/prometheus_metric_shaper.hpp"

#include "tenzir/detail/overload.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/option.hpp"
#include "tenzir/plugin/metrics.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/time.hpp"
#include "tenzir/type.hpp"
#include "tenzir/view3.hpp"

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

struct label {
  std::string key;
  std::string value;

  auto operator==(label const&) const -> bool = default;
};

using labels = std::vector<label>;

struct metric_sample {
  std::string metric;
  double value = {};
  Option<time> timestamp = None{};
  labels metric_labels;
  std::string type = "gauge";
  std::string unit = "";
};

auto prometheus_schema() -> type const& {
  static auto const result = type{
    "tenzir.metrics.prometheus",
    record_type{
      {"metric", string_type{}},
      {"value", double_type{}},
      {"timestamp", time_type{}},
      {"labels", record_type{}},
      {"type", string_type{}},
      {"unit", string_type{}},
    },
    {{"internal", ""}},
  };
  return result;
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

auto add_label(labels& result, std::string_view key, std::string value)
  -> void {
  auto sanitized = sanitize_identifier(
    key,
    [](unsigned char c) {
      return ascii_isalnum(c) or c == '_';
    },
    false);
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

auto collect_labels(record_type const& schema, view3<record> record,
                    labels const& inherited) -> labels {
  auto result = inherited;
  for (auto [key, value] : record) {
    auto field_type = schema.field(key);
    if (not field_type
        or prometheus_role_of(*field_type) != prometheus_role::label) {
      continue;
    }
    if (auto label_value = stringify_label_value(value)) {
      add_label(result, key, std::move(*label_value));
    }
  }
  return result;
}

auto make_metric_name(std::string_view source,
                      std::vector<std::string> const& path,
                      metric_descriptor const& descriptor) -> std::string {
  auto raw = fmt::format("tenzir_{}_{}", source, join(path, "_"));
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
                       std::string_view source,
                       std::vector<std::string> const& path, double value,
                       Option<time> timestamp, labels const& metric_labels,
                       metric_descriptor descriptor) -> void {
  auto sample = metric_sample{
    .metric = make_metric_name(source, path, descriptor),
    .value = value,
    .timestamp = timestamp,
    .metric_labels = metric_labels,
    .type = std::move(descriptor.type),
    .unit = std::move(descriptor.unit),
  };
  for (auto& existing : samples) {
    if (same_series(existing, sample)) {
      existing.value += sample.value;
      return;
    }
  }
  samples.push_back(std::move(sample));
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

auto flatten_value(std::vector<metric_sample>& samples, std::string_view source,
                   std::vector<std::string>& path, data_view3 value,
                   type const& field_type, Option<time> timestamp,
                   labels const& labels) -> void;

auto flatten_record(std::vector<metric_sample>& samples,
                    std::string_view source, std::vector<std::string>& path,
                    view3<record> record, record_type const& schema,
                    Option<time> timestamp, labels const& inherited) -> void {
  auto scoped_labels = collect_labels(schema, record, inherited);
  for (auto [key, value] : record) {
    auto field_type = schema.field(key);
    if (not field_type) {
      continue;
    }
    auto const role = prometheus_role_of(*field_type);
    if (role == prometheus_role::ignore or role == prometheus_role::label) {
      continue;
    }
    path.push_back(std::string{key});
    flatten_value(samples, source, path, value, *field_type, timestamp,
                  scoped_labels);
    path.pop_back();
  }
}

auto flatten_list(std::vector<metric_sample>& samples, std::string_view source,
                  std::vector<std::string>& path, view3<list> list,
                  list_type const& schema, Option<time> timestamp,
                  labels const& labels) -> void {
  auto const value_type = schema.value_type();
  auto const* record_schema = try_as<record_type>(&value_type);
  if (not record_schema) {
    return;
  }
  for (auto item : list) {
    if (auto const* item_record = try_as<view3<record>>(item)) {
      flatten_record(samples, source, path, *item_record, *record_schema,
                     timestamp, labels);
    }
  }
}

auto flatten_value(std::vector<metric_sample>& samples, std::string_view source,
                   std::vector<std::string>& path, data_view3 value,
                   type const& field_type, Option<time> timestamp,
                   labels const& labels) -> void {
  if (prometheus_role_of(field_type) == prometheus_role::ignore) {
    return;
  }
  if (auto const* record_schema = try_as<record_type>(&field_type)) {
    if (auto const* record_value = try_as<view3<record>>(value)) {
      flatten_record(samples, source, path, *record_value, *record_schema,
                     timestamp, labels);
    }
    return;
  }
  if (auto const* list_schema = try_as<list_type>(&field_type)) {
    if (auto const* list_value = try_as<view3<list>>(value)) {
      flatten_list(samples, source, path, *list_value, *list_schema, timestamp,
                   labels);
    }
    return;
  }
  if (prometheus_role_of(field_type) != prometheus_role::metric) {
    return;
  }
  auto descriptor = descriptor_of(field_type);
  match(
    value, [&](caf::none_t) {},
    [&](int64_t value) {
      add_metric_sample(samples, source, path, static_cast<double>(value),
                        timestamp, labels, descriptor);
    },
    [&](uint64_t value) {
      add_metric_sample(samples, source, path, static_cast<double>(value),
                        timestamp, labels, descriptor);
    },
    [&](double value) {
      add_metric_sample(samples, source, path, value, timestamp, labels,
                        descriptor);
    },
    [&](duration value) {
      add_metric_sample(
        samples, source, path,
        std::chrono::duration_cast<double_seconds>(value).count(), timestamp,
        labels, descriptor);
    },
    [](auto) {});
}

auto find_timestamp(view3<record> record) -> Option<time> {
  for (auto [key, value] : record) {
    if (key != "timestamp") {
      continue;
    }
    if (auto* result = try_as<time>(value)) {
      return *result;
    }
    return None{};
  }
  return None{};
}

} // namespace

prometheus_metric_shaper::prometheus_metric_shaper(type schema)
  : schema_{std::move(schema)} {
}

auto prometheus_metric_shaper::shape(table_slice const& input) const
  -> std::vector<table_slice> {
  auto samples = std::vector<metric_sample>{};
  auto path = std::vector<std::string>{};
  auto const source = metric_source(schema_.name());
  auto const* schema = try_as<record_type>(&schema_);
  if (not schema) {
    return {};
  }
  for (auto row : input.values()) {
    flatten_record(samples, source, path, row, *schema, find_timestamp(row),
                   {});
  }
  auto builder = series_builder{prometheus_schema()};
  for (auto const& sample : samples) {
    append_metric(builder, sample);
  }
  return builder.finish_as_table_slice();
}

auto shape_metrics_for_prometheus(table_slice const& input)
  -> std::vector<table_slice> {
  return prometheus_metric_shaper{input.schema()}.shape(input);
}

} // namespace tenzir::detail
