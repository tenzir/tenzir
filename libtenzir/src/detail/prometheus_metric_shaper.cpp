//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/prometheus_metric_shaper.hpp"

#include "tenzir/detail/overload.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/time.hpp"
#include "tenzir/type.hpp"
#include "tenzir/view3.hpp"

#include <fmt/format.h>

#include <array>
#include <cctype>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <vector>

namespace tenzir::detail {

namespace {

struct metric_descriptor {
  std::string_view type = "gauge";
  std::string_view unit = "";
  bool suffix_seconds = false;
};

struct label {
  std::string key;
  std::string value;
};

using labels = std::vector<label>;

auto prometheus_schema() -> const type& {
  static const auto result = type{
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
  constexpr auto prefix = std::string_view{"tenzir.metrics."};
  if (schema.starts_with(prefix)) {
    return schema.substr(prefix.size());
  }
  return schema;
}

auto is_metric_name_char(unsigned char c) -> bool {
  return std::isalnum(c) or c == '_' or c == ':';
}

auto is_label_name_char(unsigned char c) -> bool {
  return std::isalnum(c) or c == '_';
}

template <class Predicate>
auto sanitize_identifier(std::string_view input, Predicate is_valid,
                         bool allow_colon) -> std::string {
  auto result = std::string{};
  result.reserve(input.size());
  auto last_was_underscore = false;
  for (auto c : input) {
    const auto uc = static_cast<unsigned char>(c);
    const auto next = is_valid(uc) and (allow_colon or c != ':') ? c : '_';
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
  const auto first = static_cast<unsigned char>(result.front());
  if (std::isdigit(first) or (not allow_colon and result.front() == ':')) {
    result.insert(result.begin(), '_');
  }
  return result;
}

auto sanitize_metric_name(std::string_view input) -> std::string {
  return sanitize_identifier(input, is_metric_name_char, true);
}

auto sanitize_label_name(std::string_view input) -> std::string {
  return sanitize_identifier(input, is_label_name_char, false);
}

auto is_dimension_field(std::string_view field) -> bool {
  static constexpr auto fields = std::array{
    std::string_view{"pipeline_id"}, std::string_view{"operator_id"},
    std::string_view{"name"},        std::string_view{"path"},
    std::string_view{"schema"},      std::string_view{"schema_id"},
    std::string_view{"handle"},      std::string_view{"method"},
    std::string_view{"status_code"},
  };
  return std::ranges::find(fields, field) != fields.end();
}

auto stringify_label_value(data_view3 value) -> std::optional<std::string> {
  return match(
    value,
    [](caf::none_t) -> std::optional<std::string> {
      return std::nullopt;
    },
    [](bool value) -> std::optional<std::string> {
      return value ? "true" : "false";
    },
    [](int64_t value) -> std::optional<std::string> {
      return fmt::format("{}", value);
    },
    [](uint64_t value) -> std::optional<std::string> {
      return fmt::format("{}", value);
    },
    [](double value) -> std::optional<std::string> {
      return fmt::format("{}", value);
    },
    [](duration value) -> std::optional<std::string> {
      return fmt::format("{}", value);
    },
    [](time value) -> std::optional<std::string> {
      return fmt::format("{}", value);
    },
    [](std::string_view value) -> std::optional<std::string> {
      return std::string{value};
    },
    [](enumeration value) -> std::optional<std::string> {
      return fmt::format("{}", value);
    },
    [](ip value) -> std::optional<std::string> {
      return fmt::format("{}", value);
    },
    [](subnet value) -> std::optional<std::string> {
      return fmt::format("{}", value);
    },
    [](auto) -> std::optional<std::string> {
      return std::nullopt;
    });
}

auto add_label(labels& result, std::string_view key, std::string value)
  -> void {
  auto sanitized = sanitize_label_name(key);
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

auto collect_labels(view3<record> record, const labels& inherited) -> labels {
  auto result = inherited;
  for (auto [key, value] : record) {
    if (not is_dimension_field(key)) {
      continue;
    }
    if (auto label_value = stringify_label_value(value)) {
      add_label(result, key, std::move(*label_value));
    }
  }
  return result;
}

auto contains_segment(const std::vector<std::string>& path,
                      std::string_view needle) -> bool {
  return std::ranges::find(path, needle) != path.end();
}

auto has_bytes_unit(std::string_view source,
                    const std::vector<std::string>& path) -> bool {
  for (const auto& segment : path) {
    if (segment.find("bytes") != std::string::npos) {
      return true;
    }
    if (source == "process"
        and (segment == "current_memory_usage" or segment == "peak_memory_usage"
             or segment == "swap_space_usage")) {
      return true;
    }
  }
  return false;
}

auto is_allocator_cumulative_counter(std::string_view source,
                                     const std::vector<std::string>& path)
  -> bool {
  if (source != "memory" or path.empty() or path.back() != "cumulative") {
    return false;
  }
  return contains_segment(path, "bytes")
         or contains_segment(path, "allocations");
}

auto describe_metric(std::string_view source,
                     const std::vector<std::string>& path, bool is_duration)
  -> metric_descriptor {
  auto result = metric_descriptor{};
  if (is_duration) {
    result.unit = "seconds";
    result.suffix_seconds = true;
  } else if (has_bytes_unit(source, path)) {
    result.unit = "bytes";
  }
  if (is_allocator_cumulative_counter(source, path)) {
    result.type = "counter";
  }
  return result;
}

auto make_metric_name(std::string_view source,
                      const std::vector<std::string>& path,
                      metric_descriptor descriptor) -> std::string {
  auto raw = fmt::format("tenzir_{}", source);
  for (const auto& segment : path) {
    raw += '_';
    raw += segment;
  }
  auto result = sanitize_metric_name(raw);
  if (descriptor.suffix_seconds and not result.ends_with("_seconds")) {
    result += "_seconds";
  }
  return result;
}

auto append_metric(series_builder& builder, std::string_view source,
                   const std::vector<std::string>& path, double value,
                   std::optional<time> timestamp, const labels& labels,
                   metric_descriptor descriptor) -> void {
  auto metric = builder.record();
  metric.field("metric", make_metric_name(source, path, descriptor));
  metric.field("value", value);
  if (timestamp) {
    metric.field("timestamp", *timestamp);
  } else {
    metric.field("timestamp", caf::none);
  }
  auto labels_record = metric.field("labels").record();
  for (const auto& [key, label_value] : labels) {
    labels_record.field(key, label_value);
  }
  metric.field("type", descriptor.type);
  metric.field("unit", descriptor.unit);
}

auto duration_to_seconds(duration value) -> double {
  return std::chrono::duration_cast<double_seconds>(value).count();
}

auto flatten_value(series_builder& builder, std::string_view source,
                   std::vector<std::string>& path, data_view3 value,
                   std::optional<time> timestamp, const labels& labels) -> void;

auto flatten_record(series_builder& builder, std::string_view source,
                    std::vector<std::string>& path, view3<record> record,
                    std::optional<time> timestamp, const labels& inherited)
  -> void {
  auto scoped_labels = collect_labels(record, inherited);
  for (auto [key, value] : record) {
    if (key == "timestamp" or is_dimension_field(key)) {
      continue;
    }
    path.push_back(std::string{key});
    flatten_value(builder, source, path, value, timestamp, scoped_labels);
    path.pop_back();
  }
}

auto flatten_list(series_builder& builder, std::string_view source,
                  std::vector<std::string>& path, view3<list> list,
                  std::optional<time> timestamp, const labels& labels) -> void {
  for (auto item : list) {
    if (const auto* item_record = try_as<view3<record>>(item)) {
      flatten_record(builder, source, path, *item_record, timestamp, labels);
    }
  }
}

auto flatten_value(series_builder& builder, std::string_view source,
                   std::vector<std::string>& path, data_view3 value,
                   std::optional<time> timestamp, const labels& labels)
  -> void {
  match(
    value, [&](caf::none_t) {},
    [&](int64_t value) {
      append_metric(builder, source, path, static_cast<double>(value),
                    timestamp, labels, describe_metric(source, path, false));
    },
    [&](uint64_t value) {
      append_metric(builder, source, path, static_cast<double>(value),
                    timestamp, labels, describe_metric(source, path, false));
    },
    [&](double value) {
      append_metric(builder, source, path, value, timestamp, labels,
                    describe_metric(source, path, false));
    },
    [&](duration value) {
      append_metric(builder, source, path, duration_to_seconds(value),
                    timestamp, labels, describe_metric(source, path, true));
    },
    [&](view3<record> value) {
      flatten_record(builder, source, path, value, timestamp, labels);
    },
    [&](view3<list> value) {
      flatten_list(builder, source, path, value, timestamp, labels);
    },
    [](auto) {});
}

auto find_timestamp(view3<record> record) -> std::optional<time> {
  for (auto [key, value] : record) {
    if (key != "timestamp") {
      continue;
    }
    if (auto* result = try_as<time>(value)) {
      return *result;
    }
    return std::nullopt;
  }
  return std::nullopt;
}

} // namespace

auto shape_metrics_for_prometheus(const table_slice& input)
  -> std::vector<table_slice> {
  auto builder = series_builder{prometheus_schema()};
  auto path = std::vector<std::string>{};
  const auto source = metric_source(input.schema().name());
  for (auto row : input.values()) {
    flatten_record(builder, source, path, row, find_timestamp(row), {});
  }
  return builder.finish_as_table_slice();
}

} // namespace tenzir::detail
