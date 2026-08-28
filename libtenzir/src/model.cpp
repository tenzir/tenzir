//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/model.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/series_builder.hpp"

#include <fmt/format.h>

#include <algorithm>
#include <iterator>

namespace tenzir {

namespace {

struct envelope_fields {
  Option<std::string_view> model;
  Option<uint64_t> version;
  Option<uint64_t> input_count;
  Option<uint64_t> count;
  Option<uint64_t> null_count;
};

auto missing_envelope_field(envelope_fields const& fields) -> std::string_view {
  if (not fields.model) {
    return "model";
  }
  if (not fields.version) {
    return "version";
  }
  if (not fields.input_count) {
    return "input_count";
  }
  if (not fields.count) {
    return "count";
  }
  TENZIR_ASSERT(not fields.null_count);
  return "null_count";
}

template <class T>
auto model_as(data_view3 value) -> T const* {
  return try_as<T>(value);
}

template <class T>
auto model_as(data const& value) -> T const* {
  return try_as<T>(&value);
}

template <class Value>
auto parse_model_uint64(Value&& value) -> Result<uint64_t, std::string> {
  auto const* result = model_as<uint64_t>(value);
  if (not result) {
    return Err{"must be a uint"};
  }
  return *result;
}

auto model_string(data_view3 value) -> Option<std::string_view> {
  if (auto const* result = try_as<std::string_view>(value)) {
    return *result;
  }
  return None{};
}

auto model_string(data const& value) -> Option<std::string_view> {
  if (auto const* result = try_as<std::string>(&value)) {
    return *result;
  }
  return None{};
}

template <class Record>
auto parse_model_envelope_impl(Record const& record)
  -> Result<model_envelope, std::string> {
  auto fields = envelope_fields{};
  for (auto const& [name, value] : record) {
    if (name == "model") {
      auto model = model_string(value);
      if (not model or model->empty()) {
        return Err{"`model` must be a non-empty string"};
      }
      fields.model = *model;
    } else if (name == "version") {
      auto version = parse_model_uint64(value);
      if (not version) {
        return Err{fmt::format("`version` {}", version.unwrap_err())};
      }
      fields.version = std::move(version).unwrap();
    } else if (name == "input_count") {
      auto input_count = parse_model_uint64(value);
      if (not input_count) {
        return Err{fmt::format("`input_count` {}", input_count.unwrap_err())};
      }
      fields.input_count = std::move(input_count).unwrap();
    } else if (name == "count") {
      auto count = parse_model_uint64(value);
      if (not count) {
        return Err{fmt::format("`count` {}", count.unwrap_err())};
      }
      fields.count = std::move(count).unwrap();
    } else if (name == "null_count") {
      auto null_count = parse_model_uint64(value);
      if (not null_count) {
        return Err{fmt::format("`null_count` {}", null_count.unwrap_err())};
      }
      fields.null_count = std::move(null_count).unwrap();
    }
  }
  if (not fields.model or not fields.version or not fields.input_count
      or not fields.count or not fields.null_count) {
    return Err{fmt::format("missing common model field `{}`",
                           missing_envelope_field(fields))};
  }
  return model_envelope{
    .model = *fields.model,
    .version = *fields.version,
    .input_count = *fields.input_count,
    .count = *fields.count,
    .null_count = *fields.null_count,
  };
}

} // namespace

auto model_uint64(data_view3 value) -> Result<uint64_t, std::string> {
  return parse_model_uint64(value);
}

auto model_uint64(data const& value) -> Result<uint64_t, std::string> {
  return parse_model_uint64(value);
}

auto model_double(data_view3 value) -> Result<double, std::string> {
  auto const* result = try_as<double>(value);
  if (not result) {
    return Err{"must be a double"};
  }
  return *result;
}

auto parse_model_envelope(record_view3 record)
  -> Result<model_envelope, std::string> {
  return parse_model_envelope_impl(record);
}

auto parse_model_envelope(record const& record)
  -> Result<model_envelope, std::string> {
  return parse_model_envelope_impl(record);
}

auto model_record_type(std::vector<struct record_type::field> fields) -> type {
  auto result = std::vector<struct record_type::field>{};
  result.reserve(fields.size() + 5);
  result.emplace_back("model", string_type{});
  result.emplace_back("version", uint64_type{});
  result.emplace_back("input_count", uint64_type{});
  result.emplace_back("count", uint64_type{});
  result.emplace_back("null_count", uint64_type{});
  std::ranges::move(fields, std::back_inserter(result));
  return type{record_type{result}};
}

auto model_plugin::make_model_merge_state(record const& model) const
  -> Result<Box<model_merge_state>, std::string> {
  auto builder = series_builder{};
  builder.data(model);
  auto models = builder.finish();
  if (models.size() != 1 or models.front().length() != 1) {
    return Err{"model record cannot be represented as one Arrow record"};
  }
  auto records = models.front().as<record_type>();
  if (not records) {
    return Err{"persisted model is not a record"};
  }
  auto values = values3(*records->array);
  auto value = values.begin();
  if (value == values.end() or not *value) {
    return Err{"persisted model record is null"};
  }
  return make_model_merge_state(**value);
}

auto find_model_plugin(model_envelope const& envelope)
  -> Result<model_plugin const*, std::string> {
  for (auto const* plugin : plugins::get<model_plugin>()) {
    if (plugin->name() != envelope.model) {
      continue;
    }
    if (plugin->model_version() != envelope.version) {
      return Err{
        fmt::format("unsupported `{}` model version {}; expected version {}",
                    envelope.model, envelope.version, plugin->model_version())};
    }
    return plugin;
  }
  return Err{fmt::format("unsupported model `{}`", envelope.model)};
}

} // namespace tenzir
