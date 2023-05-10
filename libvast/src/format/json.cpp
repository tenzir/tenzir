//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/format/json.hpp"

#include "vast/arrow_table_slice.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/ip.hpp"
#include "vast/concept/parseable/vast/json.hpp"
#include "vast/concept/parseable/vast/pattern.hpp"
#include "vast/concept/parseable/vast/subnet.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/data.hpp"
#include "vast/data.hpp"
#include "vast/format/json/default_selector.hpp"
#include "vast/format/json/field_selector.hpp"
#include "vast/logger.hpp"
#include "vast/policy/include_field_names.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/type.hpp"
#include "vast/view.hpp"

#include <arrow/record_batch.h>
#include <caf/detail/pretty_type_name.hpp>
#include <caf/expected.hpp>
#include <caf/none.hpp>

namespace vast::format::json {

// -- utility -----------------------------------------------------------------

namespace {

// Various implementations for simdjson element + type to data conversion.
// Note: extract copies; prefer to use add where possible. extract should no
// longer be necessary once we fully support nested lists and records inside
// lists.
data extract(const ::simdjson::dom::element& value, const type& type);
data extract(const ::simdjson::dom::array& values, const type& type);
data extract(const ::simdjson::dom::object& value, const type& type);
data extract(int64_t value, const type& type);
data extract(uint64_t value, const type& type);
data extract(double value, const type& type);
data extract(bool value, const type& type);
data extract(std::string_view value, const type& type);
data extract(std::nullptr_t, const type& type);

data extract(const ::simdjson::dom::element& value, const type& type) {
  switch (value.type()) {
    case ::simdjson::dom::element_type::ARRAY:
      return extract(value.get_array().value(), type);
    case ::simdjson::dom::element_type::OBJECT:
      return extract(value.get_object().value(), type);
    case ::simdjson::dom::element_type::INT64:
      return extract(value.get_int64().value(), type);
    case ::simdjson::dom::element_type::UINT64:
      return extract(value.get_uint64().value(), type);
    case ::simdjson::dom::element_type::DOUBLE:
      return extract(value.get_double().value(), type);
    case ::simdjson::dom::element_type::STRING:
      return extract(value.get_string().value(), type);
    case ::simdjson::dom::element_type::BOOL:
      return extract(value.get_bool().value(), type);
    case ::simdjson::dom::element_type::NULL_VALUE:
      return extract(nullptr, type);
  }
  __builtin_unreachable();
}

data extract(const ::simdjson::dom::array& values, const type& type) {
  auto f = detail::overload{
    [&](const bool_type&) noexcept -> data {
      return caf::none;
    },
    [&](const int64_type&) noexcept -> data {
      return caf::none;
    },
    [&](const uint64_type&) noexcept -> data {
      return caf::none;
    },
    [&](const double_type&) noexcept -> data {
      return caf::none;
    },
    [&](const duration_type&) noexcept -> data {
      return caf::none;
    },
    [&](const time_type&) noexcept -> data {
      return caf::none;
    },
    [&](const string_type&) noexcept -> data {
      return ::simdjson::to_string(values);
    },
    [&](const ip_type&) noexcept -> data {
      return caf::none;
    },
    [&](const subnet_type&) noexcept -> data {
      return caf::none;
    },
    [&](const enumeration_type&) noexcept -> data {
      return caf::none;
    },
    [&](const list_type& lt) noexcept -> data {
      auto result = list{};
      result.reserve(values.size());
      auto vt = lt.value_type();
      for (const auto& value : values)
        result.emplace_back(extract(value, vt));
      return result;
    },
    [&](const map_type&) noexcept -> data {
      return caf::none;
    },
    [&](const record_type&) noexcept -> data {
      return caf::none;
    },
  };
  return caf::visit(f, type);
}

data extract(int64_t value, const type& type) {
  auto f = detail::overload{
    [&](const bool_type&) noexcept -> data {
      return value != 0;
    },
    [&](const int64_type&) noexcept -> data {
      return int64_t{value};
    },
    [&](const uint64_type&) noexcept -> data {
      if (value >= 0)
        return detail::narrow_cast<uint64_t>(value);
      return caf::none;
    },
    [&](const double_type&) noexcept -> data {
      return detail::narrow_cast<double>(value);
    },
    [&](const duration_type&) noexcept -> data {
      return std::chrono::duration_cast<duration>(
        std::chrono::duration<int64_t>{value});
    },
    [&](const time_type&) noexcept -> data {
      return time{}
             + std::chrono::duration_cast<duration>(
               std::chrono::duration<int64_t>{value});
    },
    [&](const string_type&) noexcept -> data {
      return fmt::to_string(value);
    },
    [&](const ip_type&) noexcept -> data {
      return caf::none;
    },
    [&](const subnet_type&) noexcept -> data {
      return caf::none;
    },
    [&](const enumeration_type& et) noexcept -> data {
      if (auto key = detail::narrow_cast<enumeration>(value);
          !et.field(key).empty())
        return key;
      return caf::none;
    },
    [&](const list_type& lt) noexcept -> data {
      return list{extract(value, lt.value_type())};
    },
    [&](const map_type&) noexcept -> data {
      return caf::none;
    },
    [&](const record_type& rt) noexcept -> data {
      auto result = record{};
      result.reserve(rt.num_fields());
      for (const auto& field : rt.fields())
        result.emplace(field.name, caf::none);
      return result;
    },
  };
  return caf::visit(f, type);
}

data extract(const ::simdjson::dom::object& value, const type& type) {
  auto f = detail::overload{
    [&](const bool_type&) noexcept -> data {
      return caf::none;
    },
    [&](const int64_type&) noexcept -> data {
      return caf::none;
    },
    [&](const uint64_type&) noexcept -> data {
      return caf::none;
    },
    [&](const double_type&) noexcept -> data {
      return caf::none;
    },
    [&](const duration_type&) noexcept -> data {
      return caf::none;
    },
    [&](const time_type&) noexcept -> data {
      return caf::none;
    },
    [&](const string_type&) noexcept -> data {
      return ::simdjson::to_string(value);
    },
    [&](const ip_type&) noexcept -> data {
      return caf::none;
    },
    [&](const subnet_type&) noexcept -> data {
      return caf::none;
    },
    [&](const enumeration_type&) noexcept -> data {
      return caf::none;
    },
    [&](const list_type&) noexcept -> data {
      return caf::none;
    },
    [&](const map_type& mt) noexcept -> data {
      auto result = map{};
      result.reserve(value.size());
      const auto kt = mt.key_type();
      const auto vt = mt.value_type();
      for (const auto& [k, v] : value)
        result.emplace(extract(k, kt), extract(v, vt));
      return result;
    },
    [&](const record_type& rt) noexcept -> data {
      // Adding nested records is a bit more complicated because we try to be
      // nice to users, but still need to work with the table slice builder API.
      // - Given a field x, if x is present as a a field inside our JSON object,
      //   we just add all its fields and extract recursively.
      // - If the extraction fails for any nested field at any nesting level, we
      //   must add null to the builder because it expects a value to be added
      //   for all leaves in the record type.
      // - If a field x is not present, but it contains a nested field x.y that
      //   is present in the JSON object in a flattened representation, we add
      //   that. While this technically means that the schema differs from the
      //   JSON object at hand, we unflatten for our users automatically in this
      //   case.
      auto try_extract_record = [&](auto&& self, const record_type& rt,
                                    std::string_view prefix) -> data {
        auto result = record{};
        result.reserve(rt.num_fields());
        for (const auto& field : rt.fields()) {
          const auto next_prefix = fmt::format("{}{}.", prefix, field.name);
          const auto key
            = std::string_view{next_prefix.data(), next_prefix.size() - 1};
          auto x = value.at_key(key);
          if (x.error() != ::simdjson::error_code::SUCCESS) {
            // (1) The field does not directly exist in the record. We try to
            // find flattened representations, or add null values as required
            // for the given field's type.
            auto recurse = [&]<concrete_type T>(const T& type) -> data {
              if constexpr (std::is_same_v<T, record_type>) {
                return self(self, type, next_prefix);
              } else {
                return caf::none;
              }
            };
            result.emplace(field.name, caf::visit(recurse, field.type));
          } else {
            // (2) The field exists and we extracted it successfully.
            auto value = extract(x.value(), field.type);
            result.emplace(field.name, std::move(value));
          }
        }
        if (std::all_of(result.begin(), result.end(), [](const auto& x) {
              return x.second == caf::none;
            }))
          return caf::none;
        return result;
      };
      return try_extract_record(try_extract_record, rt, "");
    },
  };
  return caf::visit(f, type);
}

data extract(uint64_t value, const type& type) {
  auto f = detail::overload{
    [&](const bool_type&) noexcept -> data {
      return value != 0;
    },
    [&](const int64_type&) noexcept -> data {
      if (value <= std::numeric_limits<int64_t>::max())
        return int64_t{detail::narrow_cast<int64_t>(value)};
      return caf::none;
    },
    [&](const uint64_type&) noexcept -> data {
      return uint64_t{value};
    },
    [&](const double_type&) noexcept -> data {
      return detail::narrow_cast<double>(value);
    },
    [&](const duration_type&) noexcept -> data {
      return std::chrono::duration_cast<duration>(
        std::chrono::duration<uint64_t>{value});
    },
    [&](const time_type&) noexcept -> data {
      return time{}
             + std::chrono::duration_cast<duration>(
               std::chrono::duration<uint64_t>{value});
    },
    [&](const string_type&) noexcept -> data {
      return fmt::to_string(value);
    },
    [&](const ip_type&) noexcept -> data {
      return caf::none;
    },
    [&](const subnet_type&) noexcept -> data {
      return caf::none;
    },
    [&](const enumeration_type& et) noexcept -> data {
      if (auto key = detail::narrow_cast<enumeration>(value);
          !et.field(key).empty())
        return key;
      return caf::none;
    },
    [&](const list_type& lt) noexcept -> data {
      return list{extract(value, lt.value_type())};
    },
    [&](const map_type&) noexcept -> data {
      return caf::none;
    },
    [&](const record_type& rt) noexcept -> data {
      auto result = record{};
      result.reserve(rt.num_fields());
      for (const auto& field : rt.fields())
        result.emplace(field.name, caf::none);
      return result;
    },
  };
  return caf::visit(f, type);
}

data extract(double value, const type& type) {
  auto f = detail::overload{
    [&](const bool_type&) noexcept -> data {
      return value != 0.0;
    },
    [&](const int64_type&) noexcept -> data {
      return int64_t{detail::narrow_cast<int64_t>(value)};
    },
    [&](const uint64_type&) noexcept -> data {
      return detail::narrow_cast<uint64_t>(value);
    },
    [&](const double_type&) noexcept -> data {
      return value;
    },
    [&](const duration_type&) noexcept -> data {
      return std::chrono::duration_cast<duration>(
        std::chrono::duration<double>{value});
    },
    [&](const time_type&) noexcept -> data {
      return time{}
             + std::chrono::duration_cast<duration>(
               std::chrono::duration<double>{value});
    },
    [&](const string_type&) noexcept -> data {
      return fmt::to_string(value);
    },
    [&](const ip_type&) noexcept -> data {
      return caf::none;
    },
    [&](const subnet_type&) noexcept -> data {
      return caf::none;
    },
    [&](const enumeration_type&) noexcept -> data {
      return caf::none;
    },
    [&](const list_type& lt) noexcept -> data {
      return list{extract(value, lt.value_type())};
    },
    [&](const map_type&) noexcept -> data {
      return caf::none;
    },
    [&](const record_type& rt) noexcept -> data {
      auto result = record{};
      result.reserve(rt.num_fields());
      for (const auto& field : rt.fields())
        result.emplace(field.name, caf::none);
      return result;
    },
  };
  return caf::visit(f, type);
}

data extract(std::string_view value, const type& type) {
  auto f = detail::overload{
    [&](const bool_type&) noexcept -> data {
      if (bool result = {}; parsers::json_boolean(value, result))
        return result;
      return caf::none;
    },
    [&](const int64_type&) noexcept -> data {
      if (int64_t result = {}; parsers::json_int(value, result))
        return int64_t{result};
      if (double result = {}; parsers::json_number(value, result))
        return int64_t{detail::narrow_cast<int64_t>(result)};
      return caf::none;
    },
    [&](const uint64_type&) noexcept -> data {
      if (uint64_t result = {}; parsers::json_count(value, result))
        return result;
      if (double result = {}; parsers::json_number(value, result))
        return detail::narrow_cast<uint64_t>(result);
      return caf::none;
    },
    [&](const double_type&) noexcept -> data {
      if (double result = {}; parsers::json_number(value, result))
        return result;
      return caf::none;
    },
    [&](const duration_type&) noexcept -> data {
      if (auto result = to<duration>(value))
        return *result;
      if (double result = {}; parsers::json_number(value, result))
        return std::chrono::duration_cast<duration>(
          std::chrono::duration<double>{result});
      return caf::none;
    },
    [&](const time_type&) noexcept -> data {
      if (auto result = to<time>(value))
        return *result;
      if (double result = {}; parsers::json_number(value, result))
        return time{}
               + std::chrono::duration_cast<duration>(
                 std::chrono::duration<double>{result});
      return caf::none;
    },
    [&](const string_type&) noexcept -> data {
      return std::string{value};
    },
    [&](const ip_type&) noexcept -> data {
      if (auto result = to<ip>(value))
        return *result;
      return caf::none;
    },
    [&](const subnet_type&) noexcept -> data {
      if (auto result = to<subnet>(value))
        return *result;
      return caf::none;
    },
    [&](const enumeration_type& et) noexcept -> data {
      if (auto internal = et.resolve(value))
        return detail::narrow_cast<enumeration>(*internal);
      return caf::none;
    },
    [&](const list_type& lt) noexcept -> data {
      return list{extract(value, lt.value_type())};
    },
    [&](const map_type&) noexcept -> data {
      return caf::none;
    },
    [&](const record_type& rt) noexcept -> data {
      auto result = record{};
      result.reserve(rt.num_fields());
      for (const auto& field : rt.fields())
        result.emplace(field.name, caf::none);
      return result;
    },
  };
  return caf::visit(f, type);
}

data extract(bool value, const type& type) {
  auto f = detail::overload{
    [&](const bool_type&) noexcept -> data {
      return value;
    },
    [&](const int64_type&) noexcept -> data {
      return value ? int64_t{1} : int64_t{0};
    },
    [&](const uint64_type&) noexcept -> data {
      return value ? uint64_t{1} : uint64_t{0};
    },
    [&](const double_type&) noexcept -> data {
      return caf::none;
    },
    [&](const duration_type&) noexcept -> data {
      return caf::none;
    },
    [&](const time_type&) noexcept -> data {
      return caf::none;
    },
    [&](const string_type&) noexcept -> data {
      return fmt::to_string(value);
    },
    [&](const ip_type&) noexcept -> data {
      return caf::none;
    },
    [&](const subnet_type&) noexcept -> data {
      return caf::none;
    },
    [&](const enumeration_type&) noexcept -> data {
      return caf::none;
    },
    [&](const list_type& lt) noexcept -> data {
      return list{extract(value, lt.value_type())};
    },
    [&](const map_type&) noexcept -> data {
      return caf::none;
    },
    [&](const record_type& rt) noexcept -> data {
      auto result = record{};
      result.reserve(rt.num_fields());
      for (const auto& field : rt.fields())
        result.emplace(field.name, caf::none);
      return result;
    },
  };
  return caf::visit(f, type);
}

data extract(std::nullptr_t, const type& type) {
  if (const auto* rt = caf::get_if<record_type>(&type)) {
    auto result = record{};
    result.reserve(rt->num_fields());
    for (const auto& field : rt->fields())
      result.emplace(field.name, caf::none);
    return result;
  }
  return caf::none;
}

void add(int64_t value, const type& type, table_slice_builder& builder);
void add(uint64_t value, const type& type, table_slice_builder& builder);
void add(double value, const type& type, table_slice_builder& builder);
void add(bool value, const type& type, table_slice_builder& builder);
void add(std::string_view value, const type& type,
         table_slice_builder& builder);

caf::error add(const ::simdjson::dom::element& value, const type& type,
               table_slice_builder& builder) {
  switch (value.type()) {
    case ::simdjson::dom::element_type::ARRAY: {
      // Arrays need to be extracted.
      if (!builder.add(extract(value.get_array().value(), type)))
        return caf::make_error(
          ec::parse_error,
          fmt::format("failed to extract value of type {} from JSON array {}",
                      type, ::simdjson::to_string(value.get_array())));
      return caf::none;
    }
    case ::simdjson::dom::element_type::OBJECT:
      // We cannot have an object at this point because we are visiting only the
      // leaves of the outermost record type, and for records inside lists we
      // extract rather than add.
      __builtin_unreachable();
    case ::simdjson::dom::element_type::INT64: {
      add(value.get_int64().value(), type, builder);
      return caf::none;
    }
    case ::simdjson::dom::element_type::UINT64: {
      add(value.get_uint64().value(), type, builder);
      return caf::none;
    }
    case ::simdjson::dom::element_type::DOUBLE: {
      add(value.get_double().value(), type, builder);
      return caf::none;
    }
    case ::simdjson::dom::element_type::STRING: {
      add(value.get_string().value(), type, builder);
      return caf::none;
    }
    case ::simdjson::dom::element_type::BOOL: {
      add(value.get_bool().value(), type, builder);
      return caf::none;
    }
    case ::simdjson::dom::element_type::NULL_VALUE: {
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
      return caf::none;
    }
  }
  __builtin_unreachable();
}

void add(int64_t value, const type& type, table_slice_builder& builder) {
  auto f = detail::overload{
    [&](const bool_type&) noexcept {
      const auto added = builder.add(value != 0);
      VAST_ASSERT(added);
    },
    [&](const int64_type&) noexcept {
      const auto added = builder.add(view<int64_t>{value});
      VAST_ASSERT(added);
    },
    [&](const uint64_type&) noexcept {
      if (value >= 0) {
        const auto added
          = builder.add(detail::narrow_cast<view<uint64_t>>(value));
        VAST_ASSERT(added);
        return;
      }
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const double_type&) noexcept {
      const auto added = builder.add(detail::narrow_cast<view<double>>(value));
      VAST_ASSERT(added);
    },
    [&](const duration_type&) noexcept {
      const auto added = builder.add(std::chrono::duration_cast<duration>(
        std::chrono::duration<int64_t>{value}));
      VAST_ASSERT(added);
    },
    [&](const time_type&) noexcept {
      const auto added = builder.add(time{}
                                     + std::chrono::duration_cast<duration>(
                                       std::chrono::duration<int64_t>{value}));
      VAST_ASSERT(added);
    },
    [&](const string_type&) noexcept {
      const auto added = builder.add(fmt::to_string(value));
      VAST_ASSERT(added);
    },
    [&](const ip_type&) noexcept {
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const subnet_type&) noexcept {
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const enumeration_type& et) noexcept {
      if (auto key = detail::narrow_cast<view<enumeration>>(value);
          !et.field(key).empty()) {
        const auto added = builder.add(key);
        VAST_ASSERT(added);
        return;
      }
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const list_type& lt) noexcept {
      const auto added = builder.add(list{extract(value, lt.value_type())});
      VAST_ASSERT(added);
    },
    [&](const map_type&) noexcept {
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const record_type&) noexcept {
      __builtin_unreachable();
    },
  };
  caf::visit(f, type);
}

void add(uint64_t value, const type& type, table_slice_builder& builder) {
  auto f = detail::overload{
    [&](const bool_type&) noexcept {
      const auto added = builder.add(value != 0);
      VAST_ASSERT(added);
    },
    [&](const int64_type&) noexcept {
      if (value <= std::numeric_limits<int64_t>::max()) {
        const auto added
          = builder.add(view<int64_t>{detail::narrow_cast<int64_t>(value)});
        VAST_ASSERT(added);
        return;
      }
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const uint64_type&) noexcept {
      const auto added = builder.add(view<uint64_t>{value});
      VAST_ASSERT(added);
    },
    [&](const double_type&) noexcept {
      const auto added = builder.add(detail::narrow_cast<view<double>>(value));
      VAST_ASSERT(added);
    },
    [&](const duration_type&) noexcept {
      const auto added = builder.add(std::chrono::duration_cast<duration>(
        std::chrono::duration<uint64_t>{value}));
      VAST_ASSERT(added);
    },
    [&](const time_type&) noexcept {
      const auto added = builder.add(time{}
                                     + std::chrono::duration_cast<duration>(
                                       std::chrono::duration<uint64_t>{value}));
      VAST_ASSERT(added);
    },
    [&](const string_type&) noexcept {
      const auto added = builder.add(fmt::to_string(value));
      VAST_ASSERT(added);
    },
    [&](const ip_type&) noexcept {
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const subnet_type&) noexcept {
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const enumeration_type& et) noexcept {
      if (auto key = detail::narrow_cast<view<enumeration>>(value);
          !et.field(key).empty()) {
        const auto added = builder.add(key);
        VAST_ASSERT(added);
        return;
      }
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const list_type& lt) noexcept {
      const auto added = builder.add(list{extract(value, lt.value_type())});
      VAST_ASSERT(added);
    },
    [&](const map_type&) noexcept {
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const record_type&) noexcept {
      __builtin_unreachable();
    },
  };
  caf::visit(f, type);
}

void add(double value, const type& type, table_slice_builder& builder) {
  auto f = detail::overload{
    [&](const bool_type&) noexcept {
      const auto added = builder.add(value != 0);
      VAST_ASSERT(added);
    },
    [&](const int64_type&) noexcept {
      const auto added
        = builder.add(view<int64_t>{detail::narrow_cast<int64_t>(value)});
      VAST_ASSERT(added);
    },
    [&](const uint64_type&) noexcept {
      const auto added = builder.add(detail::narrow_cast<uint64_t>(value));
      VAST_ASSERT(added);
    },
    [&](const double_type&) noexcept {
      const auto added = builder.add(value);
      VAST_ASSERT(added);
    },
    [&](const duration_type&) noexcept {
      const auto added = builder.add(std::chrono::duration_cast<duration>(
        std::chrono::duration<double>{value}));
      VAST_ASSERT(added);
    },
    [&](const time_type&) noexcept {
      const auto added = builder.add(time{}
                                     + std::chrono::duration_cast<duration>(
                                       std::chrono::duration<double>{value}));
      VAST_ASSERT(added);
    },
    [&](const string_type&) noexcept {
      const auto added = builder.add(fmt::to_string(value));
      VAST_ASSERT(added);
    },
    [&](const ip_type&) noexcept {
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const subnet_type&) noexcept {
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const enumeration_type&) noexcept {
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const list_type& lt) noexcept {
      const auto added = builder.add(list{extract(value, lt.value_type())});
      VAST_ASSERT(added);
    },
    [&](const map_type&) noexcept {
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const record_type&) noexcept {
      __builtin_unreachable();
    },
  };
  caf::visit(f, type);
}

void add(bool value, const type& type, table_slice_builder& builder) {
  auto f = detail::overload{
    [&](const bool_type&) noexcept {
      const auto added = builder.add(value);
      VAST_ASSERT(added);
    },
    [&](const int64_type&) noexcept {
      const auto added = builder.add(value ? int64_t{1} : int64_t{0});
      VAST_ASSERT(added);
    },
    [&](const uint64_type&) noexcept {
      const auto added = builder.add(value ? uint64_t{1} : uint64_t{0});
      VAST_ASSERT(added);
    },
    [&](const double_type&) noexcept {
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const duration_type&) noexcept {
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const time_type&) noexcept {
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const string_type&) noexcept {
      const auto added = builder.add(fmt::to_string(value));
      VAST_ASSERT(added);
    },
    [&](const ip_type&) noexcept {
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const subnet_type&) noexcept {
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const enumeration_type&) noexcept {
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const list_type& lt) noexcept {
      const auto added = builder.add(list{extract(value, lt.value_type())});
      VAST_ASSERT(added);
    },
    [&](const map_type&) noexcept {
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const record_type&) noexcept {
      __builtin_unreachable();
    },
  };
  caf::visit(f, type);
}

void add(std::string_view value, const type& type,
         table_slice_builder& builder) {
  auto f = detail::overload{
    [&](const bool_type&) noexcept {
      if (bool result = {}; parsers::json_boolean(value, result)) {
        const auto added = builder.add(result);
        VAST_ASSERT(added);
        return;
      }
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const int64_type&) noexcept {
      if (int64_t result = {}; parsers::json_int(value, result)) {
        const auto added = builder.add(view<int64_t>{result});
        VAST_ASSERT(added);
        return;
      }
      if (double result = {}; parsers::json_number(value, result)) {
        const auto added
          = builder.add(view<int64_t>{detail::narrow_cast<int64_t>(result)});
        VAST_ASSERT(added);
        return;
      }
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const uint64_type&) noexcept {
      if (uint64_t result = {}; parsers::json_count(value, result)) {
        const auto added = builder.add(result);
        VAST_ASSERT(added);
        return;
      }
      if (double result = {}; parsers::json_number(value, result)) {
        const auto added
          = builder.add(detail::narrow_cast<view<uint64_t>>(result));
        VAST_ASSERT(added);
        return;
      }
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const double_type&) noexcept {
      if (double result = {}; parsers::json_number(value, result)) {
        const auto added = builder.add(result);
        VAST_ASSERT(added);
        return;
      }
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const duration_type&) noexcept {
      if (auto result = to<duration>(value)) {
        const auto added = builder.add(*result);
        VAST_ASSERT(added);
        return;
      }
      if (double result = {}; parsers::json_number(value, result)) {
        const auto added = builder.add(std::chrono::duration_cast<duration>(
          std::chrono::duration<double>(result)));
        VAST_ASSERT(added);
        return;
      }
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const time_type&) noexcept {
      if (auto result = to<time>(value)) {
        const auto added = builder.add(*result);
        VAST_ASSERT(added);
        return;
      }
      if (double result = {}; parsers::json_number(value, result)) {
        const auto added
          = builder.add(time{}
                        + std::chrono::duration_cast<duration>(
                          std::chrono::duration<double>(result)));
        VAST_ASSERT(added);
        return;
      }
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const string_type&) noexcept {
      const auto added = builder.add(value);
      VAST_ASSERT(added);
    },
    [&](const ip_type&) noexcept {
      if (auto result = to<ip>(value)) {
        const auto added = builder.add(*result);
        VAST_ASSERT(added);
        return;
      }
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const subnet_type&) noexcept {
      if (auto result = to<subnet>(value)) {
        const auto added = builder.add(*result);
        VAST_ASSERT(added);
        return;
      }
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const enumeration_type& et) noexcept {
      if (auto internal = et.resolve(value)) {
        const auto added
          = builder.add(detail::narrow_cast<enumeration>(*internal));
        VAST_ASSERT(added);
        return;
      }
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const list_type& lt) noexcept {
      const auto added = builder.add(list{extract(value, lt.value_type())});
      VAST_ASSERT(added);
    },
    [&](const map_type&) noexcept {
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const record_type&) noexcept {
      __builtin_unreachable();
    },
  };
  caf::visit(f, type);
}

} // namespace

caf::error
add(const ::simdjson::dom::object& object, table_slice_builder& builder) {
  auto self
    = [&](auto&& self, const ::simdjson::dom::object& object,
          const record_type& schema, std::string_view prefix) -> caf::error {
    for (const auto& field : schema.fields()) {
      auto handle_found = [&](const ::simdjson::dom::element& element) {
        auto f = detail::overload{
          [&](const map_type& mt) {
            if (element.is_object()) {
              const auto object = element.get_object().value();
              auto result = map{};
              result.reserve(object.size());
              const auto kt = mt.key_type();
              const auto vt = mt.value_type();
              for (const auto& [k, v] : object)
                result.emplace(extract(k, kt), extract(v, vt));
              const auto added = builder.add(result);
              VAST_ASSERT(added);
            } else {
              const auto added = builder.add(caf::none);
              VAST_ASSERT(added);
            }
          },
          [&](const record_type& rt) {
            if (element.is_object()) {
              auto err = self(self, element.get_object().value(), rt, "");
              VAST_ASSERT(!err);
            } else {
              for (size_t i = 0; i < rt.num_leaves(); ++i) {
                const auto added = builder.add(caf::none);
                VAST_ASSERT(added);
              }
            }
          },
          [&](const auto&) {
            if (element.is_object()) {
              const auto added = builder.add(caf::none);
              VAST_ASSERT(added);
            } else {
              auto err = add(element, field.type, builder);
              VAST_ASSERT(!err);
            }
          },
        };
        caf::visit(f, field.type);
      };
      auto handle_not_found = [&](std::string_view next_prefix) -> caf::error {
        if (const auto* nested = caf::get_if<record_type>(&field.type)) {
          if (auto err = self(self, object, *nested, next_prefix))
            return err;
        } else {
          const auto added = builder.add(caf::none);
          VAST_ASSERT(added);
        }
        return caf::none;
      };
      if (prefix.empty()) {
        auto element = object.at_key(field.name);
        if (element.error() == ::simdjson::error_code::SUCCESS)
          handle_found(element.value());
        else if (auto err = handle_not_found(field.name))
          return err;
      } else {
        const auto prefixed_key = fmt::format("{}.{}", prefix, field.name);
        auto element = object.at_key(prefixed_key);
        if (element.error() == ::simdjson::error_code::SUCCESS)
          handle_found(element.value());
        else if (auto err = handle_not_found(prefixed_key))
          return err;
      }
    }
    return caf::none;
  };
  const auto& schema = caf::get<record_type>(builder.schema());
  return self(self, object, schema, "");
}

// -- writer ------------------------------------------------------------------

writer::writer(std::unique_ptr<std::ostream> out, const caf::settings& options)
  : super(),
    out_{std::move(out)},
    printer_{{
      .oneline = true,
      .flattened = get_or(options, "vast.export.json.flatten", false),
      .numeric_durations
      = get_or(options, "vast.export.json.numeric-durations", false),
      .omit_nulls
      = get_or(options, "vast.export.json.omit-nulls",
               get_or(options, "vast.export.json.omit-empty", false)),
      .omit_empty_records
      = get_or(options, "vast.export.json.omit-empty-records",
               get_or(options, "vast.export.json.omit-empty", false)),
      .omit_empty_lists
      = get_or(options, "vast.export.json.omit-empty-lists",
               get_or(options, "vast.export.json.omit-empty", false)),
      .omit_empty_maps
      = get_or(options, "vast.export.json.omit-empty-maps",
               get_or(options, "vast.export.json.omit-empty", false)),
    }} {
  // nop
}

caf::error writer::write(const table_slice& x) {
  auto resolved_slice = resolve_enumerations(x);
  auto type = caf::get<record_type>(resolved_slice.schema());
  auto array = to_record_batch(resolved_slice)->ToStructArray().ValueOrDie();
  auto out_iter = std::ostream_iterator<char>{out()};
  for (const auto& row : values(type, *array)) {
    VAST_ASSERT_CHEAP(row);
    const auto ok = printer_.print(out_iter, *row);
    VAST_ASSERT_CHEAP(ok);
    out_iter = fmt::format_to(out_iter, "\n");
  }
  return {};
}

caf::expected<void> writer::flush() {
  out_->flush();
  return {};
}

std::ostream& writer::out() {
  VAST_ASSERT(out_ != nullptr);
  return *out_;
}

const char* writer::name() const {
  return "json-writer";
}

// -- reader ------------------------------------------------------------------

reader::reader(const caf::settings& options, std::unique_ptr<std::istream> in)
  : super(options) {
  if (in != nullptr)
    reset(std::move(in));
  if (const auto selector_opt
      = caf::get_if<std::string>(&options, "vast.import.json.selector")) {
    auto split = detail::split(*selector_opt, ":");
    VAST_ASSERT(!split.empty());
    if (split.size() > 2 || split[0].empty()) {
      VAST_ERROR("failed to parse selector '{}': must contain at most one "
                 "':' and field name must not be empty; ignoring option",
                 *selector_opt);
      selector_ = std::make_unique<default_selector>();
    } else {
      auto field_name = std::string{split[0]};
      auto type_prefix = split.size() == 2 ? std::string{split[1]} : "";
      if (!type_prefix.empty())
        reader_name_ = fmt::format("{}-reader", type_prefix);
      selector_ = std::make_unique<field_selector>(std::move(field_name),
                                                   std::move(type_prefix));
    }
  } else {
    selector_ = std::make_unique<default_selector>();
  }
}

void reader::reset(std::unique_ptr<std::istream> in) {
  VAST_ASSERT(in != nullptr);
  input_ = std::move(in);
  lines_ = std::make_unique<detail::line_range>(*input_);
}

caf::error reader::module(vast::module m) {
  return selector_->module(m);
}

vast::module reader::module() const {
  return selector_->module();
}

const char* reader::name() const {
  return reader_name_.c_str();
}

vast::system::report reader::status() const {
  using namespace std::string_literals;
  uint64_t invalid_line = num_invalid_lines_;
  uint64_t unknown_layout = num_unknown_layouts_;
  if (num_invalid_lines_ > 0)
    VAST_WARN("{} failed to parse {} of {} recent lines",
              detail::pretty_type_name(this), num_invalid_lines_, num_lines_);
  if (num_unknown_layouts_ > 0)
    VAST_WARN("{} failed to find a matching type for {} of {} recent lines",
              detail::pretty_type_name(this), num_unknown_layouts_, num_lines_);
  num_invalid_lines_ = 0;
  num_unknown_layouts_ = 0;
  num_lines_ = 0;
  return {.data = {
            {name() + ".invalid-line"s, invalid_line},
            {name() + ".unknown-layout"s, unknown_layout},
          }};
}

caf::error
reader::read_impl(size_t max_events, size_t max_slice_size, consumer& cons) {
  VAST_TRACE_SCOPE("{} {}", VAST_ARG(max_events), VAST_ARG(max_slice_size));
  VAST_ASSERT(max_events > 0);
  VAST_ASSERT(max_slice_size > 0);
  size_t produced = 0;
  table_slice_builder_ptr bptr = nullptr;
  while (produced < max_events) {
    if (lines_->done())
      return finish(cons, caf::make_error(ec::end_of_input, "input exhausted"));
    if (batch_events_ > 0 && batch_timeout_ > reader_clock::duration::zero()
        && last_batch_sent_ + batch_timeout_ < reader_clock::now()) {
      VAST_DEBUG("{} reached batch timeout", detail::pretty_type_name(this));
      return finish(cons, ec::timeout);
    }
    bool timed_out = lines_->next_timeout(read_timeout_);
    if (timed_out) {
      VAST_DEBUG("{} stalled at line {}", detail::pretty_type_name(this),
                 lines_->line_number());
      return ec::stalled;
    }
    auto& line = lines_->get();
    ++num_lines_;
    if (line.empty()) {
      // Ignore empty lines.
      VAST_DEBUG("{} ignores empty line at {}", detail::pretty_type_name(this),
                 lines_->line_number());
      continue;
    }
    auto parse_result = json_parser_.parse(line);
    if (parse_result.error() != ::simdjson::error_code::SUCCESS) {
      if (num_invalid_lines_ == 0)
        VAST_WARN("{} failed to parse line {}: {}",
                  detail::pretty_type_name(this), lines_->line_number(), line);
      ++num_invalid_lines_;
      continue;
    }
    auto get_object_result = parse_result.get_object();
    if (get_object_result.error() != ::simdjson::error_code::SUCCESS) {
      if (num_invalid_lines_ == 0)
        VAST_WARN("{} failed to parse line as JSON object {}: {}",
                  detail::pretty_type_name(this), lines_->line_number(), line);
      ++num_invalid_lines_;
      continue;
    }
    auto&& schema = (*selector_)(get_object_result.value());
    if (!schema) {
      if (num_unknown_layouts_ == 0)
        VAST_WARN("{} failed to find a matching type at line {}: {}",
                  detail::pretty_type_name(this), lines_->line_number(), line);
      ++num_unknown_layouts_;
      continue;
    }
    bptr = builder(*schema);
    if (bptr == nullptr)
      return caf::make_error(ec::parse_error, "unable to get a builder");
    if (auto err = add(get_object_result.value(), *bptr))
      return finish(cons, //
                    caf::make_error(ec::logic_error,
                                    fmt::format("failed to add line {} of "
                                                "schema {} to builder: {}",
                                                lines_->line_number(), *schema,
                                                err)));
    produced++;
    batch_events_++;
    if (bptr->rows() == max_slice_size)
      if (auto err = finish(cons, bptr))
        return err;
  }
  return finish(cons);
}

} // namespace vast::format::json
