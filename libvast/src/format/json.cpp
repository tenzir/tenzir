//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/format/json.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/address.hpp"
#include "vast/concept/parseable/vast/integer.hpp"
#include "vast/concept/parseable/vast/json.hpp"
#include "vast/concept/parseable/vast/pattern.hpp"
#include "vast/concept/parseable/vast/subnet.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/data.hpp"
#include "vast/concept/printable/vast/json.hpp"
#include "vast/data.hpp"
#include "vast/logger.hpp"
#include "vast/policy/include_field_names.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/type.hpp"
#include "vast/view.hpp"

#include <caf/detail/pretty_type_name.hpp>
#include <caf/expected.hpp>
#include <caf/none.hpp>

namespace vast::format::json {

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
    [&](const none_type&) noexcept -> data {
      return caf::none;
    },
    [&](const bool_type&) noexcept -> data {
      return caf::none;
    },
    [&](const integer_type&) noexcept -> data {
      return caf::none;
    },
    [&](const count_type&) noexcept -> data {
      return caf::none;
    },
    [&](const real_type&) noexcept -> data {
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
    [&](const pattern_type&) noexcept -> data {
      return caf::none;
    },
    [&](const address_type&) noexcept -> data {
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
    [&](const none_type&) noexcept -> data {
      return caf::none;
    },
    [&](const bool_type&) noexcept -> data {
      return value != 0;
    },
    [&](const integer_type&) noexcept -> data {
      return integer{value};
    },
    [&](const count_type&) noexcept -> data {
      if (value >= 0)
        return detail::narrow_cast<count>(value);
      return caf::none;
    },
    [&](const real_type&) noexcept -> data {
      return detail::narrow_cast<real>(value);
    },
    [&](const duration_type&) noexcept -> data {
      return std::chrono::duration_cast<duration>(
        std::chrono::duration<integer::value_type>{value});
    },
    [&](const time_type&) noexcept -> data {
      return time{}
             + std::chrono::duration_cast<duration>(
               std::chrono::duration<integer::value_type>{value});
    },
    [&](const string_type&) noexcept -> data {
      return fmt::to_string(value);
    },
    [&](const pattern_type&) noexcept -> data {
      return caf::none;
    },
    [&](const address_type&) noexcept -> data {
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
    [&](const none_type&) noexcept -> data {
      return caf::none;
    },
    [&](const bool_type&) noexcept -> data {
      return caf::none;
    },
    [&](const integer_type&) noexcept -> data {
      return caf::none;
    },
    [&](const count_type&) noexcept -> data {
      return caf::none;
    },
    [&](const real_type&) noexcept -> data {
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
    [&](const pattern_type&) noexcept -> data {
      return caf::none;
    },
    [&](const address_type&) noexcept -> data {
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
      //   must add nil to the builder because it expects a value to be added
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
            // find flattened representations, or add nil values as required for
            // the given field's type.
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
    [&](const none_type&) noexcept -> data {
      return caf::none;
    },
    [&](const bool_type&) noexcept -> data {
      return value != 0;
    },
    [&](const integer_type&) noexcept -> data {
      if (value <= std::numeric_limits<integer::value_type>::max())
        return integer{detail::narrow_cast<integer::value_type>(value)};
      return caf::none;
    },
    [&](const count_type&) noexcept -> data {
      return count{value};
    },
    [&](const real_type&) noexcept -> data {
      return detail::narrow_cast<real>(value);
    },
    [&](const duration_type&) noexcept -> data {
      return std::chrono::duration_cast<duration>(
        std::chrono::duration<count>{value});
    },
    [&](const time_type&) noexcept -> data {
      return time{}
             + std::chrono::duration_cast<duration>(
               std::chrono::duration<count>{value});
    },
    [&](const string_type&) noexcept -> data {
      return fmt::to_string(value);
    },
    [&](const pattern_type&) noexcept -> data {
      return caf::none;
    },
    [&](const address_type&) noexcept -> data {
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
    [&](const none_type&) noexcept -> data {
      return caf::none;
    },
    [&](const bool_type&) noexcept -> data {
      return value != 0.0;
    },
    [&](const integer_type&) noexcept -> data {
      return integer{detail::narrow_cast<integer::value_type>(value)};
    },
    [&](const count_type&) noexcept -> data {
      return detail::narrow_cast<count>(value);
    },
    [&](const real_type&) noexcept -> data {
      return value;
    },
    [&](const duration_type&) noexcept -> data {
      return std::chrono::duration_cast<duration>(
        std::chrono::duration<real>{value});
    },
    [&](const time_type&) noexcept -> data {
      return time{}
             + std::chrono::duration_cast<duration>(
               std::chrono::duration<real>{value});
    },
    [&](const string_type&) noexcept -> data {
      return fmt::to_string(value);
    },
    [&](const pattern_type&) noexcept -> data {
      return caf::none;
    },
    [&](const address_type&) noexcept -> data {
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
    [&](const none_type&) noexcept -> data {
      return caf::none;
    },
    [&](const bool_type&) noexcept -> data {
      if (bool result = {}; parsers::json_boolean(value, result))
        return result;
      return caf::none;
    },
    [&](const integer_type&) noexcept -> data {
      if (integer::value_type result = {}; parsers::json_int(value, result))
        return integer{result};
      if (real result = {}; parsers::json_number(value, result))
        return integer{detail::narrow_cast<integer::value_type>(result)};
      return caf::none;
    },
    [&](const count_type&) noexcept -> data {
      if (count result = {}; parsers::json_count(value, result))
        return result;
      if (real result = {}; parsers::json_number(value, result))
        return detail::narrow_cast<count>(result);
      return caf::none;
    },
    [&](const real_type&) noexcept -> data {
      if (real result = {}; parsers::json_number(value, result))
        return result;
      return caf::none;
    },
    [&](const duration_type&) noexcept -> data {
      if (auto result = to<duration>(value))
        return *result;
      return caf::none;
    },
    [&](const time_type&) noexcept -> data {
      if (auto result = to<time>(value))
        return *result;
      return caf::none;
    },
    [&](const string_type&) noexcept -> data {
      return std::string{value};
    },
    [&](const pattern_type&) noexcept -> data {
      if (auto result = to<pattern>(value))
        return *result;
      return caf::none;
    },
    [&](const address_type&) noexcept -> data {
      if (auto result = to<address>(value))
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
    [&](const none_type&) noexcept -> data {
      return caf::none;
    },
    [&](const bool_type&) noexcept -> data {
      return value;
    },
    [&](const integer_type&) noexcept -> data {
      return value ? integer{1} : integer{0};
    },
    [&](const count_type&) noexcept -> data {
      return value ? count{1} : count{0};
    },
    [&](const real_type&) noexcept -> data {
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
    [&](const pattern_type&) noexcept -> data {
      return caf::none;
    },
    [&](const address_type&) noexcept -> data {
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
    [&](const none_type&) noexcept {
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const bool_type&) noexcept {
      const auto added = builder.add(value != 0);
      VAST_ASSERT(added);
    },
    [&](const integer_type&) noexcept {
      const auto added = builder.add(view<integer>{value});
      VAST_ASSERT(added);
    },
    [&](const count_type&) noexcept {
      if (value >= 0) {
        const auto added = builder.add(detail::narrow_cast<view<count>>(value));
        VAST_ASSERT(added);
        return;
      }
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const real_type&) noexcept {
      const auto added = builder.add(detail::narrow_cast<view<real>>(value));
      VAST_ASSERT(added);
    },
    [&](const duration_type&) noexcept {
      const auto added = builder.add(std::chrono::duration_cast<duration>(
        std::chrono::duration<integer::value_type>{value}));
      VAST_ASSERT(added);
    },
    [&](const time_type&) noexcept {
      const auto added
        = builder.add(time{}
                      + std::chrono::duration_cast<duration>(
                        std::chrono::duration<integer::value_type>{value}));
      VAST_ASSERT(added);
    },
    [&](const string_type&) noexcept {
      const auto added = builder.add(fmt::to_string(value));
      VAST_ASSERT(added);
    },
    [&](const pattern_type&) noexcept {
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const address_type&) noexcept {
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
    [&](const none_type&) noexcept {
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const bool_type&) noexcept {
      const auto added = builder.add(value != 0);
      VAST_ASSERT(added);
    },
    [&](const integer_type&) noexcept {
      if (value <= std::numeric_limits<int64_t>::max()) {
        const auto added = builder.add(
          view<integer>{detail::narrow_cast<integer::value_type>(value)});
        VAST_ASSERT(added);
        return;
      }
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const count_type&) noexcept {
      const auto added = builder.add(view<count>{value});
      VAST_ASSERT(added);
    },
    [&](const real_type&) noexcept {
      const auto added = builder.add(detail::narrow_cast<view<real>>(value));
      VAST_ASSERT(added);
    },
    [&](const duration_type&) noexcept {
      const auto added = builder.add(std::chrono::duration_cast<duration>(
        std::chrono::duration<count>{value}));
      VAST_ASSERT(added);
    },
    [&](const time_type&) noexcept {
      const auto added = builder.add(time{}
                                     + std::chrono::duration_cast<duration>(
                                       std::chrono::duration<count>{value}));
      VAST_ASSERT(added);
    },
    [&](const string_type&) noexcept {
      const auto added = builder.add(fmt::to_string(value));
      VAST_ASSERT(added);
    },
    [&](const pattern_type&) noexcept {
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const address_type&) noexcept {
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
    [&](const none_type&) noexcept {
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const bool_type&) noexcept {
      const auto added = builder.add(value != 0);
      VAST_ASSERT(added);
    },
    [&](const integer_type&) noexcept {
      const auto added = builder.add(
        view<integer>{detail::narrow_cast<integer::value_type>(value)});
      VAST_ASSERT(added);
    },
    [&](const count_type&) noexcept {
      const auto added = builder.add(detail::narrow_cast<count>(value));
      VAST_ASSERT(added);
    },
    [&](const real_type&) noexcept {
      const auto added = builder.add(value);
      VAST_ASSERT(added);
    },
    [&](const duration_type&) noexcept {
      const auto added = builder.add(std::chrono::duration_cast<duration>(
        std::chrono::duration<real>{value}));
      VAST_ASSERT(added);
    },
    [&](const time_type&) noexcept {
      const auto added = builder.add(time{}
                                     + std::chrono::duration_cast<duration>(
                                       std::chrono::duration<real>{value}));
      VAST_ASSERT(added);
    },
    [&](const string_type&) noexcept {
      const auto added = builder.add(fmt::to_string(value));
      VAST_ASSERT(added);
    },
    [&](const pattern_type&) noexcept {
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const address_type&) noexcept {
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
    [&](const none_type&) noexcept {
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const bool_type&) noexcept {
      const auto added = builder.add(value);
      VAST_ASSERT(added);
    },
    [&](const integer_type&) noexcept {
      const auto added = builder.add(value ? integer{1} : integer{0});
      VAST_ASSERT(added);
    },
    [&](const count_type&) noexcept {
      const auto added = builder.add(value ? count{1} : count{0});
      VAST_ASSERT(added);
    },
    [&](const real_type&) noexcept {
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
    [&](const pattern_type&) noexcept {
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const address_type&) noexcept {
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
    [&](const none_type&) noexcept {
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const bool_type&) noexcept {
      if (bool result = {}; parsers::json_boolean(value, result)) {
        const auto added = builder.add(result);
        VAST_ASSERT(added);
        return;
      }
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const integer_type&) noexcept {
      if (integer::value_type result = {}; parsers::json_int(value, result)) {
        const auto added = builder.add(view<integer>{result});
        VAST_ASSERT(added);
        return;
      }
      if (real result = {}; parsers::json_number(value, result)) {
        const auto added = builder.add(
          view<integer>{detail::narrow_cast<integer::value_type>(result)});
        VAST_ASSERT(added);
        return;
      }
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const count_type&) noexcept {
      if (count result = {}; parsers::json_count(value, result)) {
        const auto added = builder.add(result);
        VAST_ASSERT(added);
        return;
      }
      if (real result = {}; parsers::json_number(value, result)) {
        const auto added
          = builder.add(detail::narrow_cast<view<count>>(result));
        VAST_ASSERT(added);
        return;
      }
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const real_type&) noexcept {
      if (real result = {}; parsers::json_number(value, result)) {
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
      const auto added = builder.add(caf::none);
      VAST_ASSERT(added);
    },
    [&](const time_type&) noexcept {
      if (auto result = to<time>(value)) {
        const auto added = builder.add(*result);
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
    [&](const pattern_type&) noexcept {
      const auto added = builder.add(view<pattern>{value});
      VAST_ASSERT(added);
    },
    [&](const address_type&) noexcept {
      if (auto result = to<address>(value)) {
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
          const record_type& layout, std::string_view prefix) -> caf::error {
    for (const auto& field : layout.fields()) {
      auto handle_found = [&](const ::simdjson::dom::element& element) {
        auto f = detail::overload{
          [&](const map_type& mt) {
            if (element.is_object()) {
              const auto& object = element.get_object().value();
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
              self(self, element.get_object().value(), rt, "");
            } else {
              const auto added = builder.add(caf::none);
              VAST_ASSERT(added);
            }
          },
          [&](const auto&) {
            if (element.is_object()) {
              const auto added = builder.add(caf::none);
              VAST_ASSERT(added);
            } else {
              add(element, field.type, builder);
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
  const auto& layout = caf::get<record_type>(builder.layout());
  return self(self, object, layout, "");
}

writer::writer(ostream_ptr out, const caf::settings& options)
  : super{std::move(out)} {
  flatten_ = get_or(options, "vast.export.json.flatten", false);
  numeric_durations_
    = get_or(options, "vast.export.json.numeric-durations", false);
}

caf::error writer::write(const table_slice& x) {
  auto run = [&](const auto& printer) {
    if (flatten_)
      return print<policy::include_field_names, policy::flatten_layout>(
        printer, x, {", ", ": ", "{", "}"});
    return print<policy::include_field_names>(printer, x,
                                              {", ", ": ", "{", "}"});
  };
  if (numeric_durations_)
    return run(json_printer<policy::oneline, policy::numeric_durations>{});
  return run(json_printer<policy::oneline, policy::human_readable_durations>{});
}

const char* writer::name() const {
  return "json-writer";
}

} // namespace vast::format::json
