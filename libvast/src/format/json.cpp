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

// Various implementations for simdjson + type to element conversion.  The
// expand_records flag toggles whether we add nil values for all fields in
// records that cannot be found, which we do for records nested inside lists or
// maps for historical reasons.
data extract_impl(const ::simdjson::dom::element& value, const type& type,
                  bool expand_records);
data extract_impl(const ::simdjson::dom::array& values, const type& type,
                  bool expand_records);
data extract_impl(const ::simdjson::dom::object& value, const type& type,
                  bool expand_records);
data extract_impl(int64_t value, const type& type);
data extract_impl(uint64_t value, const type& type);
data extract_impl(double value, const type& type);
data extract_impl(bool value, const type& type);
data extract_impl(std::string_view value, const type& type);
data extract_impl(std::nullptr_t, const type& type, bool expand_records);

data extract_impl(const ::simdjson::dom::element& value, const type& type,
                  bool expand_records) {
  switch (value.type()) {
    case ::simdjson::dom::element_type::ARRAY:
      return extract_impl(value.get_array().value(), type, expand_records);
    case ::simdjson::dom::element_type::OBJECT:
      return extract_impl(value.get_object().value(), type, expand_records);
    case ::simdjson::dom::element_type::INT64:
      return extract_impl(value.get_int64().value(), type);
    case ::simdjson::dom::element_type::UINT64:
      return extract_impl(value.get_uint64().value(), type);
    case ::simdjson::dom::element_type::DOUBLE:
      return extract_impl(value.get_double().value(), type);
    case ::simdjson::dom::element_type::STRING:
      return extract_impl(value.get_string().value(), type);
    case ::simdjson::dom::element_type::BOOL:
      return extract_impl(value.get_bool().value(), type);
    case ::simdjson::dom::element_type::NULL_VALUE:
      return extract_impl(nullptr, type, expand_records);
  }
  die("unhandled JSON DOM element type");
}

data extract_impl(const ::simdjson::dom::array& values, const type& type,
                  bool expand_records) {
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
      // At this point we want to stop expanding records, so we always pass
      // false to nested extract calls.
      for (const auto& value : values)
        result.emplace_back(extract_impl(value, vt, false));
      return result;
    },
    [&](const map_type&) noexcept -> data {
      return caf::none;
    },
    [&](const record_type& rt) noexcept -> data {
      if (expand_records)
        return list(rt.num_leaves(), data{caf::none});
      return caf::none;
    },
  };
  return caf::visit(f, type);
}

data extract_impl(int64_t value, const type& type) {
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
      return list{extract_impl(value, lt.value_type())};
    },
    [&](const map_type&) noexcept -> data {
      return caf::none;
    },
    [&](const record_type& rt) noexcept -> data {
      return list(rt.num_leaves(), data{caf::none});
    },
  };
  return caf::visit(f, type);
}

data extract_impl(const ::simdjson::dom::object& value, const type& type,
                  bool expand_records) {
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
      // At this point we want to stop expanding records, so we always pass
      // false to nested extract calls.
      for (const auto& [k, v] : value)
        result.emplace(extract_impl(k, kt), extract_impl(v, vt, false));
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
        auto result = list{};
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
            result.emplace_back(caf::visit(recurse, field.type));
          } else {
            // (2) The field exists and we extracted it successfully.
            auto value = extract_impl(x.value(), field.type, expand_records);
            result.emplace_back(std::move(value));
          }
        }
        if (!expand_records)
          if (std::all_of(result.begin(), result.end(), [](const data& x) {
                return x == caf::none;
              }))
            return caf::none;
        return result;
      };
      return try_extract_record(try_extract_record, rt, "");
    },
  };
  return caf::visit(f, type);
}

data extract_impl(uint64_t value, const type& type) {
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
      return list{extract_impl(value, lt.value_type())};
    },
    [&](const map_type&) noexcept -> data {
      return caf::none;
    },
    [&](const record_type& rt) noexcept -> data {
      return list(rt.num_leaves(), data{caf::none});
    },
  };
  return caf::visit(f, type);
}

data extract_impl(double value, const type& type) {
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
      return list{extract_impl(value, lt.value_type())};
    },
    [&](const map_type&) noexcept -> data {
      return caf::none;
    },
    [&](const record_type& rt) noexcept -> data {
      return list(rt.num_leaves(), data{caf::none});
    },
  };
  return caf::visit(f, type);
}

data extract_impl(std::string_view value, const type& type) {
  auto f = detail::overload{
    [&](const none_type&) noexcept -> data {
      return caf::none;
    },
    [&](const bool_type&) noexcept -> data {
      if (bool result; parsers::json_boolean(value, result))
        return result;
      return caf::none;
    },
    [&](const integer_type&) noexcept -> data {
      if (integer::value_type result; parsers::json_int(value, result))
        return integer{result};
      if (real result; parsers::json_number(value, result))
        return integer{detail::narrow_cast<integer::value_type>(result)};
      return caf::none;
    },
    [&](const count_type&) noexcept -> data {
      if (count result; parsers::json_count(value, result))
        return result;
      if (real result; parsers::json_number(value, result))
        return detail::narrow_cast<count>(result);
      return caf::none;
    },
    [&](const real_type&) noexcept -> data {
      if (real result; parsers::json_number(value, result))
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
      return list{extract_impl(value, lt.value_type())};
    },
    [&](const map_type&) noexcept -> data {
      return caf::none;
    },
    [&](const record_type& rt) noexcept -> data {
      return list(rt.num_leaves(), data{caf::none});
    },
  };
  return caf::visit(f, type);
}

data extract_impl(bool value, const type& type) {
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
      return list{extract_impl(value, lt.value_type())};
    },
    [&](const map_type&) noexcept -> data {
      return caf::none;
    },
    [&](const record_type& rt) noexcept -> data {
      return list(rt.num_leaves(), data{caf::none});
    },
  };
  return caf::visit(f, type);
}

data extract_impl(std::nullptr_t, const type& type, bool expand_records) {
  if (!expand_records)
    return caf::none;
  auto f = detail::overload{
    [&](const record_type& rt) noexcept -> data {
      auto result = list{};
      result.reserve(rt.num_fields());
      for (const auto& field : rt.fields())
        result.emplace_back(extract_impl(nullptr, field.type, expand_records));
      return result;
    },
    [&]<concrete_type T>(const T&) noexcept -> data {
      return caf::none;
    },
  };
  auto result = caf::visit(f, type);
  return result;
}

} // namespace

data extract(const ::simdjson::dom::object& value, const type& type) {
  return extract_impl(value, type, true);
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
