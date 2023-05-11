//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/concept/convertible/data.hpp>
#include <vast/concept/convertible/to.hpp>
#include <vast/error.hpp>
#include <vast/type.hpp>
#include <vast/validate.hpp>

namespace vast {

namespace {

auto validate_opaque_(const vast::data& data) -> caf::error {
  return caf::visit(vast::detail::overload{
                      [](const record&) -> caf::error {
                        return caf::error{};
                      },
                      [](auto const&) -> caf::error {
                        return caf::make_error(ec::invalid_argument,
                                               "only records may have 'opaque' "
                                               "attribute");
                      },
                    },
                    data);
}

auto validate_(const vast::data& data, const vast::type& type,
               const enum vast::validate mode, const std::string& prefix = "",
               size_t depth = 0) -> caf::error {
  if (depth > vast::defaults::max_recursion)
    return caf::make_error(
      ec::invalid_configuration,
      fmt::format("too many layers of nesting at prefix {}", prefix));
  if (caf::holds_alternative<caf::none_t>(data))
    return caf::none;
  if (!type)
    return caf::make_error(ec::invalid_configuration,
                           fmt::format("expected type for non-null value at {}",
                                       prefix));
  return caf::visit(
    vast::detail::overload{
      [&]<vast::basic_type T>(const T& type) -> caf::error {
        // TODO: Introduce special cases for accepting counts as integers and
        // vice versa if the number is in the valid range.
        if (!type_check(vast::type{type}, data))
          return caf::make_error(ec::invalid_configuration,
                                 fmt::format("expected value of type {} at "
                                             "{}",
                                             type, prefix));
        return caf::error{};
      },
      [&](const enumeration_type& u) {
        // The data is assumed to come from a configuration file, so any
        // enumeration value would be entered as a string.
        const auto* str = caf::get_if<std::string>(&data);
        if (!str)
          return caf::make_error(ec::invalid_configuration,
                                 fmt::format("expected enum value at {}",
                                             prefix));
        if (!u.resolve(*str).has_value())
          return caf::make_error(ec::invalid_configuration,
                                 fmt::format("invalid enum value '{}' at {}",
                                             *str, prefix));
        return caf::error{};
      },
      [&](const vast::list_type& list_type) {
        const auto* list = caf::get_if<vast::list>(&data);
        if (!list)
          return caf::make_error(ec::invalid_configuration,
                                 fmt::format("expected list at {}", prefix));
        size_t idx = 0;
        for (const auto& x : *list) {
          if (auto error
              = validate_(x, list_type.value_type(), mode,
                          fmt::format("{}[{}]", prefix, idx++), depth + 1))
            return error;
        }
        return caf::error{};
      },
      [&](const vast::map_type& map_type) {
        const auto* map = caf::get_if<vast::map>(&data);
        if (!map)
          return caf::make_error(ec::invalid_configuration,
                                 fmt::format("expected map at {}", prefix));
        for (const auto& [k, v] : *map) {
          if (auto error
              = validate_(k, map_type.key_type(), mode,
                          fmt::format("{}.{}", prefix, k), depth + 1))
            return error;
          if (auto error
              = validate_(v, map_type.value_type(), mode,
                          fmt::format("{}[{}]", prefix, k), depth + 1))
            return error;
        }
        return caf::error{};
      },
      [&](const vast::record_type& record_type) {
        const auto* record = caf::get_if<vast::record>(&data);
        if (!record)
          return caf::make_error(ec::invalid_configuration,
                                 fmt::format("expected record at {}", prefix));
        std::unordered_set<std::string> prefixes;
        // Go through the data and check that every field has the expected type.
        for (const auto& [k, v] : *record) {
          auto field_offset = record_type.resolve_key(k);
          if (!field_offset) {
            // In 'permissive' mode we ignore unknown fields.
            if (mode != validate::permissive)
              return caf::make_error(vast::ec::invalid_configuration,
                                     fmt::format("unknown field {}.{}", prefix,
                                                 k));
            VAST_WARN("ignoring unknown config field {}.{}", prefix, k);
            continue;
          }
          auto field = record_type.field(*field_offset);
          if (field.type.attribute("opaque"))
            return validate_opaque_(v);
          auto nested_prefix = fmt::format("{}.{}", prefix, field.name);
          // Note that this currently can not happen for configuration parsed
          // from a YAML file, since the parser already overwrites duplicate
          // fields with the same name (and the yaml spec doesn't allow it).
          if (prefixes.contains(nested_prefix))
            return caf::make_error(
              vast::ec::invalid_configuration,
              fmt::format("duplicate configuration field {}", nested_prefix));
          if (auto error
              = validate_(v, field.type, mode, nested_prefix, depth + 1))
            return error;
          prefixes.insert(nested_prefix);
        }
        // Verify that all required fields exist in the data.
        for (const auto& field : record_type.fields()) {
          bool required
            = field.type.attribute("required") || mode == validate::exhaustive;
          if (!required)
            continue;
          auto nested_prefix = fmt::format("{}.{}", prefix, field.name);
          if (prefixes.contains(nested_prefix))
            continue;
          auto it = record->find(field.name);
          if (it == record->end())
            return caf::make_error(ec::invalid_configuration,
                                   fmt::format("missing field {}.{}", prefix,
                                               field.name));
        }
        return caf::error{};
      },
    },
    type);
}

} // namespace

auto validate(const vast::data& data, const vast::record_type& schema,
              enum vast::validate mode) -> caf::error {
  return validate_(data, vast::type{schema}, mode);
}

} // namespace vast
