//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/concept/convertible/data.hpp>
#include <tenzir/concept/convertible/to.hpp>
#include <tenzir/error.hpp>
#include <tenzir/type.hpp>
#include <tenzir/validate.hpp>

namespace tenzir {

namespace {

auto validate_opaque_(const tenzir::data& data) -> caf::error {
  return match(data, tenzir::detail::overload{
                      [](const record&) -> caf::error {
                        return caf::error{};
                      },
                      [](auto const&) -> caf::error {
                        return caf::make_error(ec::invalid_argument,
                                               "only records may have 'opaque' "
                                               "attribute");
                      },
                    });
}

auto validate_(const tenzir::data& data, const tenzir::type& type,
               const enum tenzir::validate mode, const std::string& prefix = "",
               size_t depth = 0) -> caf::error {
  if (depth > tenzir::defaults::max_recursion)
    return caf::make_error(
      ec::invalid_configuration,
      fmt::format("too many layers of nesting at prefix {}", prefix));
  if (is<caf::none_t>(data)) {
    return caf::none;
  }
  if (!type)
    return caf::make_error(ec::invalid_configuration,
                           fmt::format("expected type for non-null value at {}",
                                       prefix));
  return match(type, tenzir::detail::overload{
      [&]<tenzir::basic_type T>(const T& type) -> caf::error {
        // TODO: Introduce special cases for accepting counts as integers and
        // vice versa if the number is in the valid range.
        if (!type_check(tenzir::type{type}, data))
          return caf::make_error(ec::invalid_configuration,
                                 fmt::format("expected value of type {} at "
                                             "{}",
                                             type, prefix));
        return caf::error{};
      },
      [&](const enumeration_type& u) {
        // The data is assumed to come from a configuration file, so any
        // enumeration value would be entered as a string.
        const auto* str = try_as<std::string>(&data);
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
      [&](const tenzir::list_type& list_type) {
        const auto* list = try_as<tenzir::list>(&data);
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
      [&](const tenzir::map_type& map_type) {
        const auto* map = try_as<tenzir::map>(&data);
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
      [&](const tenzir::record_type& record_type) {
        const auto* record = try_as<tenzir::record>(&data);
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
              return caf::make_error(tenzir::ec::invalid_configuration,
                                     fmt::format("unknown field {}.{}", prefix,
                                                 k));
            TENZIR_WARN("ignoring unknown config field {}.{}", prefix, k);
            continue;
          }
          auto field = record_type.field(*field_offset);
          if (field.type.attribute("opaque")) {
            if (auto error = validate_opaque_(v))
              return error;
            continue;
          }
          auto nested_prefix = fmt::format("{}.{}", prefix, field.name);
          // Note that this currently can not happen for configuration parsed
          // from a YAML file, since the parser already overwrites duplicate
          // fields with the same name (and the yaml spec doesn't allow it).
          if (prefixes.contains(nested_prefix))
            return caf::make_error(
              tenzir::ec::invalid_configuration,
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
    });
}

} // namespace

auto validate(const tenzir::data& data, const tenzir::record_type& schema,
              enum tenzir::validate mode) -> caf::error {
  return validate_(data, tenzir::type{schema}, mode);
}

} // namespace tenzir
