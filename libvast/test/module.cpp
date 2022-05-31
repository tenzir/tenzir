//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE module

#include "vast/module.hpp"

#include "vast/aliases.hpp"
#include "vast/concept/convertible/data.hpp"
#include "vast/data.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/overload.hpp"
#include "vast/legacy_type.hpp"
#include "vast/taxonomies.hpp"
#include "vast/test/test.hpp"
#include "vast/type.hpp"

#include <caf/error.hpp>
#include <caf/sum_type.hpp>
#include <caf/test/dsl.hpp>
#include <fmt/format.h>

#include <algorithm>
#include <compare>
#include <iterator>
#include <optional>
#include <set>
#include <string>
#include <string_view>

using namespace vast;

constexpr auto reserved_names
  = std::array{"bool", "integer", "count",   "real",  "duration",
               "time", "string",  "pattern", "addr",  "subnet",
               "enum", "list",    "map",     "record"};

struct module_ng2 {
  std::vector<type> types;
  // FIXME: The order of types should not matter?
  friend bool operator==(const module_ng2& first,
                         const module_ng2& second) noexcept = default;
};

struct parsed_type {
  type parsed;                               // Potentially unresolved type
  std::vector<std::string_view> providers{}; // The types this type depends on
  bool is_algebra{}; // The record algebra must be executed to resolve the result
  //  std::vector<std::string> base_records; // FIXME: Idea
  // FIXME enum: extend, implant, base
  parsed_type(type&& new_parsed) : parsed(new_parsed) {
  }
  parsed_type(type&& new_parsed,
              std::vector<std::string_view>&& additional_providers)
    : parsed(new_parsed) {
    providers.insert(providers.end(),
                     std::make_move_iterator(additional_providers.begin()),
                     std::make_move_iterator(additional_providers.end()));
  }
  parsed_type(type&& new_parsed, std::string_view additional_provider)
    : parsed(new_parsed) {
    providers.push_back(additional_provider);
  }
};

/// Converts a declaration into a vast type.
/// @param declaration the type declaration parsed from yaml module config file
/// @param name the name for the declaration
caf::expected<parsed_type>
parse(const data& declaration, std::string_view name);

caf::expected<parsed_type>
parse(const std::vector<vast::data>& field_declarations) {
  if (field_declarations.empty())
    return caf::make_error(ec::parse_error,
                           fmt::format("record types must have at least "
                                       "one field; while parsing: {}",
                                       field_declarations));
  auto record_fields = std::vector<record_type::field_view>{};
  auto providers = std::vector<std::string_view>{};
  record_fields.reserve(field_declarations.size());
  for (const auto& record_value : field_declarations) {
    const auto* record_record_ptr = caf::get_if<record>(&record_value);
    if (record_record_ptr == nullptr)
      return caf::make_error(ec::parse_error,
                             fmt::format("a field in record type must be "
                                         "specified as a YAML dictionary, "
                                         "while parsing: {}",
                                         record_value));
    const auto& record_record = *record_record_ptr;
    if (record_record.size() != 1)
      return caf::make_error(ec::parse_error,
                             fmt::format("a field in a record type can "
                                         "have only a single key in the "
                                         "YAML dictionary; while parsing: {}",
                                         record_value));
    const auto type_or_error
      = parse(record_record.begin()->second, std::string_view{});
    if (!type_or_error)
      return caf::make_error(ec::parse_error,
                             "failed to parse record type field",
                             type_or_error.error());
    providers.insert(providers.end(),
                     std::make_move_iterator(type_or_error->providers.begin()),
                     std::make_move_iterator(type_or_error->providers.end()));
    record_fields.emplace_back(record_record.begin()->first,
                               type_or_error->parsed);
  }
  return parsed_type{type{record_type{record_fields}}, std::move(providers)};
}

caf::expected<parsed_type>
parse_enum(std::string_view name, const data& enumeration,
           std::vector<type::attribute_view>&& attributes) {
  const auto* enum_list_ptr = caf::get_if<list>(&enumeration);
  if (enum_list_ptr == nullptr)
    return caf::make_error(
      ec::parse_error, fmt::format("enum must be specified as a "
                                   "YAML list; while parsing: {} with name: {}",
                                   enumeration, name));
  const auto& enum_list = *enum_list_ptr;
  if (enum_list.empty())
    return caf::make_error(ec::parse_error, fmt::format("enum cannot be empty; "
                                                        "while parsing: {} "
                                                        "with name: {}",
                                                        enumeration, name));
  auto enum_fields = std::vector<enumeration_type::field_view>{};
  enum_fields.reserve(enum_list.size());
  for (const auto& enum_value : enum_list) {
    const auto* enum_string_ptr = caf::get_if<std::string>(&enum_value);
    if (enum_string_ptr == nullptr)
      return caf::make_error(ec::parse_error,
                             fmt::format("enum value must be specified "
                                         "as a YAML string; while parsing: {}",
                                         enum_value));
    enum_fields.push_back({*enum_string_ptr});
  }
  return parsed_type{
    type{name, enumeration_type{enum_fields}, std::move(attributes)}};
}

caf::expected<parsed_type>
parse_map(std::string_view name, const data& map_to_parse,
          std::vector<type::attribute_view>&& attributes) {
  const auto* map_record_ptr = caf::get_if<record>(&map_to_parse);
  if (map_record_ptr == nullptr)
    return caf::make_error(ec::parse_error,
                           fmt::format("a map type must be specified as "
                                       "a YAML dictionary; while parsing: {} "
                                       "with name: {}",
                                       map_to_parse, name));
  const auto& map_record = *map_record_ptr;
  const auto found_key = map_record.find("key");
  const auto found_value = map_record.find("value");
  if (found_key == map_record.end() || found_value == map_record.end())
    return caf::make_error(ec::parse_error,
                           fmt::format("a map type must have both a key "
                                       "and a value; while parsing: {} with "
                                       "name: {}",
                                       map_to_parse, name));
  const auto key_type_expected = parse(found_key->second, std::string_view{});
  if (!key_type_expected)
    return caf::make_error(ec::parse_error,
                           fmt::format("failed to parse map key while "
                                       "parsing: {} with name: {}",
                                       map_to_parse, name),
                           key_type_expected.error());
  const auto value_type_expected
    = parse(found_value->second, std::string_view{});
  if (!value_type_expected)
    return caf::make_error(ec::parse_error,
                           fmt::format("failed to parse map value while "
                                       "parsing: {} with name: {}",
                                       map_to_parse, name),
                           value_type_expected.error());
  VAST_TRACE(
    fmt::format("Creating map type with name: {}, "
                "placeholder key: {}, nested key type: {},"
                "placeholder value: {}, nested value type: {}",
                name, !key_type_expected->parsed, key_type_expected->parsed,
                !value_type_expected->parsed, value_type_expected->parsed));
  auto providers = std::vector(std::move(key_type_expected->providers));
  providers.insert(
    providers.end(),
    std::make_move_iterator(value_type_expected->providers.begin()),
    std::make_move_iterator(value_type_expected->providers.end()));
  return parsed_type{
    type{name, map_type{key_type_expected->parsed, value_type_expected->parsed},
         std::move(attributes)},
    std::move(providers)};
}

/// Only one of is_base and is_implement and is_extend can be true, if both
/// is_base and is_extend is false then is_extend is assumed to be true.
caf::expected<parsed_type> make_parsed_record_algebra(
  std::string_view name, parsed_type&& new_record, bool is_base,
  bool is_implant, const std::vector<std::string_view>& algebra_records) {
  const auto base_type = is_base      ? enumeration_type{{"base"}}
                         : is_implant ? enumeration_type{{"implant"}}
                                      : enumeration_type{{"extend"}};
  auto providers = std::vector(std::move(new_record.providers));
  auto record_types = std::vector<enumeration_type::field_view>{};
  record_types.reserve(algebra_records.size());
  for (auto type_name : algebra_records) {
    providers.emplace_back(type_name);
    record_types.push_back({type_name});
  }
  auto result
    = parsed_type(type{name, record_type{{"algebra_fields", new_record.parsed},
                                         {"base_type", base_type},
                                         {"algebra_records",
                                          enumeration_type{record_types}}}},
                  std::move(providers));
  result.is_algebra = true;
  return result;
}

caf::expected<parsed_type>
parse_record_algebra(std::string_view name, const data& record_algebra,
                     std::vector<type::attribute_view>&& attributes) {
  const auto* record_algebra_record_ptr = caf::get_if<record>(&record_algebra);
  if (record_algebra_record_ptr == nullptr)
    return caf::make_error(ec::parse_error,
                           fmt::format("record algebra must be "
                                       "specified as a YAML dictionary; while "
                                       "parsing: {} with name: {}",
                                       record_algebra, name));
  const auto& record_algebra_record = *record_algebra_record_ptr;
  const auto found_base = record_algebra_record.find("base");
  const auto found_implant = record_algebra_record.find("implant");
  const auto found_extend = record_algebra_record.find("extend");
  const auto is_base_found = found_base != record_algebra_record.end();
  const auto is_implant_found = found_implant != record_algebra_record.end();
  const auto is_extend_found = found_extend != record_algebra_record.end();
  int name_clash_specifier_cnt = 0;
  if (is_base_found)
    name_clash_specifier_cnt++;
  if (is_implant_found)
    name_clash_specifier_cnt++;
  if (is_extend_found)
    name_clash_specifier_cnt++;
  if (name_clash_specifier_cnt >= 2)
    return caf::make_error(
      ec::parse_error, fmt::format("record algebra must contain "
                                   "only one of 'base', 'implant', "
                                   "'extend'; while parsing: {} with name: {}",
                                   record_algebra, name));
  // create new record type
  const auto found_fields = record_algebra_record.find("fields");
  if (found_fields == record_algebra_record.end())
    return caf::make_error(
      ec::parse_error, fmt::format("record algebra must have one "
                                   "'fields'; while parsing: {}, with name: {}",
                                   record_algebra, name));
  const auto* fields_list_ptr = caf::get_if<list>(&found_fields->second);
  if (fields_list_ptr == nullptr)
    return caf::make_error(ec::parse_error,
                           fmt::format("'fields' in record algebra must "
                                       "be specified as YAML list; while "
                                       "parsing: {} with name: {}",
                                       record_algebra, name));
  const auto new_record_or_error = parse(*fields_list_ptr);
  if (!new_record_or_error)
    return caf::make_error(ec::parse_error,
                           fmt::format("failed to parse record algebra while "
                                       "parsing: {} with name: {}",
                                       record_algebra, name),
                           new_record_or_error.error());
  auto new_record = new_record_or_error.value();
  // retrieve records (base, implant or extend)
  if (name_clash_specifier_cnt == 0)
    // It is a normal record not a record algebra
    return parsed_type(type{name, new_record.parsed, std::move(attributes)},
                       std::move(new_record.providers));
  const auto& records = is_base_found      ? found_base->second
                        : is_implant_found ? found_implant->second
                                           : found_extend->second;
  const auto* records_list_ptr = caf::get_if<list>(&records);
  if (records_list_ptr == nullptr)
    return caf::make_error(ec::parse_error,
                           fmt::format("'base', 'implant' or 'extend' "
                                       "in a record algebra must be "
                                       "specified as a YAML list; while "
                                       "parsing: {} with name: {}",
                                       record_algebra, name));
  const auto& record_list = *records_list_ptr;
  if (record_list.empty())
    return caf::make_error(
      ec::parse_error, fmt::format("a record algebra cannot have an "
                                   "empty 'base', 'implant' or "
                                   "'extend'; while parsing: {} with name: {}",
                                   record_algebra, name));
  std::optional<record_type> merged_base_record{};
  std::vector<std::string_view> algebra_records{};
  algebra_records.reserve(record_list.size());
  for (const auto& record : record_list) {
    const auto& record_name_ptr = caf::get_if<std::string>(&record);
    if (record_name_ptr == nullptr)
      return caf::make_error(
        ec::parse_error,
        fmt::format("the 'base', 'implant' or 'extend' keywords of a "
                    "record algebra must be specified as a YAML string; while "
                    "parsing: {} with name: {}",
                    record_algebra, name));
    algebra_records.emplace_back(*record_name_ptr);
  }
  return make_parsed_record_algebra(name, std::move(new_record), is_base_found,
                                    is_implant_found, algebra_records);
}

/// Returns a built-in type or a partial placeholder type. The name and
/// attributes must be added by the caller. It does not handle inline
/// declarations.
caf::expected<parsed_type> parse_builtin(const data& declaration) {
  const auto* aliased_type_name_ptr = caf::get_if<std::string>(&declaration);
  if (aliased_type_name_ptr == nullptr)
    return caf::make_error(ec::parse_error,
                           fmt::format("built-in type can only be a "
                                       "string; while parsing: {}",
                                       declaration));
  const auto& aliased_type_name = *aliased_type_name_ptr;
  VAST_TRACE(
    fmt::format("Trying to create type aliased_type: {}", aliased_type_name));
  // Check built-in types first
  if (aliased_type_name == "bool")
    return parsed_type(type{bool_type{}});
  if (aliased_type_name == "integer")
    return parsed_type{type{integer_type{}}};
  if (aliased_type_name == "count")
    return parsed_type{type{count_type{}}};
  if (aliased_type_name == "real")
    return parsed_type{type{real_type{}}};
  if (aliased_type_name == "duration")
    return parsed_type{type{duration_type{}}};
  if (aliased_type_name == "time")
    return parsed_type{type{time_type{}}};
  if (aliased_type_name == "string")
    return parsed_type{type{string_type{}}};
  if (aliased_type_name == "pattern")
    return parsed_type{type{pattern_type{}}};
  if (aliased_type_name == "addr")
    return parsed_type{type{address_type{}}};
  if (aliased_type_name == "subnet")
    return parsed_type{type{subnet_type{}}};
  VAST_TRACE(fmt::format("Creating placeholder type for aliased_type: {}",
                         aliased_type_name));
  // Returning a partial placeholder
  return parsed_type{type{aliased_type_name, type{}}, aliased_type_name};
}

/// Can handle inline declartaions
/// FIXME: name optional?
caf::expected<parsed_type>
parse(const data& declaration, std::string_view name) {
  // Prevent using reserved names as types names
  if (std::any_of(reserved_names.begin(), reserved_names.end(),
                  [&](const auto& reserved_name) {
                    return name == reserved_name;
                  }))
    return caf::make_error(ec::parse_error,
                           fmt::format("type declaration cannot use a reserved "
                                       "name: {}; while parsing: {}",
                                       name, declaration));
  const auto* aliased_type_name_ptr = caf::get_if<std::string>(&declaration);
  // Type names can contain any character that the YAML parser can handle - no
  // need to check for allowed characters.
  if (aliased_type_name_ptr != nullptr) {
    auto alias = parse_builtin(declaration);
    if (!alias)
      return caf::make_error(ec::parse_error,
                             fmt::format("declaration must be a built-in type "
                                         "or a type alias while parsing: {} "
                                         "with name: {}",
                                         declaration, name),
                             alias.error());
    return parsed_type(type{name, alias->parsed}, std::move(alias->providers));
  }
  const auto* declaration_record_ptr = caf::get_if<record>(&declaration);
  if (declaration_record_ptr == nullptr)
    return caf::make_error(ec::parse_error,
                           fmt::format("parses type alias with invalid "
                                       "format"
                                       "type alias must be specified as a "
                                       "YAML dictionary; while parsing: {} "
                                       "with name: {}",
                                       declaration, name));
  const auto& declaration_record = *declaration_record_ptr;
  // Get the optional attributes
  auto attributes = std::vector<type::attribute_view>{};
  const auto found_attributes = declaration_record.find("attributes");
  if (found_attributes != declaration_record.end()) {
    const auto* attribute_list = caf::get_if<list>(&found_attributes->second);
    if (attribute_list == nullptr)
      return caf::make_error(ec::parse_error,
                             fmt::format("the attribute list must be "
                                         "specified as a YAML list; while "
                                         "parsing: {} with name: {}",
                                         declaration, name));
    attributes.reserve(attribute_list->size());
    for (const auto& attribute : *attribute_list) {
      const auto* attribute_string_ptr = caf::get_if<std::string>(&attribute);
      if (attribute_string_ptr != nullptr)
        attributes.push_back({*attribute_string_ptr});
      else {
        const auto* attribute_record_ptr = caf::get_if<record>(&attribute);
        if (attribute_record_ptr == nullptr)
          return caf::make_error(ec::parse_error,
                                 fmt::format("attribute must be specified "
                                             "as a YAML dictionary: {}; while "
                                             "parsing: {} with name: {}",
                                             attribute, declaration, name));
        const auto& attribute_record = *attribute_record_ptr;
        if (attribute_record.size() != 1)
          return caf::make_error(ec::parse_error,
                                 fmt::format("attribute must have a "
                                             "single field: {}; while parsing: "
                                             "{} with name: {}",
                                             attribute, declaration, name));
        const auto& attribute_key = attribute_record.begin()->first;
        const auto* attribute_value_ptr
          = caf::get_if<std::string>(&attribute_record.begin()->second);
        if (attribute_value_ptr == nullptr) {
          if (attribute_record.begin()->second == vast::data{}) {
            attributes.push_back({attribute_key});
            continue;
          }
          return caf::make_error(ec::parse_error,
                                 fmt::format("attribute must be a string: {}; "
                                             "while parsing: {} with name: {}",
                                             attribute, declaration, name));
        }
        const auto& attribute_value = *attribute_value_ptr;
        attributes.push_back({attribute_key, attribute_value});
      }
    }
  }
  // Check that only one of type, enum, list, map and record is specified
  // by the user
  const auto found_type = declaration_record.find("type");
  const auto found_enum = declaration_record.find("enum");
  const auto found_list = declaration_record.find("list");
  const auto found_map = declaration_record.find("map");
  const auto found_record = declaration_record.find("record");
  const auto is_type_found = found_type != declaration_record.end();
  const auto is_enum_found = found_enum != declaration_record.end();
  const auto is_list_found = found_list != declaration_record.end();
  const auto is_map_found = found_map != declaration_record.end();
  const auto is_record_found = found_record != declaration_record.end();
  int type_selector_cnt
    = static_cast<int>(is_type_found) + static_cast<int>(is_enum_found)
      + static_cast<int>(is_list_found) + static_cast<int>(is_map_found)
      + static_cast<int>(is_record_found);
  if (type_selector_cnt != 1)
    return caf::make_error(ec::parse_error,
                           fmt::format("one of type, enum, list, map, "
                                       "record is expected; while parsing: {} "
                                       "with name: {}",
                                       declaration, name));
  // Type alias
  if (is_type_found) {
    // It can only be a built int type
    auto type_expected = parse_builtin(found_type->second);
    if (!type_expected)
      return caf::make_error(ec::parse_error,
                             fmt::format("failed to parse type alias while "
                                         "parsing: {} with name: {}",
                                         declaration, name),
                             type_expected.error());
    if (!type_expected->parsed)
      VAST_TRACE(fmt::format("Creating a placeholder with name: {}, "
                             "nested_type: {}",
                             name, type_expected->parsed));
    else
      VAST_TRACE(fmt::format("Creating type with name: {}, "
                             "nested_type: {}",
                             name, type_expected->parsed));
    // create a type alias or a placeholder.
    return parsed_type(type{name, type_expected->parsed, std::move(attributes)},
                       std::move(type_expected->providers));
  }
  // Enumeration
  if (is_enum_found)
    return parse_enum(name, found_enum->second, std::move(attributes));
  // List
  if (is_list_found) {
    auto type_expected = parse(found_list->second, std::string_view{});
    if (!type_expected)
      return caf::make_error(ec::parse_error,
                             fmt::format("failed to parse list while parsing: "
                                         "{} with name: {}",
                                         declaration, name),
                             type_expected.error());
    if (!type_expected->parsed)
      VAST_TRACE(fmt::format("Creating placeholder list type with name: {}, "
                             "nested_type: {}",
                             name, type_expected->parsed));
    else
      VAST_TRACE(fmt::format("Creating list type with name: {}, "
                             "nested_type: {}",
                             name, type_expected->parsed));
    return parsed_type(type{name, list_type{type_expected->parsed},
                            std::move(attributes)},
                       std::move(type_expected->providers));
  }
  // Map
  if (is_map_found)
    return parse_map(name, found_map->second, std::move(attributes));
  // Record or Record algebra
  if (is_record_found) {
    const auto* record_list_ptr = caf::get_if<list>(&found_record->second);
    if (record_list_ptr != nullptr) {
      // Record
      auto new_record = parse(*record_list_ptr);
      if (!new_record)
        return caf::make_error(ec::parse_error,
                               fmt::format("failed to parse record while "
                                           "parsing: {} with name: {}",
                                           declaration, name),
                               new_record.error());
      return parsed_type(type{name, new_record->parsed, std::move(attributes)},
                         std::move(new_record->providers));
    }
    // Record algebra
    return parse_record_algebra(name, found_record->second,
                                std::move(attributes));
  }
  return caf::make_error(ec::logic_error, "unknown type found when parsing");
}

caf::expected<parsed_type>
parse(const record::value_type& variable_declaration) {
  return parse(variable_declaration.second, variable_declaration.first);
}

caf::expected<type> resolve(const bool is_algebra, const type& to_resolve,
                            const std::vector<type>& resolved_types);

struct placeholder {
  std::string_view name;
  std::string_view aliased_name;
};

/// Returns true if it is a placeholder type.
inline bool is_placeholder(const type& placeholder_candidate) {
  return !placeholder_candidate;
}

std::optional<placeholder>
try_read_placeholder(const type& placeholder_candidate) {
  if (!placeholder_candidate) {
    std::string_view last_name;
    for (const auto& name : placeholder_candidate.names())
      last_name = name;
    // The placeholder type is the only none type when parsing.
    return placeholder{placeholder_candidate.name(), last_name};
  }
  return std::nullopt;
}

caf::expected<type>
resolve_placeholder_or_inline(const type& unresolved_type,
                              const std::vector<type>& resolved_types) {
  if (auto placeholder = try_read_placeholder(unresolved_type); placeholder) {
    VAST_TRACE("Resolving placeholder with "
               "name: {}",
               placeholder->name);
    const auto type_found
      = std::find_if(resolved_types.begin(), resolved_types.end(),
                     [&](const auto& resolved_type) {
                       return placeholder->aliased_name == resolved_type.name();
                     });
    if (type_found == resolved_types.end())
      return caf::make_error(ec::parse_error, ""); // FIXME:
    return *type_found;
  }
  VAST_TRACE("Resolving inline type");
  return resolve(false, unresolved_type, resolved_types);
}

/// Returns the list type. The name and attribute must be added by the caller.
caf::expected<type> resolve_list(const list_type& unresolved_list_type,
                                 const std::vector<type>& resolved_types) {
  VAST_TRACE("Resolving list_type");
  const auto resolved_type = resolve_placeholder_or_inline(
    unresolved_list_type.value_type(), resolved_types);
  if (!resolved_type)
    return caf::make_error(ec::parse_error,
                           "Failed to resolve list type: "); // FIXME:
  return type{list_type{*resolved_type}};
}

caf::expected<type> resolve_map(const map_type& unresolved_map_type,
                                const std::vector<type>& resolved_types) {
  VAST_TRACE("Resolving map_type");
  const auto resolved_key_type = resolve_placeholder_or_inline(
    unresolved_map_type.key_type(), resolved_types);
  if (!resolved_key_type)
    return caf::make_error(ec::parse_error,
                           "Failed to resolve map key type: "); // FIXME:
  const auto resolved_value_type = resolve_placeholder_or_inline(
    unresolved_map_type.value_type(), resolved_types);
  if (!resolved_value_type)
    return caf::make_error(ec::parse_error,
                           "Failed to resolve map value type: "); // FIXME:
  return type{map_type{*resolved_key_type, *resolved_value_type}};
}

caf::expected<type> resolve_record(const record_type& unresolved_record_type,
                                   const std::vector<type>& resolved_types) {
  VAST_TRACE("Resolving record_type");
  auto record_fields = std::vector<record_type::field_view>{};
  for (const auto& field : unresolved_record_type.fields()) {
    const auto& resolved_type
      = resolve_placeholder_or_inline(field.type, resolved_types);
    if (!resolved_type)
      return caf::make_error(ec::parse_error,
                             "Failed to resolve record field key type",
                             resolved_type.error());
    record_fields.emplace_back(field.name, *resolved_type);
  }
  return type{record_type{record_fields}};
}

caf::expected<type> resolve(const bool is_algebra, const type& to_resolve,
                            const std::vector<type>& resolved_types) {
  if (is_algebra) {
    const auto& algebra_ptr = caf::get_if<record_type>(&to_resolve);
    if (!algebra_ptr)
      return caf::make_error(ec::parse_error, "invalid format in record "
                                              "algebra");
    // "algebra_fields"
    const auto& algebra_fields_type = algebra_ptr->field(0).type;
    const auto& algebra_fields = caf::get_if<record_type>(&algebra_fields_type);
    if (!algebra_fields)
      return caf::make_error(ec::logic_error, "no algebra fields found");
    // "base_type"
    const auto& base_type_type = algebra_ptr->field(1).type;
    const auto& base_type = caf::get_if<enumeration_type>(&base_type_type);
    if (!base_type)
      return caf::make_error(ec::logic_error, "no base type found");
    // "algebra_records"
    const auto& algebra_records_type = algebra_ptr->field(2).type;
    const auto& algebra_records
      = caf::get_if<enumeration_type>(&algebra_records_type);
    if (!algebra_records)
      return caf::make_error(ec::logic_error, "no algebra records found");
    // set conflict handling
    record_type::merge_conflict merge_conflict_handling
      = record_type::merge_conflict::fail;
    if (base_type->field(0) == "implant")
      merge_conflict_handling = record_type::merge_conflict::prefer_left;
    if (base_type->field(0) == "extend")
      merge_conflict_handling = record_type::merge_conflict::prefer_right;
    const auto& new_record
      = resolve(false, algebra_fields_type, resolved_types);
    if (!new_record)
      return caf::make_error(ec::parse_error, "failed to resole algebra fields",
                             new_record.error());
    std::optional<record_type> merged_base_record{};
    VAST_WARN("merging {} records", algebra_records->fields().size());
    for (const auto& record : algebra_records->fields()) {
      const auto& base_type
        = std::find_if(resolved_types.begin(), resolved_types.end(),
                       [&](const auto& resolved_type) {
                         return record.name == resolved_type.name();
                       });
      // FIXME: no duplicates should be allowed into resolved_types, check
      // somewhere!
      if (base_type == resolved_types.end())
        return caf::make_error(ec::logic_error, "base type is not resolved "
                                                "yet");
      // FIXME: for (const auto& attribute : base_type->attributes())
      //  attributes.push_back(attribute);
      const auto resolved_base_type = *base_type;
      const auto* base_record_ptr
        = caf::get_if<record_type>(&resolved_base_type);
      if (base_record_ptr == nullptr)
        return caf::make_error(ec::parse_error,
                               "parses a record algebra base, implant or "
                               "extend with invalid format");
      if (!merged_base_record) {
        merged_base_record = *base_record_ptr;
        continue;
      }
      VAST_WARN("merging: {} with: {}", *merged_base_record, *base_record_ptr);
      const auto new_merged_base_record
        = merge(*merged_base_record, *base_record_ptr,
                record_type::merge_conflict::fail);
      if (!new_merged_base_record)
        return caf::make_error(ec::parse_error,
                               "parses conflicting record types when parsing a "
                               "record algebra base, implant or extend");
      merged_base_record = *new_merged_base_record;
    }
    const auto* resolved_record_ptr
      = caf::get_if<record_type>(&*new_record); // FIXME: &*?
    if (!resolved_record_ptr)
      return caf::make_error(ec::logic_error, "new record is not a "
                                              "record_type");
    VAST_WARN("merging final record: {} with: {}", *merged_base_record,
              *resolved_record_ptr);
    const auto final_merged_record = merge(
      *merged_base_record, *resolved_record_ptr, merge_conflict_handling);
    if (!final_merged_record)
      return caf::make_error(ec::parse_error,
                             "failed to merge records while evaluating record "
                             "algebra",
                             final_merged_record.error());
    VAST_WARN("merging result: type: {}, name: {}", *final_merged_record,
              to_resolve.name());
    return type{to_resolve.name(), *final_merged_record};
  }
  if (auto placeholder = try_read_placeholder(to_resolve); placeholder) {
    const auto type_found
      = std::find_if(resolved_types.begin(), resolved_types.end(),
                     [&](const auto& resolved_type) {
                       return placeholder->aliased_name == resolved_type.name();
                     });
    // FIXME: no duplicates should be allowed into resolved_types, check
    // somewhere!
    if (type_found == resolved_types.end())
      return caf::make_error(
        ec::logic_error, fmt::format("placeholder type is not resolved yet "
                                     "while trying to resolve placeholder: {}",
                                     placeholder->aliased_name));
    // FIXME: TEST if the placeholder type is not a Type Alias!
    return type{placeholder->name, *type_found};
  }
  const auto collect_unresolved = detail::overload{
    [&](const list_type& unresolved_list_type) {
      return resolve_list(unresolved_list_type, resolved_types);
    },
    [&](const map_type& unresolved_map_type) {
      return resolve_map(unresolved_map_type, resolved_types);
    },
    [&](const record_type& unresolved_record_type) {
      return resolve_record(unresolved_record_type, resolved_types);
    },
    [&](const concrete_type auto& resolved_type) {
      caf::expected<type> result = type{resolved_type};
      return result;
    },
  };
  VAST_TRACE("Resolving complex type: {}", to_resolve);
  const auto resolution_result = caf::visit(collect_unresolved, to_resolve);
  if (resolution_result)
    return type{to_resolve.name(), *resolution_result};
  return caf::make_error(ec::logic_error, "Unexpected resolution failure",
                         resolution_result.error());
}

class resolution_manager {
public:
  caf::expected<parsed_type>
  next_to_resolve(const std::vector<parsed_type>& unresolved_types) {
    while (true) {
      if (resolving_types_.empty()) {
        const auto& to_resolve = unresolved_types.front();
        resolving_types_.push_back(to_resolve.parsed.name());
      }
      const auto type_name_to_resolve = resolving_types_.back();
      const auto& type_to_resolve
        = std::find_if(unresolved_types.begin(), unresolved_types.end(),
                       [&](const auto& current_parsed_type) {
                         return current_parsed_type.parsed.name()
                                == type_name_to_resolve;
                       });
      // Not amongst unresolved types so it should  be resolved already
      if (type_to_resolve == unresolved_types.end())
        //  return type_to_resolve->parsed;
        return caf::make_error(ec::logic_error,
                               fmt::format("unresolved type expected when "
                                           "trying to resolve: {}",
                                           type_name_to_resolve));
      if (!type_to_resolve->providers.empty()) {
        resolving_types_.insert(resolving_types_.end(),
                                type_to_resolve->providers.begin(),
                                type_to_resolve->providers.end());
      }
      if (type_name_to_resolve == resolving_types_.back())
        return *type_to_resolve;
    };
  }
  void resolved() {
    resolving_types_.pop_back();
  }

private:
  std::vector<std::string_view> resolving_types_{};
};

caf::expected<module_ng2> to_module2(const data& raw_module) {
  const auto* module_declaration_ptr = caf::get_if<record>(&raw_module);
  if (module_declaration_ptr == nullptr)
    return caf::make_error(ec::parse_error, "parses a module with an invalid "
                                            "format");
  const auto& module_declaration = *module_declaration_ptr;
  // types
  const auto found_types = module_declaration.find("types");
  if (found_types == module_declaration.end())
    return caf::make_error(ec::parse_error, "parses a module with no types");
  const auto* types_ptr = caf::get_if<record>(&found_types->second);
  if (types_ptr == nullptr)
    return caf::make_error(ec::parse_error, "parses a module with invalid "
                                            "types");
  const auto& types = *types_ptr;
  if (types.empty()) // FIXME: Maybe we allow empty types in module?
    return caf::make_error(ec::parse_error, "parses a module with empty "
                                            "types");
  std::vector<parsed_type> parsed_types;
  // Resolve aliases to built-in types or create placeholder types
  for (const auto& current_type : types) {
    const auto parsed_type = parse(current_type);
    if (!parsed_type)
      return caf::make_error(
        ec::parse_error, fmt::format("failed to parse type: {}", current_type),
        parsed_type.error());
    parsed_types.push_back(*parsed_type);
  }
  std::vector<type> resolved_types;
  // Move parsed items that are already resolved to resolved types.
  const auto resolved_items = std::stable_partition(
    parsed_types.begin(), parsed_types.end(), [](const auto& current_type) {
      return !current_type.providers.empty();
    });
  // FIXME: insert / backinserter / moveiterator
  for (auto i = resolved_items; i < parsed_types.end(); i++)
    resolved_types.push_back(i->parsed);
  parsed_types.erase(resolved_items, parsed_types.end());
  /*  for (auto& parsed_type : parsed_types) {
    VAST_ERROR("Unresolved: {}", parsed_type.parsed);
    for (auto& provider : parsed_type.providers) {
      VAST_ERROR("  provider: {}", provider);
    }
    }*/
  // Remove dependencies already resolved
  for (auto& parsed_type : parsed_types) {
    std::erase_if(parsed_type.providers, [&](const auto& current_provider) {
      return std::any_of(resolved_types.begin(), resolved_types.end(),
                         [&](const auto& resolved_type) {
                           return current_provider == resolved_type.name();
                         });
    });
  }
  // From this point on parsed types contain only types that need to be
  // resolved.
  VAST_TRACE("Before resolving {}", parsed_types.empty());
  resolution_manager manager{};
  // FIXME: Validate that all dependency can be resolved (all proveders are
  // amongst  parsed.names)
  // FIXME: Validate that there are no duplicates
  // FIXME: Check for circular dependencies
  while (!parsed_types.empty()) {
    // FIXME: In case of an invalid schema parsed_types may never get empty!
    const auto type_to_resolve = manager.next_to_resolve(parsed_types);
    if (!type_to_resolve)
      return caf::make_error(ec::parse_error,
                             "failed to determine the next type to resolve",
                             type_to_resolve.error());
    VAST_TRACE("Next to resolve: {}, is algebra: {}", type_to_resolve->parsed,
               type_to_resolve->is_algebra);
    const auto resolve_result = resolve(
      type_to_resolve->is_algebra, type_to_resolve->parsed, resolved_types);
    if (!resolve_result)
      return caf::make_error(ec::parse_error,
                             fmt::format("Failed to resolve: {}",
                                         type_to_resolve->parsed),
                             resolve_result.error());
    const auto& resolved_type = *resolve_result;
    std::erase_if(parsed_types, [&](const auto& current_type) {
      return current_type.parsed.name() == resolved_type.name();
    });
    VAST_TRACE("Resolved: {}", resolved_type.name());
    resolved_types.push_back(resolved_type);
    // Remove the resolved dependency
    for (auto& parsed_type : parsed_types) {
      std::erase_if(parsed_type.providers, [&](const auto& current) {
        return current == resolved_type.name();
      });
    }
    manager.resolved();
  }
  return module_ng2{.types = resolved_types};
}

//////////////////////////////////////////////////////////////////////////////
// Strawman API
// FIXME: Move it to its final place
/////////////////////////////////////////////////////////////////////////////

/// The resolved Module parsed from multiple module files in different module
/// directories.
struct module_ng {
  /// The path to the module files.
  std::vector<std::string> filenames = {};

  /// The map of the module names to the type names within the module and the
  /// parsed configuration from the YAML configuration file.
  std::map<std::string, std::map<std::string, record>> dir;

  /// The name of the module
  std::string name = {};

  /// The description of the module
  std::string description = {};

  std::vector<std::string> references = {};
  std::vector<type> types = {};
  concepts_map concepts = {};
  models_map models = {};

  template <class Inspector>
  friend auto inspect(Inspector& f, module_ng& x) {
    return f(x.name, x.description, x.references, x.types, x.concepts,
             x.models);
  }

  inline static const record_type& layout() noexcept {
    static const auto result = record_type{
      {"module", string_type{}},
      {"description", string_type{}},
      {"references", list_type{string_type{}}},
      {"types", list_type{record_type{}}},
      {"concepts", concepts_data_layout},
      {"models", models_data_layout},
    };
    return result;
  };

  // MAYBE:static caf::error merge(const module& other);
  // MAYBE static module combine(const module& other);
};

caf::expected<module_ng>
load_module_ng(const std::filesystem::path& module_file);

/// The global identifier namespace of modules.
struct module_gin {
  std::map<std::string, module_ng> modules;

  template <class Inspector>
  friend auto inspect(Inspector& f, module_gin& x) {
    return f(x.modules);
  }

private:
  caf::error
  load_recursive(const detail::stable_set<std::filesystem::path>& module_dirs,
                 size_t max_recursion = defaults::max_recursion);
};

// FIXME: Maybe not needed
// static caf::expected<module_gin>
// load_module_gin(const caf::actor_system_config& cfg);

using symbol_table_ng = std::map<std::string, type>;

caf::error
convert(const record& input, type& out, const symbol_table_ng& table);

caf::error convert(const record&, type&, const symbol_table_ng&) {
  return caf::none;
}

// TEST(multiple members) {
//  auto x = module_ng{};
//  type y = type{};
//  auto table = symbol_table_ng{};
//   auto r = record{
//     {"module", std::string{"foo"}},
//     {"description", std::string{"blab"}},
//     {"references", list{{std::string{"http://foo.com"}},
//                         {std::string{"https://www.google.com/search?q=foo"}}}},
//     {"types",
//      record{
//        {"id", record{{"type", std::string{"string"}},
//                      {"description", std::string{"A random unique ID
//                      with..."}},
//                      {"attributes", record{{"index", std::string{"has"
//   //  REQUIRE_SUCCESS(convert(r, y, table)); // FIXME: x instead of y
//                                                                  "h"}}}}}}}}};
//  Parsing string_type with attributes
//}

/////////////////////////////////////////////////////////////////////////////
// The unit tests
/////////////////////////////////////////////////////////////////////////////

TEST(Parsing string type) {
  const auto declaration = record{{
    "types",
    record{
      {"string_field1", "string"},
    },
  }};
  const auto result = unbox(to_module2(declaration));
  const auto expected_result
    = module_ng2{.types = {
                   type{"string_field1", string_type{}},
                 }};
  CHECK_EQUAL(result, expected_result);
  const auto declaration2 = record{{
    "types",
    record{
      {
        "string_field1",
        record{{"type", "string"}},
      },
    },
  }};
  const auto result2 = unbox(to_module2(declaration));
  CHECK_EQUAL(result2, expected_result);
  const auto declaration3 = record{{
    "types",
    record{
      {
        "string_field2",
        record{{"type", "string"},
               {"attributes", list{"ioc", // record{{"ioc2", nullptr}},
                                   record{{"index", "hash"}}}}},
      },
    },
  }};
  const auto result3 = unbox(to_module2(declaration3));
  const auto expected_result3 = module_ng2{
    .types = {type{
      "string_field2", string_type{}, {{"ioc"}, {"index", "hash"}}, //, {"ioc2"}
    }}};
  CHECK_EQUAL(result3, expected_result3);
}

TEST(parsing bool type) {
  const auto declaration = record{{
    "types",
    record{
      {
        "bool_field",
        record{{"type", "bool"}},
      },
    },
  }};
  const auto result = unbox(to_module2(declaration));
  const auto expected_type = module_ng2{.types = {
                                          type{"bool_field", bool_type{}},
                                        }};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing integer type) {
  const auto declaration = record{{
    "types",
    record{
      {
        "int_field",
        record{{"type", "integer"}},
      },
    },
  }};
  const auto result = unbox(to_module2(declaration));
  const auto expected_type = module_ng2{.types = {
                                          type{"int_field", integer_type{}},
                                        }};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing count_type) {
  const auto declaration = record{{
    "types",
    record{
      {
        "count_field",
        record{{"type", "count"}},
      },
    },
  }};
  const auto result = unbox(to_module2(declaration));
  const auto expected_type = module_ng2{.types = {
                                          type{"count_field", count_type{}},
                                        }};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing real_type) {
  const auto declaration = record{{
    "types",
    record{
      {
        "real_field",
        record{{"type", "real"}},
      },
    },
  }};
  const auto result = unbox(to_module2(declaration));
  const auto expected_type = module_ng2{.types = {
                                          type{"real_field", real_type{}},
                                        }};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing duration_type) {
  const auto declaration = record{{
    "types",
    record{
      {
        "duration_field",
        record{{"type", "duration"}},
      },
    },
  }};
  const auto result = unbox(to_module2(declaration));
  const auto expected_type
    = module_ng2{.types = {
                   type{"duration_field", duration_type{}},
                 }};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing time_type) {
  const auto declaration = record{{
    "types",
    record{
      {
        "time_field",
        record{{"type", "time"}},
      },
    },
  }};
  const auto result = unbox(to_module2(declaration));
  const auto expected_type = module_ng2{.types = {
                                          type{"time_field", time_type{}},
                                        }};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing string_type without attributes) {
  const auto declaration = record{{
    "types",
    record{
      {
        "string_field",
        record{{"type", "string"}},
      },
    },
  }};
  const auto result = unbox(to_module2(declaration));
  const auto expected_type = module_ng2{.types = {
                                          type{"string_field", string_type{}},
                                        }};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing pattern_type) {
  const auto declaration = record{{
    "types",
    record{
      {
        "pattern_field",
        record{{"type", "pattern"}},
      },
    },
  }};
  const auto result = unbox(to_module2(declaration));
  const auto expected_type = module_ng2{.types = {
                                          type{"pattern_field", pattern_type{}},
                                        }};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing address_type) {
  const auto declaration = record{{
    "types",
    record{
      {
        "address_field",
        record{{"type", "addr"}},
      },
    },
  }};
  const auto result = unbox(to_module2(declaration));
  const auto expected_type = module_ng2{.types = {
                                          type{"address_field", address_type{}},
                                        }};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing subnet_type) {
  const auto declaration = record{{
    "types",
    record{
      {
        "subnet_field",
        record{{"type", "subnet"}},
      },
    },
  }};
  const auto result = unbox(to_module2(declaration));
  const auto expected_type = module_ng2{.types = {
                                          type{"subnet_field", subnet_type{}},
                                        }};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing enumeration_type) {
  const auto declaration = record{{
    "types",
    record{
      {
        "enum_field",
        record{{"enum", list{"on", "off", "unknown"}}},
      },
    },
  }};
  const auto result = unbox(to_module2(declaration));
  const auto expected_type = module_ng2{
    .types = {
      type{"enum_field", enumeration_type{{"on"}, {"off"}, {"unknown"}}},
    }};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing list_type) {
  const auto declaration = record{{
    "types",
    record{
      {
        "list_field",
        record{{"list", "count"}},
      },
    },
  }};
  const auto result = unbox(to_module2(declaration));
  const auto expected_type
    = module_ng2{.types = {
                   type{"list_field", list_type{count_type{}}},
                 }};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing map_type) {
  const auto declaration = record{{
    "types",
    record{
      {
        "map_field",
        record{
          {"map", record{{"key", "count"}, {"value", "string"}}},
        },
      },
    },
  }};
  const auto result = unbox(to_module2(declaration));
  const auto expected_type
    = module_ng2{.types = {
                   type{
                     "map_field",
                     map_type{count_type{}, string_type{}},
                   },
                 }};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing record_type) {
  const auto declaration = record{{
    "types",
    record{
      {
        "record_field",
        record{{
          "record",
          list{
            record{{"src_ip", "string"}},
            record{{"dst_ip", "string"}},
          },
        }},
      },
    },
  }};
  const auto result = unbox(to_module2(declaration));
  const auto expected_type = module_ng2{.types = {
                                          type{
                                            "record_field",
                                            record_type{
                                              {"src_ip", string_type{}},
                                              {"dst_ip", string_type{}},
                                            },
                                          },
                                        }};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing inline record_type) {
  const auto declaration = record{{
    "types",
    record{
      {
        "record_field",
        record{{"record",
                list{
                  record{{"source", record{{"type", "string"}}}},
                  record{{"destination", record{{"type", "string"}}}},
                }}},
      },
    },
  }};
  const auto result = unbox(to_module2(declaration));
  const auto expected_type = module_ng2{.types = {
                                          type{
                                            "record_field",
                                            record_type{
                                              {"source", string_type{}},
                                              {"destination", string_type{}},
                                            },
                                          },
                                        }};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing inline record_type with attributes) {
  const auto declaration = record{{
    "types",
    record{
      {
        "record_field",
        record{{
          "record",
          list{record{{
                 "source",
                 record{
                   {"type", "string"},
                   {"attributes", list{"originator"}},
                 },
               }},
               record{
                 {"destination",
                  record{
                    {"type", "string"},
                    {"attributes", list{"responder"}},
                  }},
               }},
        }},
      },
    },
  }};
  const auto result = unbox(to_module2(declaration));
  const auto expected_type
    = module_ng2{.types = {
                   type{
                     "record_field",
                     record_type{
                       {"source", type{string_type{}, {{"originator"}}}},
                       {"destination", type{string_type{}, {{"responder"}}}},
                     },
                   },
                 }};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing record algebra) {
  ////////////////////////////////////////////
  const auto expected_base_record_type
    = type{"common", record_type{{"field", bool_type{}}}};
  const auto expected_record_algebra = type{
    "record_algebra_field",
    record_type{
      {"field", type{bool_type{}}},
      {"msg", type{string_type{}}},
    },
  };
  // Base Record Algebra test with name clash
  // Extend Record Algebra test with name clash
  const auto expected_extended_record_algebra = type{
    "record_algebra_field",
    record_type{
      {"field", type{string_type{}}},
    },
  };
  // Implant Record Algebra test with name clash
  const auto expected_implanted_record_algebra = type{
    "record_algebra_field",
    record_type{
      {"field", type{bool_type{}}},
    },
  };
  ///////////////////////////////////////////
  // Base Record Algebra test
  const auto record_algebra_from_yaml = record{{
    "types",
    record{
      {
        "common",
        record{{
          "record",
          list{record{{"field", record{{"type", "bool"}}}}},
        }},
      },
      {
        "record_algebra_field",
        record{{
          "record",
          record{
            {"base", list{"common"}},
            {"fields", list{record{{"field", "string"}}}},
          },
        }},
      },
      {
        "record_algebra_field",
        record{{
          "record",
          record{
            {"extend", list{"common"}},
            {"fields", list{record{{"field", "string"}}}},
          },
        }},
      },
      {
        "record_algebra_field",
        record{{
          "record",
          record{
            {"implant", list{"common"}},
            {"fields", list{record{{"field", "string"}}}},
          },
        }},
      },
      {
        "record_algebra_field",
        record{{
          "record",
          record{
            {"base", list{"common"}},
            {"fields", list{record{{"msg", "string"}}}},
          },
        }},
      },
    },
  }};
}

TEST(YAML Module) {
  const auto declaration = record{{
    "types",
    record{
      {
        "count_field",
        record{{"type", "count"}},
      },
      {
        "string_field",
        record{{"type", "string"}},
      },
    },
  }};
  const auto result = unbox(to_module2(declaration));
  const auto expected_result
    = module_ng2{.types = {type{"count_field", count_type{}},
                           type{"string_field", string_type{}}}};
  CHECK_EQUAL(result, expected_result);
}

TEST(YAML Module - type alias) {
  const auto declaration = record{{
    "types",
    record{
      {
        "string_field",
        record{{"type", "string"},
               {"attributes", list{"ioc", record{{"index", "hash"}}}}},
      },
      {
        "string_field_alias",
        record{{"type", "string_field"}},
      },
    },
  }};
  const auto result = unbox(to_module2(declaration));
  const auto expected_result = module_ng2{.types = {
                                            type{
                                              "string_field",
                                              string_type{},
                                              {{"ioc"}, {"index", "hash"}},
                                            },
                                            type{
                                              "string_field_alias",
                                              type{
                                                "string_field",
                                                string_type{},
                                                {{"ioc"}, {"index", "hash"}},
                                              },
                                            },
                                          }};
  CHECK_EQUAL(result, expected_result);
}

TEST(YAML Module - yaml alias node) {
  const auto* const yaml = "types:\n"
                           "  type1:\n"
                           "    list: &record1\n"
                           "      record:\n"
                           "      - src: addr\n"
                           "      - dst: addr\n"
                           "\n"
                           "  type2:\n"
                           "    map:\n"
                           "      key: string\n"
                           "      value: *record1\n"
                           "\n"
                           "  type3:\n"
                           "    type: string\n"
                           "    attributes:\n"
                           "      - attr1_key:\n"
                           "      - attr2_key\n";
  const auto declaration = unbox(from_yaml(yaml));
  const auto result = unbox(to_module2(declaration));
  const auto expected = module_ng2{
    .types = {type{
                "type1",
                list_type{record_type{
                  {"src", address_type{}},
                  {"dst", address_type{}},
                }},
              },
              type{
                "type2",
                map_type{string_type{},
                         record_type{
                           {"src", address_type{}},
                           {"dst", address_type{}},
                         }},
              },
              type{"type3",
                   string_type{},
                   {{"attr1_key"}, {"attr2_key"}, {"null", "value1"}}}},
  };
  CHECK_EQUAL(result, expected);
}

TEST(YAML Module - order independent parsing - type aliases) {
  const auto declaration = record{{
    "types",
    record{
      {
        "type1",
        record{{"type", "type2"}},
      },
      {
        "type2",
        record{{"type", "string"}},
      },
    },
  }};
  const auto result = unbox(to_module2(declaration));
  const auto expected_result
    = module_ng2{.types = {type{"type2", string_type{}},
                           type{"type1", type{"type2", string_type{}}}}};
  CHECK_EQUAL(result, expected_result);
}

TEST(YAML Module - order independent parsing - type enumeration) {
  const auto declaration = record{{
    "types",
    record{
      {
        "enum_field",
        record{{"enum", list{"on", "off", "unknown"}}},
      },
    },
  }};
  const auto result = unbox(to_module2(declaration));
  const auto expected_result = module_ng2{
    .types = {
      type{"enum_field", enumeration_type{{"on"}, {"off"}, {"unknown"}}},
    }};
  CHECK_EQUAL(result, expected_result);
}

TEST(YAML Module - order independent parsing - list_type) {
  const auto declaration = record{{
    "types",
    record{
      {"type1", record{{"list", "type2"}}},
      {"type2", record{{"list", "type3"}}},
      {"type3", record{{"type", "string"}}},
    },
  }};
  const auto result = unbox(to_module2(declaration));
  const auto expected_result = module_ng2{
    .types
    = {type{"type3", string_type{}},
       type{"type2", list_type{type{"type3", string_type{}}}},
       type{"type1", list_type{
                       type{"type2", list_type{type{"type3", string_type{}}}},
                     }}}};
  CHECK_EQUAL(result, expected_result);
}

TEST(YAML Module - order indepenedent parsing - map_type) {
  const auto declaration = record{{
    "types",
    record{
      {"map_type",
       record{
         {"map", record{{"key", "type1"}, {"value", "type2"}}},
       }},
      {"type1", record{{"type", "count"}}},
      {"type2", record{{"type", "string"}}},
    },
  }};
  const auto result = unbox(to_module2(declaration));
  const auto expected_result
    = module_ng2{.types = {
                   type{"type1", count_type{}},
                   type{"type2", string_type{}},
                   type{"map_type", map_type{type{"type1", count_type{}},
                                             type{"type2", string_type{}}}},
                 }};
  CHECK_EQUAL(result, expected_result);
}

TEST(YAML Module - order indepenedent parsing - record_type) {
  const auto declaration = record{{
    "types",
    record{
      {
        "record_field",
        record{{"record",
                list{
                  record{
                    {"source",
                     record{
                       {"type", "type2"},
                     }},
                  },
                  record{
                    {"destination",
                     record{
                       {"type", "type3"},
                     }},
                  },
                }}},
      },
      {"type2", record{{"type", "string"}}},
      {"type3", record{{"type", "string"}}},
    },
  }};
  const auto result = unbox(to_module2(declaration));
  const auto expected_result = module_ng2{
    .types = {
      type{"type2", string_type{}},
      type{"type3", string_type{}},
      type{"record_field",
           record_type{{"source", type{"type2", string_type{}}},
                       {"destination", type{"type3", string_type{}}}}},
    }};
  CHECK_EQUAL(result, expected_result);
}

TEST(YAML Module - order independent parsing - record algebra) {
  // Creating a base record for later Record Algebra tests.
  const auto base_record_declaration = record{
    {"types",
     record{
       {"record_algebra_field",
        record{{"record",
                record{
                  {"base", list{"common"}},
                  {"fields", list{record{{"msg", "string"}}}},
                }}}},
       {"common",
        record{{"record", list{record{{"field", record{{"type", "bool"}}}}}}}},
     }}};
  const auto result = unbox(to_module2(base_record_declaration));
  const auto expected_result
    = module_ng2{.types = {
                   type{"common", record_type{{"field", bool_type{}}}},
                   type{"record_algebra_field",
                        record_type{
                          {"field", bool_type{}},
                          {"msg", string_type{}},
                        }},
                 }};
  CHECK_EQUAL(result, expected_result);
  /*
  // Base Record Algebra test
  auto record_algebra_from_yaml = record{{
    "types",
    record{
      {

    ,
  };
  auto record_algebra = unbox(to_module2(record_algebra_from_yaml));
  auto expected_record_algebra = type{
    "record_algebra_field",
    record_type{
      {"field", type{bool_type{}}},
      {"msg", type{string_type{}}},
    },
  };
  CHECK_EQUAL(record_algebra, expected_record_algebra);
  // Base Record Algebra test with name clash
  auto clashing_record_algebra_from_yaml = record{{
    "types",
    record{
      {

    "record_algebra_field",
    record{{
      "record",
      record{
        {"base", list{"common"}},
        {"fields", list{record{{"field", "string"}}}},
      },
    }},
  };
  auto clashing_record_algebra
    = to_module2(clashing_record_algebra_from_yaml);
  CHECK_ERROR(clashing_record_algebra);
  // Extend Record Algebra test with name clash
  auto clashing_extend_record_algebra_from_yaml = record{{
    "types",
    record{
      {

    "record_algebra_field",
    record{{
      "record",
      record{
        {"extend", list{"common"}},
        {"fields", list{record{{"field", "string"}}}},
      },
    }},
  };
  auto extended_record_algebra
    = to_module2(clashing_extend_record_algebra_from_yaml);
  auto expected_extended_record_algebra = type{
    "record_algebra_field",
    record_type{
      {"field", type{string_type{}}},
    },
  };
  CHECK_EQUAL(unbox(extended_record_algebra),
  expected_extended_record_algebra);
  // Implant Record Algebra test with name clash
  auto clashing_implant_record_algebra_from_yaml = record{{
    "types",
    record{
      {

    "record_algebra_field",
    record{{
      "record",
      record{
        {"implant", list{"common"}},
        {"fields", list{record{{"field", "string"}}}},
      },
    }},
  };
  auto implanted_record_algebra
    = to_module2(clashing_implant_record_algebra_from_yaml);
  auto expected_implanted_record_algebra = type{
    "record_algebra_field",
    record_type{
      {"field", type{bool_type{}}},
    },
  };
  CHECK_EQUAL(unbox(implanted_record_algebra),
              expected_implanted_record_algebra);
  */
}

// FIXME:: Write checks with attributes!
// FIXME:: Test case: Map both key and value depends on the same type!

/*
// FIXME:
TEST(metadata layer merging) {
  const auto t1 = type{
    "foo",
    bool_type{},
    {{"one", "eins"}, {"two", "zwei"}},
  };
  MESSAGE("attributes do get merged in unnamed metadata layers");
  const auto t2 = type{
    "foo",
    type{
      bool_type{},
      {{"two", "zwei"}},
    },
    {{"one", "eins"}},
  };
  CHECK_EQUAL(t1, t2);
  MESSAGE("attributes do not get merged in named metadata layers");
  const auto t3 = type{
    type{
      "foo",
      bool_type{},
      {{"two", "zwei"}},
    },
    {{"one", "eins"}},
  };
  CHECK_NOT_EQUAL(t1, t3);
  MESSAGE("attribute merging prefers new attributes");
  const auto t4 = type{
    "foo",
    type{
      bool_type{},
      {{"one"}, {"two", "zwei"}},
    },
    {{"one", "eins"}},
  };
  CHECK_EQUAL(t1, t4);
  }*/
