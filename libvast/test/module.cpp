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

/// Converts a declaration into a vast type.
/// @param known_types types already converted
/// @param declaration the type declaration parsed from yaml module config file
/// @param name the name for the declaration
caf::expected<type>
to_type(const std::vector<type>& known_types, const data& declaration,
        std::string_view name = "");

caf::expected<record_type> to_record(const std::vector<type>& known_types,
                                     const list& field_declarations) {
  if (field_declarations.empty())
    return caf::make_error(ec::parse_error, "record types must have at least "
                                            "one field");
  auto record_fields = std::vector<record_type::field_view>{};
  record_fields.reserve(field_declarations.size());
  for (const auto& record_value : field_declarations) {
    const auto* record_record_ptr = caf::get_if<record>(&record_value);
    if (record_record_ptr == nullptr)
      return caf::make_error(ec::parse_error, "a field in record type must be "
                                              "specified as a YAML dictionary");
    const auto& record_record = *record_record_ptr;
    if (record_record.size() != 1)
      return caf::make_error(ec::parse_error, "a field in a record type can "
                                              "have only a single key in the "
                                              "YAML dictionary");
    auto type_or_error = to_type(known_types, record_record.begin()->second);
    if (!type_or_error)
      return caf::make_error(
        ec::parse_error, fmt::format("failed to parse record type field: {}",
                                     type_or_error.error()));
    record_fields.emplace_back(record_record.begin()->first, *type_or_error);
  }
  return record_type{record_fields};
}

type get_known_type(const std::vector<type>& known_types,
                    std::string_view name) {
  for (const auto& known_type : known_types) {
    if (name == known_type.name()) {
      return known_type;
    }
  }
  return {}; // none type
}

constexpr auto reserved_names
  = std::array{"bool", "integer", "count",   "real",    "duration",
               "time", "string",  "pattern", "address", "subnet",
               "enum", "list",    "map",     "record"};

caf::expected<type> to_enum(std::string_view name, const data& enumeration,
                            std::vector<type::attribute_view>&& attributes) {
  const auto* enum_list_ptr = caf::get_if<list>(&enumeration);
  if (enum_list_ptr == nullptr)
    return caf::make_error(ec::parse_error, "enum must be specified as a "
                                            "YAML list");
  const auto& enum_list = *enum_list_ptr;
  if (enum_list.empty())
    return caf::make_error(ec::parse_error, "enum cannot be empty");
  auto enum_fields = std::vector<enumeration_type::field_view>{};
  enum_fields.reserve(enum_list.size());
  for (const auto& enum_value : enum_list) {
    const auto* enum_string_ptr = caf::get_if<std::string>(&enum_value);
    if (enum_string_ptr == nullptr)
      return caf::make_error(ec::parse_error, "enum value must be specified "
                                              "as a YAML string");
    enum_fields.push_back({*enum_string_ptr});
  }
  return type{name, enumeration_type{enum_fields}, std::move(attributes)};
}

caf::expected<type> to_map(std::string_view name, const data& map_to_parse,
                           std::vector<type::attribute_view>&& attributes,
                           const std::vector<type>& known_types) {
  const auto* map_record_ptr = caf::get_if<record>(&map_to_parse);
  if (map_record_ptr == nullptr)
    return caf::make_error(ec::parse_error, "a map type must be specified as "
                                            "a YAML dictionary");
  const auto& map_record = *map_record_ptr;
  auto found_key = map_record.find("key");
  auto found_value = map_record.find("value");
  if (found_key == map_record.end() || found_value == map_record.end())
    return caf::make_error(ec::parse_error, "a map type must have both a key "
                                            "and a value");
  auto key_type_expected = to_type(known_types, found_key->second);
  if (!key_type_expected)
    return caf::make_error(ec::parse_error,
                           fmt::format("failed to parse map key: {}",
                                       key_type_expected.error()));
  auto value_type_expected = to_type(known_types, found_value->second);
  if (!value_type_expected)
    return caf::make_error(ec::parse_error,
                           fmt::format("failed to parse map value: {}",
                                       value_type_expected.error()));
  return type{name, map_type{*key_type_expected, *value_type_expected},
              std::move(attributes)};
}

caf::expected<type>
to_record_algebra(std::string_view name, const data& record_algebra,
                  std::vector<type::attribute_view>&& attributes,
                  const std::vector<type>& known_types) {
  const auto* record_algebra_record_ptr = caf::get_if<record>(&record_algebra);
  if (record_algebra_record_ptr == nullptr)
    return caf::make_error(ec::parse_error, "record algebra must be "
                                            "specified as a YAML dictionary");
  const auto& record_algebra_record = *record_algebra_record_ptr;
  auto found_base = record_algebra_record.find("base");
  auto found_implant = record_algebra_record.find("implant");
  auto found_extend = record_algebra_record.find("extend");
  auto is_base_found = found_base != record_algebra_record.end();
  auto is_implant_found = found_implant != record_algebra_record.end();
  auto is_extend_found = found_extend != record_algebra_record.end();
  int name_clash_specifier_cnt = 0;
  record_type::merge_conflict merge_conflict_handling
    = record_type::merge_conflict::fail;
  if (is_base_found)
    name_clash_specifier_cnt++;
  if (is_implant_found) {
    name_clash_specifier_cnt++;
    // right is the new record type
    merge_conflict_handling = record_type::merge_conflict::prefer_left;
  }
  if (is_extend_found) {
    name_clash_specifier_cnt++;
    merge_conflict_handling = record_type::merge_conflict::prefer_right;
  }
  if (name_clash_specifier_cnt >= 2)
    return caf::make_error(ec::parse_error, "record algebra must contain "
                                            "only one of 'base', 'implant', "
                                            "'extend'");
  // create new record type
  auto found_fields = record_algebra_record.find("fields");
  if (found_fields == record_algebra_record.end())
    return caf::make_error(ec::parse_error, "record algebra must have one "
                                            "'fields'");
  const auto* fields_list_ptr = caf::get_if<list>(&found_fields->second);
  if (fields_list_ptr == nullptr)
    return caf::make_error(ec::parse_error, "'fields' in record algebra must "
                                            "be specified as YAML list");
  auto new_record_or_error = to_record(known_types, *fields_list_ptr);
  if (!new_record_or_error)
    return caf::make_error(ec::parse_error,
                           fmt::format("failed to parse record algebra "
                                       "fields: {}",
                                       new_record_or_error.error()));
  auto new_record = new_record_or_error.value();
  // retrive records (base, implant or extend)
  if (name_clash_specifier_cnt == 0)
    return type{name, new_record, std::move(attributes)};
  const auto& records = is_base_found      ? found_base->second
                        : is_implant_found ? found_implant->second
                                           : found_extend->second;
  const auto* records_list_ptr = caf::get_if<list>(&records);
  if (records_list_ptr == nullptr)
    return caf::make_error(ec::parse_error, "'base', 'implant' or 'extend' "
                                            "in a record algebra must be "
                                            "specified as a YAML list");
  const auto& record_list = *records_list_ptr;
  if (record_list.empty())
    return caf::make_error(ec::parse_error, "a record algebra cannot have an "
                                            "empty 'base', 'implant' or "
                                            "'extend'");
  std::optional<record_type> merged_base_record{};
  for (const auto& record : record_list) {
    const auto* record_name_ptr = caf::get_if<std::string>(&record);
    if (record_name_ptr == nullptr)
      return caf::make_error(
        ec::parse_error, "the 'base', 'implant' or 'extend' keywords of a "
                         "record algebra must be specified as a YAML string");
    const auto& record_name = *record_name_ptr;
    const auto& base_type = get_known_type( // base or implant or extend
      known_types, record_name);
    if (!base_type)
      return caf::make_error(ec::parse_error,
                             "parses unknown record type when parsing a "
                             "record algebra base, implant or extend");
    for (const auto& attribute : base_type.attributes())
      attributes.push_back(attribute);
    const auto* base_record_ptr = caf::get_if<record_type>(&base_type);
    if (base_record_ptr == nullptr)
      return caf::make_error(ec::parse_error, "'base', 'implant' or 'extend' "
                                              "of a record algebra must be "
                                              "specified as YAML dictionary");
    if (!merged_base_record) {
      merged_base_record = *base_record_ptr;
      continue;
    }
    const auto new_merged_base_record = merge(
      *merged_base_record, *base_record_ptr, record_type::merge_conflict::fail);
    if (!new_merged_base_record)
      return caf::make_error(ec::parse_error,
                             "types in 'base', 'implant' or 'extend' conflicts "
                             "with a type in record algebra 'fields'");
    merged_base_record = *new_merged_base_record;
  }
  auto final_merged_record
    = merge(*merged_base_record, new_record, merge_conflict_handling);
  if (!final_merged_record)
    return caf::make_error(ec::parse_error,
                           fmt::format("failed to merge record algebra: {}",
                                       final_merged_record.error()));
  return type{name, *final_merged_record, std::move(attributes)};
}

caf::expected<type> to_type(const std::vector<type>& known_types,
                            const data& declaration, std::string_view name) {
  const auto* known_type_name_ptr = caf::get_if<std::string>(&declaration);
  // Prevent using reserved names as types names
  if (std::any_of(reserved_names.begin(), reserved_names.end(),
                  [&](const auto& reserved_name) {
                    return name == reserved_name;
                  }))
    return caf::make_error(
      ec::parse_error,
      fmt::format("type declaration cannot use a reserved name: {}", name));
  // Type names can contain any character that the YAML parser can handle - no
  // need to check for allowed characters.
  if (known_type_name_ptr != nullptr) {
    const auto& known_type_name = *known_type_name_ptr;
    // Check built-in types first
    if (known_type_name == "bool")
      return type{name, bool_type{}};
    if (known_type_name == "integer")
      return type{name, integer_type{}};
    if (known_type_name == "count")
      return type{name, count_type{}};
    if (known_type_name == "real")
      return type{name, real_type{}};
    if (known_type_name == "duration")
      return type{name, duration_type{}};
    if (known_type_name == "time")
      return type{name, time_type{}};
    if (known_type_name == "string")
      return type{name, string_type{}};
    if (known_type_name == "pattern")
      return type{name, pattern_type{}};
    if (known_type_name == "address")
      return type{name, address_type{}};
    if (known_type_name == "subnet")
      return type{name, subnet_type{}};
    // Check type aliases aka. known types
    const auto& known_type = get_known_type(known_types, known_type_name);
    if (!known_type)
      return caf::make_error(ec::parse_error, fmt::format("found unknown type: "
                                                          "{}",
                                                          known_type_name));
    return type{name, known_type};
  }
  const auto* declaration_record_ptr = caf::get_if<record>(&declaration);
  if (declaration_record_ptr == nullptr)
    return caf::make_error(ec::parse_error, "type alias must be specified as a "
                                            "YAML dictionary");
  const auto& declaration_record = *declaration_record_ptr;
  // Get the optional attributes
  auto attributes = std::vector<type::attribute_view>{};
  auto found_attributes = declaration_record.find("attributes");
  if (found_attributes != declaration_record.end()) {
    const auto* attribute_list = caf::get_if<list>(&found_attributes->second);
    if (attribute_list == nullptr)
      return caf::make_error(ec::parse_error, "the attribute list must be "
                                              "specified as a YAML list");
    for (const auto& attribute : *attribute_list) {
      const auto* attribute_string_ptr = caf::get_if<std::string>(&attribute);
      if (attribute_string_ptr != nullptr)
        attributes.push_back({*attribute_string_ptr});
      else {
        const auto* attribute_record_ptr = caf::get_if<record>(&attribute);
        if (attribute_record_ptr == nullptr)
          return caf::make_error(ec::parse_error, "attribute must be specified "
                                                  "as a YAML dictionary");
        const auto& attribute_record = *attribute_record_ptr;
        if (attribute_record.size() != 1)
          return caf::make_error(ec::parse_error, "attribute must have a "
                                                  "single field");
        const auto& attribute_key = attribute_record.begin()->first;
        const auto* attribute_value_ptr
          = caf::get_if<std::string>(&attribute_record.begin()->second);
        if (attribute_value_ptr == nullptr)
          return caf::make_error(ec::parse_error, "attribute must be a string");
        const auto& attribute_value = *attribute_value_ptr;
        attributes.push_back({attribute_key, attribute_value});
      }
    }
  }
  // Check that only one of type, enum, list, map and record is specified
  // by the user
  auto found_type = declaration_record.find("type");
  auto found_enum = declaration_record.find("enum");
  auto found_list = declaration_record.find("list");
  auto found_map = declaration_record.find("map");
  auto found_record = declaration_record.find("record");
  auto is_type_found = found_type != declaration_record.end();
  auto is_enum_found = found_enum != declaration_record.end();
  auto is_list_found = found_list != declaration_record.end();
  auto is_map_found = found_map != declaration_record.end();
  auto is_record_found = found_record != declaration_record.end();
  int type_selector_cnt
    = static_cast<int>(is_type_found) + static_cast<int>(is_enum_found)
      + static_cast<int>(is_list_found) + static_cast<int>(is_map_found)
      + static_cast<int>(is_record_found);
  if (type_selector_cnt != 1)
    return caf::make_error(ec::parse_error, "one of type, enum, list, map, "
                                            "record is expected");
  // Type alias
  if (is_type_found) {
    auto type_expected = to_type(known_types, found_type->second);
    if (!type_expected)
      return caf::make_error(ec::parse_error,
                             fmt::format("failed to parse type alias: {}",
                                         type_expected.error()));
    return type{name, *type_expected, std::move(attributes)};
  }
  // Enumeration
  if (is_enum_found)
    return to_enum(name, found_enum->second, std::move(attributes));
  // List
  if (is_list_found) {
    auto type_expected = to_type(known_types, found_list->second);
    if (!type_expected)
      return caf::make_error(ec::parse_error,
                             fmt::format("failed to parse list: {}",
                                         type_expected.error()));
    return type{name, list_type{*type_expected}, std::move(attributes)};
  }
  // Map
  if (is_map_found)
    return to_map(name, found_map->second, std::move(attributes), known_types);
  // Record or Record algebra
  if (is_record_found) {
    const auto* record_list_ptr = caf::get_if<list>(&found_record->second);
    if (record_list_ptr != nullptr) {
      // Record
      const auto& new_record = to_record(known_types, *record_list_ptr);
      if (!new_record)
        return caf::make_error(ec::parse_error,
                               fmt::format("failed to parse record: {}",
                                           new_record.error()));
      return type{name, *new_record, std::move(attributes)};
    }
    // Record algebra
    return to_record_algebra(name, found_record->second, std::move(attributes),
                             known_types);
  }
  return caf::make_error(ec::parse_error, "unknown type");
}

caf::expected<type> to_type(const std::vector<type>& known_types,
                            const record::value_type& variable_declaration) {
  return to_type(known_types, variable_declaration.second,
                 variable_declaration.first);
}

struct module_ng2 {
  std::vector<type> types;
  // FIXME: The order of types should not matter?
  friend bool operator==(const module_ng2& first,
                         const module_ng2& second) noexcept = default;
};

caf::expected<module_ng2> to_module(const data& declaration) {
  const auto* decl_ptr = caf::get_if<record>(&declaration);
  if (decl_ptr == nullptr)
    return caf::make_error(ec::parse_error, "parses a module with an invalid "
                                            "format");
  const auto& decl = *decl_ptr;
  // types
  auto found_types = decl.find("types");
  if (found_types == decl.end())
    return caf::make_error(ec::parse_error, "parses a module with no types");
  const auto* types_ptr = caf::get_if<record>(&found_types->second);
  if (types_ptr == nullptr)
    return caf::make_error(ec::parse_error, "parses a module with invalid "
                                            "types");
  const auto& types = *types_ptr;
  if (types.empty())
    return caf::make_error(ec::parse_error, "parses a module with empty "
                                            "types");
  std::vector<type> known_types;
  for (const auto& current_type : types) {
    const auto& parsed_type = to_type(known_types, current_type);
    if (!parsed_type)
      return parsed_type.error();
    known_types.push_back(*parsed_type);
  }
  return module_ng2{.types = known_types};
}

//////////////////////////////////////////////////////////////////

struct parsed_type {
  type parsed;                               // Potentially unresolved type
  std::vector<std::string_view> providers{}; // The types this type depends on
  bool is_algebra{}; // The record algebra must be executed to resolve the result
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

/// Returns a built-in type or a partial placeholder type. The name and
/// attributes must be added by the caller. It does not handle inline
/// declarations.
caf::expected<parsed_type> to_builtin(const data& declaration) {
  const auto* aliased_type_name_ptr = caf::get_if<std::string>(&declaration);
  if (aliased_type_name_ptr == nullptr)
    return caf::make_error(ec::parse_error, "built-in type can only be a "
                                            "string");
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
  if (aliased_type_name == "address")
    return parsed_type{type{address_type{}}};
  if (aliased_type_name == "subnet")
    return parsed_type{type{subnet_type{}}};
  VAST_TRACE(fmt::format("Creating placeholder type for aliased_type: {}",
                         aliased_type_name));
  // Returning a partial placeholder
  return parsed_type{type{aliased_type_name, type{}}, aliased_type_name};
}

caf::expected<parsed_type>
to_type2(const data& declaration, std::string_view name);

caf::expected<parsed_type>
to_record2(const std::vector<vast::data>& record_list) {
  if (record_list.empty())
    return caf::make_error(ec::parse_error, "parses an empty record");
  auto record_fields = std::vector<record_type::field_view>{};
  auto providers = std::vector<std::string_view>{};
  for (const auto& record_value : record_list) {
    const auto* record_record_ptr = caf::get_if<record>(&record_value);
    if (record_record_ptr == nullptr)
      return caf::make_error(ec::parse_error, "parses an record with invalid "
                                              "format");
    const auto& record_record = *record_record_ptr;
    if (record_record.size() != 1)
      return caf::make_error(ec::parse_error, "parses an record field with an "
                                              "invalid format");
    auto type_or_error
      = to_type2(record_record.begin()->second, std::string_view{});
    if (!type_or_error)
      return type_or_error.error();
    providers.insert(providers.end(),
                     std::make_move_iterator(type_or_error->providers.begin()),
                     std::make_move_iterator(type_or_error->providers.end()));
    record_fields.emplace_back(record_record.begin()->first,
                               type_or_error->parsed);
  }
  return parsed_type{type{record_type{record_fields}}, std::move(providers)};
}

/// Can handle inline declartaions
/// FIXME: name optional?
caf::expected<parsed_type>
to_type2(const data& declaration, std::string_view name) {
  // Prevent using reserved names as types names
  if (std::any_of(reserved_names.begin(), reserved_names.end(),
                  [&](const auto& reserved_name) {
                    return name == reserved_name;
                  }))
    return caf::make_error(
      ec::parse_error,
      fmt::format("parses a new type with a reserved name: {}", name));
  const auto* aliased_type_name_ptr = caf::get_if<std::string>(&declaration);
  // Type names can contain any character that the YAML parser can handle - no
  // need to check for allowed characters.
  if (aliased_type_name_ptr != nullptr)
    return to_builtin(declaration);
  const auto* decl_ptr = caf::get_if<record>(&declaration);
  if (decl_ptr == nullptr)
    return caf::make_error(ec::parse_error, "parses type alias with invalid "
                                            "format");
  const auto& decl = *decl_ptr;
  // Get the optional attributes
  auto attributes = std::vector<type::attribute_view>{};
  auto found_attributes = decl.find("attributes");
  if (found_attributes != decl.end()) {
    const auto* attribute_list = caf::get_if<list>(&found_attributes->second);
    if (attribute_list == nullptr)
      return caf::make_error(ec::parse_error, "parses an attribute list with "
                                              "an invalid format");
    for (const auto& attribute : *attribute_list) {
      const auto* attribute_string_ptr = caf::get_if<std::string>(&attribute);
      if (attribute_string_ptr != nullptr)
        attributes.push_back({*attribute_string_ptr});
      else {
        const auto* attribute_record_ptr = caf::get_if<record>(&attribute);
        if (attribute_record_ptr == nullptr)
          return caf::make_error(ec::parse_error, "parses an attribute with an "
                                                  "invalid format");
        const auto& attribute_record = *attribute_record_ptr;
        if (attribute_record.size() != 1)
          return caf::make_error(ec::parse_error, "parses an attribute not "
                                                  "having one and only one "
                                                  "field");
        const auto& attribute_key = attribute_record.begin()->first;
        const auto* attribute_value_ptr
          = caf::get_if<std::string>(&attribute_record.begin()->second);
        if (attribute_value_ptr == nullptr)
          return caf::make_error(ec::parse_error, "parses an attribute with a "
                                                  "non-string value");
        const auto& attribute_value = *attribute_value_ptr;
        attributes.push_back({attribute_key, attribute_value});
      }
    }
  }
  auto found_type = decl.find("type");
  auto found_list = decl.find("list");
  auto found_map = decl.find("map");
  auto found_record = decl.find("record");
  auto is_type_found = found_type != decl.end();
  auto is_list_found = found_list != decl.end();
  auto is_map_found = found_map != decl.end();
  auto is_record_found = found_record != decl.end();
  int type_selector_cnt = 0;
  if (is_type_found)
    type_selector_cnt++;
  if (is_list_found)
    type_selector_cnt++;
  if (is_map_found)
    type_selector_cnt++;
  if (is_record_found)
    type_selector_cnt++;
  if (type_selector_cnt != 1)
    return caf::make_error(ec::parse_error, "expects one of type, enum, list, "
                                            "map, record");
  // Type alias
  if (is_type_found) {
    // It can only be a built int type
    auto type_expected = to_builtin(found_type->second);
    if (!type_expected)
      return type_expected.error();
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
  // List
  if (is_list_found) {
    auto type_expected = to_type2(found_list->second, std::string_view{});
    if (!type_expected)
      return type_expected.error();
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
  if (is_map_found) {
    const auto* map_record_ptr = caf::get_if<record>(&found_map->second);
    if (map_record_ptr == nullptr)
      return caf::make_error(ec::parse_error, "parses a map with an invalid "
                                              "format");
    const auto& map_record = *map_record_ptr;
    auto found_key = map_record.find("key");
    auto found_value = map_record.find("value");
    if (found_key == map_record.end() || found_value == map_record.end())
      return caf::make_error(ec::parse_error, "parses a map without a key or a "
                                              "value");
    auto key_type_expected = to_type2(found_key->second, std::string_view{});
    if (!key_type_expected)
      return key_type_expected.error();
    auto value_type_expected
      = to_type2(found_value->second, std::string_view{});
    if (!value_type_expected)
      return value_type_expected.error();
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
      type{name,
           map_type{key_type_expected->parsed, value_type_expected->parsed},
           std::move(attributes)},
      std::move(providers)};
  }
  // Record or Record algebra
  if (is_record_found) {
    const auto* record_list_ptr = caf::get_if<list>(&found_record->second);
    if (record_list_ptr != nullptr) {
      // Record
      auto new_record = to_record2(*record_list_ptr);
      if (!new_record)
        return new_record.error();
      return parsed_type(type{name, new_record->parsed, std::move(attributes)},
                         std::move(new_record->providers));
    }
    // TODO Record algebra
  }
  // FIXME: record and other types from previous version
  return caf::make_error(ec::parse_error, "unimplemented");
}

caf::expected<parsed_type>
to_type2(const record::value_type& variable_declaration) {
  return to_type2(variable_declaration.second, variable_declaration.first);
}

caf::expected<type>
try_resolve(const type& to_resolve, const std::vector<type>& resolved_types);

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
    auto type_found
      = std::find_if(resolved_types.begin(), resolved_types.end(),
                     [&](const auto& resolved_type) {
                       return placeholder->aliased_name == resolved_type.name();
                     });
    if (type_found == resolved_types.end())
      return caf::make_error(ec::parse_error, ""); // FIXME:
    return *type_found;
  }
  VAST_TRACE("Resolving inline type while resolving list_type");
  return try_resolve(unresolved_type, resolved_types);
}

/// Returns the list type. The name and attribute must be added by the caller.
caf::expected<type> resolve_list(const list_type& unresolved_list_type,
                                 const std::vector<type>& resolved_types) {
  VAST_TRACE("Resolving list_type");
  auto resolved_type = resolve_placeholder_or_inline(
    unresolved_list_type.value_type(), resolved_types);
  if (!resolved_type)
    return caf::make_error(ec::parse_error,
                           "Failed to resolve list type: "); // FIXME:
  return type{list_type{*resolved_type}};
}

caf::expected<type> resolve_map(const map_type& unresolved_map_type,
                                const std::vector<type>& resolved_types) {
  VAST_TRACE("Resolving map_type");
  auto resolved_key_type = resolve_placeholder_or_inline(
    unresolved_map_type.key_type(), resolved_types);
  if (!resolved_key_type)
    return caf::make_error(ec::parse_error,
                           "Failed to resolve map key type: "); // FIXME:
  auto resolved_value_type = resolve_placeholder_or_inline(
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
      return caf::make_error(
        ec::parse_error,
        "Failed to resolve record field key type: "); // FIXME:
    record_fields.emplace_back(field.name, *resolved_type);
  }
  return type{record_type{record_fields}};
}

caf::expected<type>
try_resolve(const type& to_resolve, const std::vector<type>& resolved_types) {
  if (auto placeholder = try_read_placeholder(to_resolve); placeholder) {
    VAST_TRACE("Resolving placeholder");
    auto type_found
      = std::find_if(resolved_types.begin(), resolved_types.end(),
                     [&](const auto& resolved_type) {
                       return placeholder->aliased_name == resolved_type.name();
                     });
    // FIXME: no duplicates should be allowed into resolved_types, check
    // somewhere!
    if (type_found == resolved_types.end())
      return caf::make_error(ec::logic_error, "placeholder type is not "
                                              "resolved yet");
    // FIXME: TEST if the placeholder type is not a Type Alias!
    return type{placeholder->name, *type_found};
  }
  auto collect_unresolved = detail::overload{
    [&](const list_type& unresolved_list_type) {
      return resolve_list(unresolved_list_type, resolved_types);
    },
    [&](const map_type& unresolved_map_type) {
      return resolve_map(unresolved_map_type, resolved_types);
    },
    [&](const record_type& unresolved_record_type) {
      return resolve_record(unresolved_record_type, resolved_types);
    },
    [&](const concrete_type auto&) {
      caf::expected<type> result = caf::make_error(
        ec::logic_error,
        fmt::format("Unhandled type in try_resolve: {}", to_resolve));
      return result;
    },
  };
  VAST_TRACE("Resolving complex type: {}", to_resolve);
  auto resolution_result = caf::visit(collect_unresolved, to_resolve);
  if (resolution_result)
    return type{to_resolve.name(), *resolution_result};
  return caf::make_error(ec::logic_error, "Unexpected resolution failure");
}

class resolution_manager {
public:
  caf::expected<type>
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
        return caf::make_error(ec::logic_error, "unresolved type expected");
      if (!type_to_resolve->providers.empty()) {
        resolving_types_.insert(resolving_types_.end(),
                                type_to_resolve->providers.begin(),
                                type_to_resolve->providers.end());
      }
      if (type_name_to_resolve == resolving_types_.back())
        return type_to_resolve->parsed;
    };
  }
  void resolved() {
    resolving_types_.pop_back();
  }

private:
  std::vector<std::string_view> resolving_types_{};
};

caf::expected<module_ng2> to_module2(const data& declaration) {
  const auto* decl_ptr = caf::get_if<record>(&declaration);
  if (decl_ptr == nullptr)
    return caf::make_error(ec::parse_error, "parses a module with an invalid "
                                            "format");
  const auto& decl = *decl_ptr;
  // types
  auto found_types = decl.find("types");
  if (found_types == decl.end())
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
    const auto parsed_type = to_type2(current_type);
    if (!parsed_type)
      return parsed_type.error();
    parsed_types.push_back(*parsed_type);
  }
  std::vector<type> resolved_types;
  // Move parsed items that are already resolved to resolved types.
  auto removed_items = std::remove_if(parsed_types.begin(), parsed_types.end(),
                                      [](const auto& current_type) {
                                        return current_type.providers.empty();
                                      });
  for (auto i = removed_items; i < parsed_types.end(); i++) {
    VAST_TRACE("Resolved: {}", i->parsed.name());
    resolved_types.push_back(i->parsed);
  }
  parsed_types.erase(removed_items, parsed_types.end());
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
  // FIXME: Make the changed meaning of parsed_type clearer: rename or
  // extract! Attempt to resolve type
  VAST_TRACE("Before resolving {}", parsed_types.empty());
  resolution_manager manager{};
  while (!parsed_types.empty()) {
    // FIXME: In case of an invalid schema parsed_types may never get empty!
    auto type_to_resolve = manager.next_to_resolve(parsed_types);
    if (!type_to_resolve)
      return caf::make_error(ec::parse_error, "Failed to determine next type "
                                              "to resolve");
    VAST_TRACE("Next to resolve: {}", type_to_resolve->name());
    auto resolve_result = try_resolve(*type_to_resolve, resolved_types);
    if (!resolve_result)
      return caf::make_error(
        ec::parse_error, fmt::format("Failed to resolve: {}", type_to_resolve));
    auto resolved_type = *resolve_result;
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

TEST(YAML Type - parsing string with attributs and parsing a known type) {
  std::vector<type> known_types;
  auto string_type_with_attrs = record::value_type{
    "string_field",
    record{{"type", "string"},
           {"attributes", list{"ioc", record{{"index", "hash"}}}}},
  };
  auto result = unbox(to_type(known_types, string_type_with_attrs));
  known_types.emplace_back(result);
  auto expected_type = type{
    "string_field",
    string_type{},
    {{"ioc"}, {"index", "hash"}},
  };
  CHECK_EQUAL(result, expected_type);
  // Parsing a known_type
  auto string_field_type = record::value_type{
    "string_field_alias",
    record{{"type", "string_field"}},
  };
  result = unbox(to_type(known_types, string_field_type));
  expected_type = type{"string_field_alias", type{expected_type}};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - parsing bool type) {
  std::vector<type> known_types;
  auto bool_type_wo_attrs = record::value_type{
    "bool_field",
    record{{"type", "bool"}},
  };
  auto result = unbox(to_type(known_types, bool_type_wo_attrs));
  auto expected_type = type{"bool_field", bool_type{}};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing integer type) {
  std::vector<type> known_types;
  auto integer_type_wo_attrs = record::value_type{
    "int_field",
    record{{"type", "integer"}},
  };
  auto result = unbox(to_type(known_types, integer_type_wo_attrs));
  auto expected_type = type{"int_field", integer_type{}};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing count_type) {
  std::vector<type> known_types;
  auto count_type_wo_attrs = record::value_type{
    "count_field",
    record{{"type", "count"}},
  };
  auto result = unbox(to_type(known_types, count_type_wo_attrs));
  auto expected_type = type{"count_field", count_type{}};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing real_type) {
  std::vector<type> known_types;
  auto real_type_wo_attrs = record::value_type{
    "real_field",
    record{{"type", "real"}},
  };
  auto result = unbox(to_type(known_types, real_type_wo_attrs));
  auto expected_type = type{"real_field", real_type{}};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing duration_type) {
  std::vector<type> known_types;
  auto duration_type_wo_attrs = record::value_type{
    "duration_field",
    record{{"type", "duration"}},
  };
  auto result = unbox(to_type(known_types, duration_type_wo_attrs));
  auto expected_type = type{"duration_field", duration_type{}};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing time_type) {
  std::vector<type> known_types;
  auto time_type_wo_attrs = record::value_type{
    "time_field",
    record{{"type", "time"}},
  };
  auto result = unbox(to_type(known_types, time_type_wo_attrs));
  auto expected_type = type{"time_field", time_type{}};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing string_type without attributes) {
  std::vector<type> known_types;
  auto string_type_wo_attrs = record::value_type{
    "string_field",
    record{{"type", "string"}},
  };
  auto result = unbox(to_type(known_types, string_type_wo_attrs));
  auto expected_type = type{"string_field", string_type{}};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing pattern_type) {
  std::vector<type> known_types;
  auto pattern_type_wo_attrs = record::value_type{
    "pattern_field",
    record{{"type", "pattern"}},
  };
  auto result = unbox(to_type(known_types, pattern_type_wo_attrs));
  auto expected_type = type{"pattern_field", pattern_type{}};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing address_type) {
  std::vector<type> known_types;
  auto address_type_wo_attrs = record::value_type{
    "address_field",
    record{{"type", "address"}},
  };
  auto result = unbox(to_type(known_types, address_type_wo_attrs));
  auto expected_type = type{"address_field", address_type{}};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing subnet_type) {
  std::vector<type> known_types;
  auto subnet_type_wo_attrs = record::value_type{
    "subnet_field",
    record{{"type", "subnet"}},
  };
  auto result = unbox(to_type(known_types, subnet_type_wo_attrs));
  auto expected_type = type{"subnet_field", subnet_type{}};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing enumeration_type) {
  std::vector<type> known_types;
  auto enum_type_wo_attrs = record::value_type{
    "enum_field",
    record{{"enum", list{"on", "off", "unknown"}}},
  };
  auto result = unbox(to_type(known_types, enum_type_wo_attrs));
  auto expected_type
    = type{"enum_field", enumeration_type{{"on"}, {"off"}, {"unknown"}}};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing list_type) {
  std::vector<type> known_types;
  auto list_type_wo_attrs = record::value_type{
    "list_field",
    record{{"list", "count"}},
  };
  auto result = unbox(to_type(known_types, list_type_wo_attrs));
  auto expected_type = type{"list_field", list_type{count_type{}}};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing map_type) {
  std::vector<type> known_types;
  auto map_type_wo_attrs = record::value_type{
    "map_field",
    record{
      {"map", record{{"key", "count"}, {"value", "string"}}},
    },
  };
  auto result = unbox(to_type(known_types, map_type_wo_attrs));
  auto expected_type = type{
    "map_field",
    map_type{count_type{}, string_type{}},
  };
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing record_type) {
  std::vector<type> known_types;
  auto record_type_wo_attrs = record::value_type{
    "record_field",
    record{{
      "record",
      list{
        record{{"src_ip", "string"}},
        record{{"dst_ip", "string"}},
      },
    }},
  };
  auto result = unbox(to_type(known_types, record_type_wo_attrs));
  auto expected_type = type{
    "record_field",
    record_type{
      {"src_ip", string_type{}},
      {"dst_ip", string_type{}},
    },
  };
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing inline record_type) {
  std::vector<type> known_types;
  auto inline_record_type = record::value_type{
    "record_field",
    record{{"record",
            list{
              record{{"source", record{{"type", "string"}}}},
              record{{"destination", record{{"type", "string"}}}},
            }}},
  };
  auto result = unbox(to_type(known_types, inline_record_type));
  auto expected_type = type{
    "record_field",
    record_type{
      {"source", string_type{}},
      {"destination", string_type{}},
    },
  };
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing inline record_type with attributes) {
  std::vector<type> known_types;
  auto inline_record_type_with_attr = record::value_type{
    "record_field",
    record{{
      "record",
      list{record{
             {"source",
              record{
                {"type", "string"},
                {"attributes", list{"originator"}},
              }},
           },
           record{
             {"destination",
              record{
                {"type", "string"},
                {"attributes", list{"responder"}},
              }},
           }},
    }},
  };
  auto result = unbox(to_type(known_types, inline_record_type_with_attr));
  auto expected_type = type{
    "record_field",
    record_type{
      {"source", type{string_type{}, {{"originator"}}}},
      {"destination", type{string_type{}, {{"responder"}}}},
    },
  };
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing record algebra) {
  std::vector<type> known_types;
  // Creating a base record for later Record Algebra tests.
  auto base_record_type_from_yaml = record::value_type{
    "common",
    record{{"record", list{record{{"field", record{{"type", "bool"}}}}}}}};
  auto base_record_type
    = unbox(to_type(known_types, base_record_type_from_yaml));
  auto expected_base_record_type
    = type{"common", record_type{{"field", bool_type{}}}};
  CHECK_EQUAL(base_record_type, expected_base_record_type);
  known_types.push_back(base_record_type);
  // Base Record Algebra test
  auto record_algebra_from_yaml = record::value_type{
    "record_algebra_field",
    record{{
      "record",
      record{
        {"base", list{"common"}},
        {"fields", list{record{{"msg", "string"}}}},
      },
    }},
  };
  auto record_algebra = unbox(to_type(known_types, record_algebra_from_yaml));
  auto expected_record_algebra = type{
    "record_algebra_field",
    record_type{
      {"field", type{bool_type{}}},
      {"msg", type{string_type{}}},
    },
  };
  CHECK_EQUAL(record_algebra, expected_record_algebra);
  // Base Record Algebra test with name clash
  auto clashing_record_algebra_from_yaml = record::value_type{
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
    = to_type(known_types, clashing_record_algebra_from_yaml);
  CHECK_ERROR(clashing_record_algebra);
  // Extend Record Algebra test with name clash
  auto clashing_extend_record_algebra_from_yaml = record::value_type{
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
    = to_type(known_types, clashing_extend_record_algebra_from_yaml);
  auto expected_extended_record_algebra = type{
    "record_algebra_field",
    record_type{
      {"field", type{string_type{}}},
    },
  };
  CHECK_EQUAL(unbox(extended_record_algebra), expected_extended_record_algebra);
  // Implant Record Algebra test with name clash
  auto clashing_implant_record_algebra_from_yaml = record::value_type{
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
    = to_type(known_types, clashing_implant_record_algebra_from_yaml);
  auto expected_implanted_record_algebra = type{
    "record_algebra_field",
    record_type{
      {"field", type{bool_type{}}},
    },
  };
  CHECK_EQUAL(unbox(implanted_record_algebra),
              expected_implanted_record_algebra);
}

TEST(multiple members) {
  // auto x = module_ng{};
  // type y = type{};
  // auto table = symbol_table_ng{};
  //  auto r = record{
  //    {"module", std::string{"foo"}},
  //    {"description", std::string{"blab"}},
  //    {"references", list{{std::string{"http://foo.com"}},
  //                        {std::string{"https://www.google.com/search?q=foo"}}}},
  //    {"types",
  //     record{
  //       {"id", record{{"type", std::string{"string"}},
  //                     {"description", std::string{"A random unique ID
  //                     with..."}},
  //                     {"attributes", record{{"index", std::string{"has"
  //  //  REQUIRE_SUCCESS(convert(r, y, table)); // FIXME: x instead of y
  //                                                                 "h"}}}}}}}}};
  // Parsing string_type with attributes
}

TEST(YAML Module) {
  auto declaration = record{{
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
  auto result = unbox(to_module(declaration));
  auto expected_result
    = module_ng2{.types = {type{"count_field", count_type{}},
                           type{"string_field", string_type{}}}};
  CHECK_EQUAL(result, expected_result);
}

TEST(YAML Module - order independent parsing - type aliases) {
  auto declaration = record{{
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
  auto result = unbox(to_module2(declaration));
  auto expected_result
    = module_ng2{.types = {type{"type2", string_type{}},
                           type{"type1", type{"type2", string_type{}}}}};
  CHECK_EQUAL(result, expected_result);
}

TEST(YAML Module - order independent parsing - list_type) {
  auto declaration = record{{
    "types",
    record{
      {"type1", record{{"list", "type2"}}},
      {"type2", record{{"list", "type3"}}},
      {"type3", record{{"type", "string"}}},
    },
  }};
  auto result = unbox(to_module2(declaration));
  auto expected_result = module_ng2{
    .types
    = {type{"type3", string_type{}},
       type{"type2", list_type{type{"type3", string_type{}}}},
       type{"type1", list_type{
                       type{"type2", list_type{type{"type3", string_type{}}}},
                     }}}};
  CHECK_EQUAL(result, expected_result);
}

TEST(YAML Type - order indepenedent parsing - map_type) {
  std::vector<type> known_types;
  auto declaration = record{{
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
  auto result = unbox(to_module2(declaration));
  auto expected_result
    = module_ng2{.types = {
                   type{"type1", count_type{}},
                   type{"type2", string_type{}},
                   type{"map_type", map_type{type{"type1", count_type{}},
                                             type{"type2", string_type{}}}},
                 }};
  CHECK_EQUAL(result, expected_result);
}

TEST(YAML Type - order indepenedent parsing - record_type) {
  std::vector<type> known_types;
  auto declaration = record{{
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
  auto result = unbox(to_module2(declaration));
  auto expected_result = module_ng2{
    .types = {
      type{"type2", string_type{}},
      type{"type3", string_type{}},
      type{"record_field",
           record_type{{"source", type{"type2", string_type{}}},
                       {"destination", type{"type3", string_type{}}}}},
    }};
  CHECK_EQUAL(result, expected_result);
}

// FIXME:: Write checks with attributes!
// FIXME:: Test case: Map both key and value depends on the same type!
