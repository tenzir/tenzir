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

/// Controls what to do when a field name clash occurs in a record algebra.
enum class record_algebra_type { base, implant, extend };

/// Describes how to compose records in a record algebra.
struct record_composition {
  /// Controls what to do when a field name clash occurs in a record algebra.
  record_algebra_type kind;
  /// The records to merge into the Record Algebra Fields which is stored as a
  /// record in #parsed.
  std::vector<std::string> records{};
};

struct parsed_type {
  /// Potentially unresolved type
  type parsed;
  /// The types this type depends on
  /// The name of the type at parse time; the parser cannot determine whether it
  /// is qualified or unqualified.
  std::vector<std::string> providers{};
  std::optional<record_composition> algebra;
  parsed_type(type new_parsed) : parsed(std::move(new_parsed)) {
  }
  parsed_type(type new_parsed,
              const std::vector<std::string>& additional_providers)
    : parsed(std::move(new_parsed)) {
    for (const auto& provider : additional_providers)
      providers.emplace_back(provider);
  }
  parsed_type(type new_parsed, std::string_view additional_provider)
    : parsed(std::move(new_parsed)) {
    providers.emplace_back(additional_provider);
  }
};

/// The module
struct module_ng2 {
  /// The name of the module
  std::string name = {};

  /// The description of the module
  std::string description = {};

  /// The URIs pointing to the description of the format represented by the
  /// module
  std::vector<std::string> references = {};

  /// The ready-to-use resolved types with qualified names.
  std::vector<type> types;

  // FIXME: The order of types should not matter?
  friend bool operator==(const module_ng2& first,
                         const module_ng2& second) noexcept = default;
};

/// Qualifies the type name with the module name.
///
/// The qualified type name is the type name prefixed with the module_name.
std::string inline qualify(std::string_view type_name,
                           std::string_view module_name) {
  return fmt::format("{}.{}", module_name, type_name);
}

/// Determines if the possibly unqualified type_name is equal to the qualified
/// name using the given module_name for qualification.
bool inline is_equal_to_qualified(std::string_view type_name,
                                  std::string_view module_name,
                                  std::string_view qualified_name) {
  // determine if the type_name can be qualified or unqualified based on the
  // name length only.
  bool is_qualified_long = type_name.length() == qualified_name.length();
  bool is_unqualified_long
    = module_name.length() + 1 + type_name.length() == qualified_name.length();
  // The qualification is not done; it only checks for the possibility.
  if (!is_qualified_long && !is_unqualified_long)
    return false;
  if (!qualified_name.ends_with(type_name))
    return false; // Type names are different.
  if (is_qualified_long)
    return true; // The type name is qualified.
  if (is_unqualified_long)
    return true; // The type name is unqualified.
  return false;
}

// The result of the parsing. The module contains resolved types, but only the
// parsed types are available after parsing.
struct parsed_module {
  module_ng2 module{};
  std::vector<parsed_type> parsed_types{};

  void mark_resolved(const type& resolved_type) {
    module.types.push_back(resolved_type);
    // Remove the resolved dependency
    for (auto& parsed_type : parsed_types) {
      std::erase_if(parsed_type.providers, [&](const auto& current) {
        return is_equal_to_qualified(current, module.name,
                                     resolved_type.name());
      });
    }
  }

  void mark_resolved(std::vector<parsed_type>::iterator begin,
                     std::vector<parsed_type>::iterator end) {
    for (auto i = begin; i < end; i++)
      mark_resolved(i->parsed);
  }

  caf::expected<type>
  resolve_placeholder_or_inline(const type& unresolved_type);
  caf::expected<type> resolve_record(const record_type& unresolved_record_type);
  /// Returns the list type. The name and attribute must be added by the caller.
  caf::expected<type> resolve_list(const list_type& unresolved_list_type);
  caf::expected<type> resolve_map(const map_type& unresolved_map_type);
  caf::expected<type>
  resolve(const type& to_resolve,
          // const std::vector<type>& resolved_types,
          const std::optional<record_composition>& algebra = std::nullopt);
};

/// Converts a declaration into a vast type.
/// @param declaration the type declaration parsed from yaml module config file
/// @param name the name for the declaration, empty for inline types
caf::expected<parsed_type>
parse(const data& declaration, std::string_view name = std::string_view{});

caf::expected<parsed_type>
parse_record_fields(const std::vector<vast::data>& field_declarations) {
  if (field_declarations.empty())
    return caf::make_error(ec::parse_error,
                           fmt::format("record types must have at least "
                                       "one field; while parsing: {}",
                                       field_declarations));
  auto record_fields = std::vector<record_type::field_view>{};
  auto providers = std::vector<std::string>{};
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
    const auto type_or_error = parse(record_record.begin()->second);
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
  return parsed_type{type{record_type{record_fields}}, providers};
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
  const auto key_type_expected = parse(found_key->second);
  if (!key_type_expected)
    return caf::make_error(ec::parse_error,
                           fmt::format("failed to parse map key while "
                                       "parsing: {} with name: {}",
                                       map_to_parse, name),
                           key_type_expected.error());
  const auto value_type_expected = parse(found_value->second);
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
  auto providers = std::vector(key_type_expected->providers);
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
caf::expected<parsed_type>
make_parsed_record_algebra(std::string_view name, const parsed_type& new_record,
                           bool is_base, bool is_implant,
                           const std::vector<std::string>& algebra_records) {
  const auto algebra_type = is_base      ? record_algebra_type::base
                            : is_implant ? record_algebra_type::implant
                                         : record_algebra_type::extend;
  auto providers = std::vector(new_record.providers);
  for (const auto& type_name : algebra_records)
    providers.emplace_back(type_name);
  auto result = parsed_type(type{name, new_record.parsed}, providers);
  result.algebra = {algebra_type, algebra_records};
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
  const auto new_record_or_error = parse_record_fields(*fields_list_ptr);
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
  std::vector<std::string> algebra_records{};
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
                                    is_implant_found,
                                    std::move(algebra_records));
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

/// Can handle inline declarations
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
    auto type_expected = parse(found_list->second);
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
      auto new_record = parse_record_fields(*record_list_ptr);
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

/// Parses a type declaration and always qualifies it(prefixes it with the
/// module name).
caf::expected<parsed_type> parse(const std::string_view module_name,
                                 const record::value_type& type_declaration) {
  return parse(type_declaration.second,
               qualify(type_declaration.first, module_name));
}

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
parsed_module::resolve_placeholder_or_inline(const type& unresolved_type) {
  if (auto placeholder = try_read_placeholder(unresolved_type); placeholder) {
    VAST_TRACE("Resolving placeholder with "
               "name: {}",
               placeholder->name);
    const auto type_found = std::find_if(
      module.types.begin(), module.types.end(), [&](const auto& resolved_type) {
        return is_equal_to_qualified(placeholder->aliased_name, module.name,
                                     resolved_type.name());
      });
    if (type_found == module.types.end())
      return caf::make_error(ec::logic_error, fmt::format("type cannot be "
                                                          "resolved: {}",
                                                          unresolved_type));
    return *type_found;
  }
  VAST_TRACE("Resolving inline type");
  return resolve(unresolved_type);
}

/// Returns the list type. The name and attribute must be added by the caller.
caf::expected<type>
parsed_module::resolve_list(const list_type& unresolved_list_type) {
  VAST_TRACE("Resolving list_type");
  const auto resolved_type
    = resolve_placeholder_or_inline(unresolved_list_type.value_type());
  if (!resolved_type)
    return caf::make_error(ec::parse_error,
                           fmt::format("Failed to resolve list: {}",
                                       unresolved_list_type),
                           resolved_type.error());
  return type{list_type{*resolved_type}};
}

caf::expected<type>
parsed_module::resolve_map(const map_type& unresolved_map_type) {
  VAST_TRACE("Resolving map_type");
  const auto resolved_key_type
    = resolve_placeholder_or_inline(unresolved_map_type.key_type());
  if (!resolved_key_type)
    return caf::make_error(ec::parse_error,
                           fmt::format("Failed to resolve map key: {}",
                                       unresolved_map_type.key_type()),
                           resolved_key_type.error());
  const auto resolved_value_type
    = resolve_placeholder_or_inline(unresolved_map_type.value_type());
  if (!resolved_value_type)
    return caf::make_error(ec::parse_error,
                           fmt::format("Failed to resolve map value: {}",
                                       unresolved_map_type.value_type()),
                           resolved_value_type.error());
  return type{map_type{*resolved_key_type, *resolved_value_type}};
}

caf::expected<type>
parsed_module::resolve_record(const record_type& unresolved_record_type) {
  VAST_TRACE("Resolving record_type");
  auto record_fields = std::vector<record_type::field_view>{};
  for (const auto& field : unresolved_record_type.fields()) {
    const auto& resolved_type = resolve_placeholder_or_inline(field.type);
    if (!resolved_type)
      return caf::make_error(ec::parse_error,
                             "Failed to resolve record field key type",
                             resolved_type.error());
    record_fields.emplace_back(field.name, *resolved_type);
  }
  return type{record_type{record_fields}};
}

caf::expected<type>
parsed_module::resolve(const type& to_resolve,
                       const std::optional<record_composition>& algebra) {
  if (algebra) {
    // set conflict handling
    record_type::merge_conflict merge_conflict_handling
      = record_type::merge_conflict::fail;
    if (algebra->kind == record_algebra_type::implant)
      merge_conflict_handling = record_type::merge_conflict::prefer_left;
    if (algebra->kind == record_algebra_type::extend)
      merge_conflict_handling = record_type::merge_conflict::prefer_right;
    const auto& new_record = resolve(to_resolve);
    if (!new_record)
      return caf::make_error(ec::parse_error, "failed to resole algebra fields",
                             new_record.error());
    std::optional<record_type> merged_base_record{};
    for (const auto& record : algebra->records) {
      const auto& base_type
        = std::find_if(module.types.begin(), module.types.end(),
                       [&](const auto& resolved_type) {
                         return is_equal_to_qualified(record, module.name,
                                                      resolved_type.name());
                       });
      // FIXME: no duplicates should be allowed into resolved_types, check
      // somewhere!
      if (base_type == module.types.end())
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
    const auto type_found = std::find_if(
      module.types.begin(), module.types.end(), [&](const auto& resolved_type) {
        return is_equal_to_qualified(placeholder->aliased_name, module.name,
                                     resolved_type.name());
      });
    // FIXME: no duplicates should be allowed into resolved_types, check
    // somewhere!
    if (type_found == module.types.end())
      return caf::make_error(
        ec::logic_error, fmt::format("placeholder type is not resolved yet "
                                     "while trying to resolve placeholder: {}",
                                     placeholder->aliased_name));
    // FIXME: TEST if the placeholder type is not a Type Alias!
    return type{placeholder->name, *type_found};
  }
  const auto collect_unresolved = detail::overload{
    [&](const list_type& unresolved_list_type) {
      return resolve_list(unresolved_list_type);
    },
    [&](const map_type& unresolved_map_type) {
      return resolve_map(unresolved_map_type);
    },
    [&](const record_type& unresolved_record_type) {
      return resolve_record(unresolved_record_type);
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
  next_to_resolve(const parsed_module& parsed_module) {
    while (true) {
      if (resolving_types_.empty()) {
        const auto& to_resolve = parsed_module.parsed_types.front();
        resolving_types_.emplace_back(to_resolve.parsed.name());
      }
      const auto type_name_to_resolve = resolving_types_.back();
      const auto& type_to_resolve = std::find_if(
        parsed_module.parsed_types.begin(), parsed_module.parsed_types.end(),
        [&](const auto& current_parsed_type) {
          return is_equal_to_qualified(type_name_to_resolve,
                                       parsed_module.module.name,
                                       current_parsed_type.parsed.name());
        });
      // Not amongst unresolved types so it should  be resolved already
      if (type_to_resolve == parsed_module.parsed_types.end())
        //  return type_to_resolve->parsed;
        return caf::make_error(ec::logic_error,
                               fmt::format("unresolved type expected when "
                                           "trying to resolve: {}",
                                           type_name_to_resolve));
      if (!type_to_resolve->providers.empty()) {
        for (const auto& provider : type_to_resolve->providers)
          resolving_types_.push_back(provider);
      }
      if (type_name_to_resolve == resolving_types_.back())
        return *type_to_resolve;
    };
  }
  void resolved() {
    resolving_types_.pop_back();
  }

private:
  std::vector<std::string> resolving_types_{};
};

/// Parses the mandatory module name
caf::expected<std::string> parse_module_name(const record& module) {
  // the name is under the 'module' key
  const auto name_element = module.find("module");
  if (name_element == module.end())
    return caf::make_error(ec::parse_error, "module must have a name");
  const auto* name = caf::get_if<std::string>(&name_element->second);
  if (name == nullptr)
    return caf::make_error(ec::parse_error, "the format of the module's name "
                                            "is invalid");
  return *name;
}

/// Parses the optional module description
caf::expected<std::string> parse_module_description(const record& module) {
  const auto description_element = module.find("description");
  if (description_element == module.end())
    return "";
  const auto* description
    = caf::get_if<std::string>(&description_element->second);
  if (description == nullptr) {
    const auto* no_description
      = caf::get_if<caf::none_t>(&description_element->second);
    if (no_description == nullptr)
      return caf::make_error(ec::parse_error, "the format of the module's "
                                              "description is invalid");
    return "";
  }
  return *description;
}

/// Parses the optional module references
caf::expected<std::vector<std::string>>
parse_module_references(const record& module) {
  auto result = std::vector<std::string>{};
  const auto references_element = module.find("references");
  if (references_element == module.end())
    return result;
  const auto* references = caf::get_if<vast::list>(&references_element->second);
  if (references == nullptr) {
    const auto* no_references
      = caf::get_if<caf::none_t>(&references_element->second);
    if (no_references == nullptr)
      return caf::make_error(ec::parse_error,
                             "the module's references must be a "
                             "list");
    return result;
  }
  for (const auto& reference_element : *references) {
    const auto* reference = caf::get_if<std::string>(&reference_element);
    if (reference == nullptr)
      return caf::make_error(ec::parse_error, "every reference amongst the "
                                              "module's references must be a "
                                              "string");
    result.emplace_back(*reference);
  }
  return std::move(result);
}

/// Parses the optional module types
caf::expected<std::vector<parsed_type>>
parse_module_types(std::string_view module_name, const record& module) {
  auto result = std::vector<parsed_type>{};
  const auto found_types = module.find("types");
  if (found_types == module.end())
    return result;
  const auto* types_ptr = caf::get_if<record>(&found_types->second);
  if (types_ptr == nullptr)
    return caf::make_error(ec::parse_error, "parses a module with invalid "
                                            "types");
  // Parse and resolve aliases to built-in types or create placeholder types
  for (const auto& current_type : *types_ptr) {
    const auto parsed_type = parse(module_name, current_type);
    if (!parsed_type)
      return caf::make_error(
        ec::parse_error, fmt::format("failed to parse type: {}", current_type),
        parsed_type.error());
    result.push_back(*parsed_type);
  }
  return std::move(result);
}

caf::expected<module_ng2> to_module2(const data& module) {
  auto parse_result = parsed_module{};
  const auto* module_declaration_ptr = caf::get_if<record>(&module);
  if (module_declaration_ptr == nullptr)
    return caf::make_error(ec::parse_error, "parses a module with an invalid "
                                            "format");
  const auto& module_declaration = *module_declaration_ptr;
  auto module_name = parse_module_name(module_declaration);
  if (!module_name)
    return caf::make_error(ec::parse_error, "failed to parse module name",
                           module_name.error());
  parse_result.module.name = std::move(*module_name);
  auto module_description = parse_module_description(module_declaration);
  if (!module_description)
    return caf::make_error(ec::parse_error,
                           "failed to parse module description",
                           module_description.error());
  parse_result.module.description = std::move(*module_description);
  auto module_references = parse_module_references(module_declaration);
  if (!module_references)
    return caf::make_error(ec::parse_error, "failed to parse module references",
                           module_references.error());
  parse_result.module.references = std::move(*module_references);
  auto module_types
    = parse_module_types(parse_result.module.name, module_declaration);
  if (!module_types)
    return caf::make_error(ec::parse_error, "failed to parse types in module",
                           module_types.error());
  parse_result.parsed_types = std::move(*module_types);
  // resolve types
  // Move parsed items that are already resolved to resolved types.
  const auto resolved_items
    = std::stable_partition(parse_result.parsed_types.begin(),
                            parse_result.parsed_types.end(),
                            [](const auto& current_type) {
                              return !current_type.providers.empty();
                            });
  parse_result.mark_resolved(resolved_items, parse_result.parsed_types.end());
  parse_result.parsed_types.erase(resolved_items,
                                  parse_result.parsed_types.end());
  /*  for (auto& parsed_type : parsed_types) {
    VAST_ERROR("Unresolved: {}", parsed_type.parsed);
    for (auto& provider : parsed_type.providers) {
      VAST_ERROR("  provider: {}", provider);
    }
    }*/
  // Remove dependencies already resolved
  // From this point on parsed types contain only types that need to be
  // resolved.
  VAST_TRACE("Before resolving {}", parse_result.parsed_types.empty());
  resolution_manager manager{};
  // FIXME: Validate that all dependency can be resolved (all proveders are
  // amongst  parsed.names)
  // FIXME: Validate that there are no duplicates
  // FIXME: Check for circular dependencies
  while (!parse_result.parsed_types.empty()) {
    // FIXME: In case of an invalid schema parsed_types may never get empty!
    const auto type_to_resolve = manager.next_to_resolve(parse_result);
    if (!type_to_resolve)
      return caf::make_error(ec::parse_error,
                             "failed to determine the next type to resolve",
                             type_to_resolve.error());
    VAST_TRACE("Next to resolve: {}, is algebra: {}", type_to_resolve->parsed,
               type_to_resolve->algebra.has_value());
    const auto resolve_result
      = parse_result.resolve(type_to_resolve->parsed, type_to_resolve->algebra);
    if (!resolve_result)
      return caf::make_error(ec::parse_error,
                             fmt::format("Failed to resolve: {}",
                                         type_to_resolve->parsed),
                             resolve_result.error());
    auto resolved_type = *resolve_result;
    std::erase_if(parse_result.parsed_types, [&](const auto& current_type) {
      return current_type.parsed.name() == resolved_type.name();
    });
    VAST_TRACE("Resolved: {}, {}", resolved_type, resolved_type.type_index());
    parse_result.mark_resolved(std::move(resolved_type));
    manager.resolved();
  }
  return std::move(parse_result.module);
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
  const auto declaration = record{
    {"module", "test"},
    {"types",
     record{
       {"string_field1", "string"},
     }},
  };
  const auto result = unbox(to_module2(declaration));
  const auto expected_result
    = module_ng2{.name = "test",
                 .types = {
                   type{"test.string_field1", string_type{}},
                 }};
  CHECK_EQUAL(result, expected_result);
  const auto declaration2 = record{
    {"module", "test"},
    {"types",
     record{
       {
         "string_field1",
         record{{"type", "string"}},
       },
     }},
  };
  const auto result2 = unbox(to_module2(declaration));
  CHECK_EQUAL(result2, expected_result);
  const auto declaration3 = record{
    {"module", "test"},
    {"types",
     record{
       {
         "string_field2",
         record{{"type", "string"},
                {"attributes", list{"ioc", // record{{"ioc2", nullptr}},
                                    record{{"index", "hash"}}}}},
       },
     }},
  };
  const auto result3 = unbox(to_module2(declaration3));
  const auto expected_result3 = module_ng2{.name = "test",
                                           .types = {type{
                                             "test.string_field2",
                                             string_type{},
                                             {{"ioc"}, {"index", "hash"}},
                                           }}};
  CHECK_EQUAL(result3, expected_result3);
}

TEST(parsing bool type) {
  const auto declaration = record{
    {"module", "test"},
    {"types",
     record{
       {
         "bool_field",
         record{{"type", "bool"}},
       },
     }},
  };
  const auto result = unbox(to_module2(declaration));
  const auto expected_type = module_ng2{.name = "test",
                                        .types = {
                                          type{"test.bool_field", bool_type{}},
                                        }};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing integer type) {
  const auto declaration = record{
    {"module", "test"},
    {"types",
     record{
       {
         "int_field",
         record{{"type", "integer"}},
       },
     }},
  };
  const auto result = unbox(to_module2(declaration));
  const auto expected_type
    = module_ng2{.name = "test",
                 .types = {
                   type{"test.int_field", integer_type{}},
                 }};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing count_type) {
  const auto declaration = record{
    {"module", "test"},
    {"types",
     record{
       {
         "count_field",
         record{{"type", "count"}},
       },
     }},
  };
  const auto result = unbox(to_module2(declaration));
  const auto expected_type
    = module_ng2{.name = "test",
                 .types = {
                   type{"test.count_field", count_type{}},
                 }};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing real_type) {
  const auto declaration = record{
    {"module", "test"},
    {"types",
     record{
       {
         "real_field",
         record{{"type", "real"}},
       },
     }},
  };
  const auto result = unbox(to_module2(declaration));
  const auto expected_type = module_ng2{.name = "test",
                                        .types = {
                                          type{"test.real_field", real_type{}},
                                        }};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing duration_type) {
  const auto declaration = record{
    {"module", "test"},
    {"types",
     record{
       {
         "duration_field",
         record{{"type", "duration"}},
       },
     }},
  };
  const auto result = unbox(to_module2(declaration));
  const auto expected_type
    = module_ng2{.name = "test",
                 .types = {
                   type{"test.duration_field", duration_type{}},
                 }};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing time_type) {
  const auto declaration = record{
    {"module", "test"},
    {"types",
     record{
       {
         "time_field",
         record{{"type", "time"}},
       },
     }},
  };
  const auto result = unbox(to_module2(declaration));
  const auto expected_type = module_ng2{.name = "test",
                                        .types = {
                                          type{"test.time_field", time_type{}},
                                        }};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing string_type without attributes) {
  const auto declaration = record{
    {"module", "test"},
    {"types",
     record{
       {
         "string_field",
         record{{"type", "string"}},
       },
     }},
  };
  const auto result = unbox(to_module2(declaration));
  const auto expected_type
    = module_ng2{.name = "test",
                 .types = {
                   type{"test.string_field", string_type{}},
                 }};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing pattern_type) {
  const auto declaration = record{
    {"module", "test"},
    {"types",
     record{
       {
         "pattern_field",
         record{{"type", "pattern"}},
       },
     }},
  };
  const auto result = unbox(to_module2(declaration));
  const auto expected_type
    = module_ng2{.name = "test",
                 .types = {
                   type{"test.pattern_field", pattern_type{}},
                 }};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing address_type) {
  const auto declaration = record{
    {"module", "test"},
    {"types",
     record{
       {
         "address_field",
         record{{"type", "addr"}},
       },
     }},
  };
  const auto result = unbox(to_module2(declaration));
  const auto expected_type
    = module_ng2{.name = "test",
                 .types = {
                   type{"test.address_field", address_type{}},
                 }};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing subnet_type) {
  const auto declaration = record{
    {"module", "test"},
    {"types",
     record{
       {
         "subnet_field",
         record{{"type", "subnet"}},
       },
     }},
  };
  const auto result = unbox(to_module2(declaration));
  const auto expected_type
    = module_ng2{.name = "test",
                 .types = {
                   type{"test.subnet_field", subnet_type{}},
                 }};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing enumeration_type) {
  const auto declaration = record{
    {"module", "test"},
    {"types",
     record{
       {
         "enum_field",
         record{{"enum", list{"on", "off", "unknown"}}},
       },
     }},
  };
  const auto result = unbox(to_module2(declaration));
  const auto expected_type = module_ng2{
    .name = "test",
    .types = {
      type{"test.enum_field", enumeration_type{{"on"}, {"off"}, {"unknown"}}},
    }};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing list_type) {
  const auto declaration = record{
    {"module", "test"},
    {"types",
     record{
       {
         "list_field",
         record{{"list", "count"}},
       },
     }},
  };
  const auto result = unbox(to_module2(declaration));
  const auto expected_type
    = module_ng2{.name = "test",
                 .types = {
                   type{"test.list_field", list_type{count_type{}}},
                 }};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing map_type) {
  const auto declaration = record{
    {"module", "test"},
    {"types",
     record{
       {
         "map_field",
         record{
           {"map", record{{"key", "count"}, {"value", "string"}}},
         },
       },
     }},
  };
  const auto result = unbox(to_module2(declaration));
  const auto expected_type
    = module_ng2{.name = "test",
                 .types = {
                   type{
                     "test.map_field",
                     map_type{count_type{}, string_type{}},
                   },
                 }};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing record_type) {
  const auto declaration = record{
    {"module", "test"},
    {"types",
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
     }},
  };
  const auto result = unbox(to_module2(declaration));
  const auto expected_type = module_ng2{
    .name = "test",
    .types = {type{
      "test.record_field",
      record_type{
        {"src_ip", string_type{}},
        {"dst_ip", string_type{}},
      },
    }},
  };
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing inline record_type) {
  const auto declaration = record{
    {"module", "test"},
    {"types",
     record{
       {
         "record_field",
         record{{"record",
                 list{
                   record{{"source", record{{"type", "string"}}}},
                   record{{"destination", record{{"type", "strin"
                                                          "g"}}}},
                 }}},
       },
     }},
  };
  const auto result = unbox(to_module2(declaration));
  const auto expected_type = module_ng2{
    .name = "test",
    .types = {type{
      "test.record_field",
      record_type{
        {"source", string_type{}},
        {"destination", string_type{}},
      },
    }},
  };
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing inline record_type with attributes) {
  const auto declaration = record{
    {"module", "test"},
    {"types",
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
     }},
  };
  const auto result = unbox(to_module2(declaration));
  const auto expected_type = module_ng2{
    .name = "test",
    .types = {type{
      "test.record_field",
      record_type{
        {"source", type{string_type{}, {{"originator"}}}},
        {"destination", type{string_type{}, {{"responder"}}}},
      },
    }},
  };
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing record algebra) {
  const auto expected_base_record_type
    = type{"test.common", record_type{{"field", bool_type{}}}};
  const auto expected_record_algebra = type{
    "test.record_algebra_field",
    record_type{
      {"field", type{bool_type{}}},
      {"msg", type{string_type{}}},
    },
  };
  // Base Record Algebra test with name clash
  // Extend Record Algebra test with name clash
  const auto expected_extended_record_algebra = type{
    "test.record_algebra_field",
    record_type{
      {"field", type{string_type{}}},
    },
  };
  // Implant Record Algebra test with name clash
  const auto expected_implanted_record_algebra = type{
    "test.record_algebra_field",
    record_type{
      {"field", type{bool_type{}}},
    },
  };
  // Base Record Algebra test
  const auto record_algebra_from_yaml = record{
    {"module", "test"},
    {"types",
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
     }},
  };
}

TEST(YAML Module) {
  const auto declaration = record{
    {"module", "test"},
    {"types",
     record{
       {
         "count_field",
         record{{"type", "count"}},
       },
       {
         "string_field",
         record{{"type", "string"}},
       },
     }},
  };
  const auto result = unbox(to_module2(declaration));
  const auto expected_result
    = module_ng2{.name = "test",
                 .types = {type{"test.count_field", count_type{}},
                           type{"test.string_field", string_type{}}}};
  CHECK_EQUAL(result, expected_result);
}

TEST(YAML Module - type alias) {
  const auto declaration = record{
    {"module", "test"},
    {"types",
     record{
       {
         "string_field",
         record{{"type", "string"},
                {"attributes", list{"ioc", record{{"index", "has"
                                                            "h"}}}}},
       },
       {
         "string_field_alias",
         record{{"type", "string_field"}},
       },
     }},
  };
  const auto result = unbox(to_module2(declaration));
  const auto expected_result = module_ng2{
    .name = "test",
    .types = {type{
                "test.string_field",
                string_type{},
                {{"ioc"}, {"index", "hash"}},
              },
              type{
                "test.string_field_alias",
                type{
                  "test.string_field",
                  string_type{},
                  {{"ioc"}, {"index", "hash"}},
                },
              }},
  };
  CHECK_EQUAL(result, expected_result);
}

TEST(YAML Module - yaml alias node) {
  const auto* const yaml = "module: test\n"
                           "types:\n"
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
    .name = "test",
    .types
    = {type{
         "test.type1",
         list_type{record_type{
           {"src", address_type{}},
           {"dst", address_type{}},
         }},
       },
       type{
         "test.type2",
         map_type{string_type{},
                  record_type{
                    {"src", address_type{}},
                    {"dst", address_type{}},
                  }},
       },
       type{"test.type3", string_type{}, {{"attr1_key"}, {"attr2_key"}}}},
  };
  CHECK_EQUAL(result, expected);
}

TEST(YAML Module - order independent parsing - type aliases) {
  const auto declaration = record{
    {"module", "test"},
    {"types",
     record{
       {
         "type1",
         record{{"type", "type2"}},
       },
       {
         "type2",
         record{{"type", "string"}},
       },
     }},
  };
  const auto result = unbox(to_module2(declaration));
  const auto expected_result = module_ng2{
    .name = "test",
    .types = {type{"test.type2", string_type{}},
              type{"test.type1", type{"test.type2", string_type{}}}}};
  CHECK_EQUAL(result, expected_result);
}

TEST(YAML Module - order independent parsing - type enumeration) {
  const auto declaration = record{
    {"module", "test"},
    {"types",
     record{
       {
         "enum_field",
         record{{"enum", list{"on", "off", "unknown"}}},
       },
     }},
  };
  const auto result = unbox(to_module2(declaration));
  const auto expected_result = module_ng2{
    .name = "test",
    .types = {
      type{"test.enum_field", enumeration_type{{"on"}, {"off"}, {"unknown"}}},
    }};
  CHECK_EQUAL(result, expected_result);
}

TEST(YAML Module - order independent parsing - list_type) {
  const auto declaration = record{
    {"module", "test"},
    {"types",
     record{
       {"type1", record{{"list", "type2"}}},
       {"type2", record{{"list", "type3"}}},
       {"type3", record{{"type", "string"}}},
     }},
  };
  const auto result = unbox(to_module2(declaration));
  const auto expected_result = module_ng2{
    .name = "test",
    .types
    = {type{"test.type3", string_type{}},
       type{"test.type2", list_type{type{"test.type3", string_type{}}}},
       type{"test.type1",
            list_type{
              type{"test.type2", list_type{type{"test.type3", string_type{}}}},
            }}}};
  CHECK_EQUAL(result, expected_result);
}

TEST(YAML Module - order indepenedent parsing - map_type) {
  const auto declaration = record{
    {"module", "test"},
    {"types",
     record{
       {"map_type",
        record{
          {"map", record{{"key", "type1"}, {"value", "type2"}}},
        }},
       {"type1", record{{"type", "count"}}},
       {"type2", record{{"type", "string"}}},
     }},
  };
  const auto result = unbox(to_module2(declaration));
  const auto expected_result = module_ng2{
    .name = "test",
    .types = {
      type{"test.type1", count_type{}},
      type{"test.type2", string_type{}},
      type{"test.map_type", map_type{type{"test.type1", count_type{}},
                                     type{"test.type2", string_type{}}}},
    }};
  CHECK_EQUAL(result, expected_result);
  // both key and value depends on the same type
  const auto declaration_same_key_and_value_type = record{
    {"module", "test"},
    {"types",
     record{
       {"map_type",
        record{
          {"map", record{{"key", "type1"}, {"value", "type1"}}},
        }},
       {"type1", record{{"type", "string"}}},
     }},
  };
  const auto same_key_and_value_result
    = unbox(to_module2(declaration_same_key_and_value_type));
  const auto expected_same_key_and_value_result = module_ng2{
    .name = "test",
    .types = {
      type{"test.type1", string_type{}},
      type{"test.map_type", map_type{type{"test.type1", string_type{}},
                                     type{"test.type1", string_type{}}}},
    }};
  CHECK_EQUAL(same_key_and_value_result, expected_same_key_and_value_result);
}

TEST(YAML Module - order indepenedent parsing - record_type) {
  const auto declaration = record{
    {"module", "test"},
    {"types",
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
     }},
  };
  const auto result = unbox(to_module2(declaration));
  const auto expected_result = module_ng2{
    .name = "test",
    .types = {
      type{"test.type2", string_type{}},
      type{"test.type3", string_type{}},
      type{"test.record_field",
           record_type{{"source", type{"test.type2", string_type{}}},
                       {"destination", type{"test.type3", string_type{}}}}},
    }};
  CHECK_EQUAL(result, expected_result);
}

TEST(YAML Module - name description references) {
  // Normal case must succeed
  const auto* normal_case
    = "{ module: test, description: desc, references: [ref1, ref2]}";
  const auto expected_normal_case = module_ng2{
    .name = "test",
    .description = "desc",
    .references = {"ref1", "ref2"},
  };
  const auto normal_case_converted = unbox(from_yaml(normal_case));
  CHECK_EQUAL(expected_normal_case, to_module2(normal_case_converted));
  // Missing name must fail
  const auto* missing_name = "{ description: desc, references: [ref1, ref2]}";
  auto missing_name_converted = unbox(from_yaml(missing_name));
  CHECK_ERROR(to_module2(missing_name_converted));
  // Wrong name must fail
  const auto* wrong_name
    = "{ module: 42, description: desc, references: [ref1, ref2]}";
  auto wrong_name_converted = unbox(from_yaml(wrong_name));
  CHECK_ERROR(to_module2(wrong_name_converted));
  // name without value must fail
  const auto* no_value_name
    = "{ module: , description: desc, references: [ref1, ref2]}";
  auto no_value_name_converted = unbox(from_yaml(no_value_name));
  CHECK_ERROR(to_module2(no_value_name_converted));
  // Missing description must succeed
  const auto* missing_description = "{ module: test, references: [ref1, ref2]}";
  const auto expected_no_description = module_ng2{
    .name = "test",
    .description = "",
    .references = {"ref1", "ref2"},
  };
  const auto missing_description_converted
    = unbox(from_yaml(missing_description));
  CHECK_EQUAL(expected_no_description,
              to_module2(missing_description_converted));
  // Description without value must succeed
  const auto* no_value_description
    = "{ module: test, description: , references: [ref1, ref2]}";
  const auto no_value_description_converted
    = unbox(from_yaml(no_value_description));
  CHECK_EQUAL(expected_no_description,
              to_module2(no_value_description_converted));
  // Wrong description must fail
  const auto* wrong_description
    = "{ module: test, description: [list], references: [ref1, ref2]}";
  auto wrong_description_converted = unbox(from_yaml(wrong_description));
  CHECK_ERROR(to_module2(wrong_description_converted));
  // Missing description must succeed
  const auto* missing_references = "{ module: test, description: desc}";
  const auto expected_no_references = module_ng2{
    .name = "test",
    .description = "desc",
    .references = {},
  };
  const auto missing_references_converted
    = unbox(from_yaml(missing_references));
  CHECK_EQUAL(expected_no_references, to_module2(missing_references_converted));
  // Wrong references must fail
  const auto* wrong_references
    = "{ module: test, description: desc, references: {url: ref1}}";
  auto wrong_references_converted = unbox(from_yaml(wrong_references));
  CHECK_ERROR(to_module2(wrong_references_converted));
  // Empty references must succeed
  const auto* empty_references
    = "{ module: test, description: desc, references: []}";
  const auto empty_references_converted = unbox(from_yaml(empty_references));
  CHECK_EQUAL(expected_no_references, to_module2(empty_references_converted));
  // References without value must succeed
  const auto* no_value_references
    = "{ module: test, description: desc, references:}";
  const auto no_value_references_converted
    = unbox(from_yaml(no_value_references));
  CHECK_EQUAL(expected_no_references,
              to_module2(no_value_references_converted));
  // Wrong references must fail
  const auto* wrong_reference
    = "{ module: test, description: desc, references: [42]}";
  auto wrong_reference_converted = unbox(from_yaml(wrong_reference));
  CHECK_ERROR(to_module2(wrong_reference_converted));
  // Reference without value is just skipped by the yaml parser so it works
  const auto* no_value_reference
    = "{ module: test, description: desc, references: [cool_site,]}";
  auto no_value_reference_converted = unbox(from_yaml(no_value_reference));
  const auto expected_no_value_references = module_ng2{
    .name = "test",
    .description = "desc",
    .references = {"cool_site"},
  };
  CHECK_EQUAL(expected_no_value_references,
              to_module2(no_value_reference_converted));
}

TEST(YAML Module - minimal suricata sample) {
  const auto* suricata_yaml
    = "module: suricata\n"
      "\n"
      "description: >-\n"
      "  Suricata is an open-source threat detection engine, combining\n"
      "  intrusion  detection (IDS), intrusion prevention (IPS), network\n"
      "  security monitoring (NSM) and PCAP processing.\n"
      "\n"
      "references:\n"
      "  - 'https://suricata.io/'\n"
      "  - 'https://github.com/OISF/suricata'\n"
      "\n"
      "types:\n"
      "  count_id:\n"
      "    type: count\n"
      "    attributes:\n"
      "      - index: hash\n"
      "  string_id:\n"
      "    type: string\n"
      "    attributes:\n"
      "      - index: hash\n"
      "  port: count\n"     // MOVE to base.schema
      "  timestamp: time\n" // MOVE to base.schema
      "  common:\n"
      "    record:\n"
      "      - timestamp: timestamp\n"
      "      - pcap_cnt: count\n"
      "      - vlan:\n"
      "          list: count\n"
      "      - in_iface: string\n"
      "      # I noticed that it would actually be nicer to *just*\n"
      "      # reference the concept here, i.e., write\n"
      "      #\n"
      "      #   - src_ip: vast.net.src.ip\n"
      "      #\n"
      "      # But since concepts are not typed, this doesn't work.\n"
      "      # See the note in the corresponding VAST schema.\n"
      "      - src_ip:\n"
      "          type: addr\n"
      "#          concept: vast.net.src.ip\n"
      "      - src_port:\n"
      "          type: port\n"
      "#          concept: vast.net.src.port\n"
      "      - dest_ip:\n"
      "          type: addr\n"
      "#          concept: vast.net.dst.ip\n"
      "      - dest_ip:\n"
      "          type: port\n"
      "#          concept: vast.net.dst.port\n"
      "      - proto: \n"
      "          type: string\n"
      "#          concept: vast.net.proto\n"
      "      - event_type: string\n"
      "      - community_id:\n"
      "          type: string_id\n"
      "#          concept: vast.net.community_id\n"
      "  component-flow:\n"
      "    record:\n"
      "      - pkts_toserver: count\n"
      "      - pkts_toclient: count\n"
      "      - bytes_toserver: count\n"
      "      - bytes_toclient: count\n"
      "      - start: time\n"
      "      - end: time\n"
      "      - age: count\n"
      "      - state: string\n"
      "      - reason: string\n"
      "      - alerted: bool\n"
      "  alert:\n"
      "    record:\n"
      "      base:\n"
      "        - common\n"
      "      fields:\n"
      "        - alert:\n"
      "            record:\n"
      "              - app_proto: string\n"
      "              - action:\n"
      "                  enum:\n"
      "                    - allowed\n"
      "                    - blocked\n"
      "              - gid: count_id\n"
      "              - signature_id: count_id\n"
      "              - rev: count\n"
      "              - signature: string\n"
      "              - category: string\n"
      "              - severity: count\n"
      "              - source:\n"
      "                  record:\n"
      "                    - ip: addr\n"
      "                    - port: port\n"
      "              - target:\n"
      "                  record:\n"
      "                    - ip: addr\n"
      "                    - port: port\n"
      "              - flow: component-flow\n"
      "              - payload: string\n"
      "              - payload_printable: string\n"
      "              - stream: count\n"
      "              - packet: string\n"
      "              - packet_info:\n"
      "                  record:\n"
      "                    - linktype: count\n"
      "  anomaly:\n"
      "    record:\n"
      "      base:\n"
      "        - common\n"
      "      fields:\n"
      "        - tx_id: count_id\n"
      "        - anomaly:\n"
      "            record:\n"
      "              - type: string\n"
      "              - event: string\n"
      "              - code: count\n"
      "              - layer: string\n"
      "  dcerpc_interface:\n"
      "    record:\n"
      "      - uuid: string\n"
      "      - version: string\n"
      "      - ack_result: count\n"
      "  dcerpc:\n"
      "    record:\n"
      "      base:\n"
      "        - common\n"
      "      fields:\n"
      "        - dcerpc:\n"
      "            record:\n"
      "              - request: string\n"
      "              - response: string\n"
      "              - call_id: count\n"
      "              - rpc_version: string\n"
      "              - interfaces:\n"
      "                  list: dcerpc_interface\n"
      "              - req:\n"
      "                  record:\n"
      "                    - opnum: count\n"
      "                    - frag_cnt: count\n"
      "                    - stub_data_size: count\n"
      "              - res:\n"
      "                  record:\n"
      "                    - frag_cnt: count\n"
      "                    - stub_data_size: count\n"
      "  # At the time of writing no canonical documentation exists\n"
      "  # for dhcp events. The fields can be derived from the logging\n"
      "  # code in:\n"
      "  # "
      "https://github.com/OISF/suricata/blob/master/rust/src/dhcp/logger.rs\n"
      "  dhcp:\n"
      "    record:\n"
      "      base:\n"
      "        - common\n"
      "      fields:\n"
      "        - dhcp:\n"
      "            record:\n"
      "            - type: string # enum{request, reply, <unknown>}\n"
      "            - id: count_id\n"
      "            - client_mac: string\n"
      "            - assigned_ip: addr\n"
      "            - client_ip: addr\n"
      "            - dhcp_type:\n"
      "                enum:\n"
      "                  - discover\n"
      "                  - offer\n"
      "                  - request\n"
      "                  - decline\n"
      "                  - ack\n"
      "                  - nak\n"
      "                  - release\n"
      "                  - inform\n"
      "                  - unknown\n"
      "            # In requests\n"
      "            - client_id: string_id\n"
      "            - hostname: string\n"
      "            - requested_ip: addr\n"
      "            - params:\n"
      "                list: string\n"
      "            # In replies:\n"
      "            - relay_ip: addr\n"
      "            - next_server_ip: addr\n"
      "            - lease_time: count\n"
      "            - rebinding_time: count\n"
      "            - renewal_time: count\n"
      "            - subnet_mask: addr\n"
      "            - routers:\n"
      "                list: addr\n"
      "            - dns_servers:\n"
      "                list: addr\n"
      "  dns:\n"
      "    record:\n"
      "      base:\n"
      "        - common\n"
      "      fields:\n"
      "        - dns:\n"
      "            record:\n"
      "              - version: count\n"
      "              - type:\n"
      "                  enum:\n"
      "                    - answer\n"
      "                    - query\n"
      "              - id: count_id\n"
      "              - flags: string\n"
      "              - rrname: string\n"
      "              - rrtype: string\n"
      "              - rcode: string\n"
      "              - rdata: string\n"
      "              - ttl: count\n"
      "              - tx_id: count_id\n"
      "              - grouped:\n"
      "                  record:\n"
      "                    - A:\n"
      "                        list: addr\n"
      "  ftp:\n"
      "    record:\n"
      "      base:\n"
      "        - common\n"
      "      fields:\n"
      "        - ftp:\n"
      "            record:\n"
      "              - command: string_id\n"
      "              - command_data: string_id\n"
      "              - reply:\n"
      "                  list: string\n"
      "              - completion_code:\n"
      "                  list: string\n"
      "              - dynamic_port: port\n"
      "              - mode: string\n"
      "              - reply_received: string\n"
      "  ftp_data:\n"
      "    record:\n"
      "      base:\n"
      "        - common\n"
      "      fields:\n"
      "        - ftp_data:\n"
      "            record:\n"
      "              - filename: string_id\n"
      "              - command: string_id\n"
      "  http:\n"
      "    record:\n"
      "      base:\n"
      "        - common\n"
      "      fields:\n"
      "        - http:\n"
      "            record:\n"
      "              - hostname: string\n"
      "              - url: string\n"
      "              - http_port: port\n"
      "              - http_user_agent: string\n"
      "              - http_content_type: string\n"
      "              - http_method: string\n"
      "              - http_refer: string\n"
      "              - protocol: string\n"
      "              - status: count\n"
      "              - redirect: string\n"
      "              - length: count\n"
      "        - tx_id: count_id\n"
      "  fileinfo:\n"
      "    record:\n"
      "      base:\n"
      "        - common\n"
      "      fields:\n"
      "        - fileinfo:\n"
      "            record:\n"
      "              - filename: string\n"
      "              - magic: string\n"
      "              - gaps: bool\n"
      "              - state: string\n"
      "              - md5: string_id\n"
      "              - sha1: string_id\n"
      "              - sha256: string_id\n"
      "              - stored: bool\n"
      "              - file_id: count_id\n"
      "              - size: count\n"
      "              - tx_id: count_id\n"
      "        - http:\n"
      "            record:\n"
      "              - hostname: string\n"
      "              - url: string\n"
      "              - http_port: port\n"
      "              - http_user_agent: string\n"
      "              - http_content_type: string\n"
      "              - http_method: string\n"
      "              - http_refer: string\n"
      "              - protocol: string\n"
      "              - status: count\n"
      "              - redirect: string\n"
      "              - length: count\n"
      "        - app_proto: string\n"
      "  flow:\n"
      "    record:\n"
      "      base:\n"
      "        - common\n"
      "      fields:\n"
      "        - flow: component-flow\n"
      "        - app_proto: string\n"
      "  ikev2:\n"
      "    record:\n"
      "      base:\n"
      "        - common\n"
      "      fields:\n"
      "        - tx_id: count_id\n"
      "        - ikev2:\n"
      "            record:\n"
      "              - version_major: count\n"
      "              - version_minor: count\n"
      "              - exchange_type: count\n"
      "              - message_id: count\n"
      "              - init_spi: string\n"
      "              - resp_spi: string\n"
      "              - role: string\n"
      "              - errors: count\n"
      "              - payload:\n"
      "                  list: string\n"
      "              - notify:\n"
      "                  list: string\n"
      "# TODO: continue with krb5 event until end of old schema file.\n";
  const auto declaration = unbox(from_yaml(suricata_yaml));
  CHECK_NOERROR(to_module2(declaration));
}

TEST(YAML Module - order independent parsing - record algebra) {
  // Creating a base record for later Record Algebra tests.
  const auto base_record_declaration = record{
    {"module", "test"},
    {"types",
     record{
       {"record_algebra_field",
        record{{"record",
                record{
                  {"base", list{"common"}},
                  {"fields", list{record{{"msg", "string"}}}},
                }}}},
       {"common",
        record{{"record", list{record{{"field", record{{"type", "boo"
                                                                "l"}}}}}}}},
     }}};
  const auto result = unbox(to_module2(base_record_declaration));
  const auto expected_result
    = module_ng2{.name = "test",
                 .types = {
                   type{"test.common", record_type{{"field", bool_type{}}}},
                   type{"test.record_algebra_field",
                        record_type{
                          {"field", bool_type{}},
                          {"msg", string_type{}},
                        }},
                 }};
  CHECK_EQUAL(result, expected_result);
  // Base Record Algebra test with name clash
  const auto clashing_base_record_declaration = record{
    {"module", "test"},
    {"types",
     record{
       {"record_algebra_field",
        record{{"record",
                record{
                  {"base", list{"common"}},
                  {"fields", list{record{{"msg", "string"}}}},
                }}}},
       {"common",
        record{{"record", list{record{{"msg", record{{"type", "bool"}}}}}}}},
     }}};
  const auto clashing_record_algebra
    = to_module2(clashing_base_record_declaration);
  VAST_INFO("base record algebra clash: {}", clashing_record_algebra.error());
  CHECK_ERROR(clashing_record_algebra);
  // Extend Record Algebra test with name clash
  const auto clashing_extend_record_algebra_from_yaml = record{
    {"module", "test"},
    {"types",
     record{
       {"record_algebra_field",
        record{{"record",
                record{
                  {"extend", list{"common"}},
                  {"fields", list{record{{"msg", "string"}}}},
                }}}},
       {"common",
        record{{"record", list{record{{"msg", record{{"type", "bool"}}}}}}}},
     }},
  };
  const auto extended_record_algebra
    = unbox(to_module2(clashing_extend_record_algebra_from_yaml));
  const auto expected_extended_record_algebra
    = module_ng2{.name = "test",
                 .types = {
                   type{"test.common", record_type{{"msg", bool_type{}}}},
                   type{"test.record_algebra_field",
                        record_type{
                          {"msg", string_type{}},
                        }},
                 }};
  CHECK_EQUAL(extended_record_algebra, expected_extended_record_algebra);
  // Implant Record Algebra test with name clash
  const auto clashing_implant_record_algebra_from_yaml = record{
    {"module", "test"},
    {"types",
     record{
       {"record_algebra_field",
        record{{"record",
                record{
                  {"implant", list{"common"}},
                  {"fields", list{record{{"msg", "string"}}}},
                }}}},
       {"common",
        record{{"record", list{record{{"msg", record{{"type", "bool"}}}}}}}},
     }},
  };
  const auto implanted_record_algebra
    = unbox(to_module2(clashing_implant_record_algebra_from_yaml));
  const auto expected_implanted_record_algebra = module_ng2{
    .name = "test",
    .description = "",
    .references = {},
    .types = {type{"test.common", record_type{{"msg", bool_type{}}}},
              type{"test.record_algebra_field",
                   record_type{
                     {"msg", bool_type{}},
                   }}},
  };
  CHECK_EQUAL(implanted_record_algebra, expected_implanted_record_algebra);
}

/*
// FIXME:: Write checks with attributes!
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
