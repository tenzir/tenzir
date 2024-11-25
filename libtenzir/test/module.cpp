//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/module.hpp"

#include "tenzir/aliases.hpp"
#include "tenzir/concept/convertible/data.hpp"
#include "tenzir/data.hpp"
#include "tenzir/legacy_type.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/type.hpp"

#include <caf/error.hpp>
#include <caf/sum_type.hpp>
#include <caf/test/dsl.hpp>
#include <fmt/format.h>

#include <optional>
#include <string_view>

using namespace tenzir;

/// Converts a declaration into a tenzir type.
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
    const auto* record_record_ptr = try_as<record>(&record_value);
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
  = std::array{"bool", "integer", "count",   "real",   "duration",
               "time", "string",  "address", "subnet", "enum",
               "list", "map",     "record"};

caf::expected<type> to_enum(std::string_view name, const data& enumeration,
                            std::vector<type::attribute_view>&& attributes) {
  const auto* enum_list_ptr = try_as<list>(&enumeration);
  if (enum_list_ptr == nullptr)
    return caf::make_error(ec::parse_error, "enum must be specified as a "
                                            "YAML list");
  const auto& enum_list = *enum_list_ptr;
  if (enum_list.empty())
    return caf::make_error(ec::parse_error, "enum cannot be empty");
  auto enum_fields = std::vector<enumeration_type::field_view>{};
  enum_fields.reserve(enum_list.size());
  for (const auto& enum_value : enum_list) {
    const auto* enum_string_ptr = try_as<std::string>(&enum_value);
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
  const auto* map_record_ptr = try_as<record>(&map_to_parse);
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
  const auto* record_algebra_record_ptr = try_as<record>(&record_algebra);
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
  const auto* fields_list_ptr = try_as<list>(&found_fields->second);
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
  const auto* records_list_ptr = try_as<list>(&records);
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
    const auto* record_name_ptr = try_as<std::string>(&record);
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
    const auto* base_record_ptr = try_as<record_type>(&base_type);
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
  const auto* known_type_name_ptr = try_as<std::string>(&declaration);
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
      return type{name, int64_type{}};
    if (known_type_name == "count")
      return type{name, uint64_type{}};
    if (known_type_name == "real")
      return type{name, double_type{}};
    if (known_type_name == "duration")
      return type{name, duration_type{}};
    if (known_type_name == "time")
      return type{name, time_type{}};
    if (known_type_name == "string")
      return type{name, string_type{}};
    if (known_type_name == "address")
      return type{name, ip_type{}};
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
  const auto* declaration_record_ptr = try_as<record>(&declaration);
  if (declaration_record_ptr == nullptr)
    return caf::make_error(ec::parse_error, "type alias must be specified as a "
                                            "YAML dictionary");
  const auto& declaration_record = *declaration_record_ptr;
  // Get the optional attributes
  auto attributes = std::vector<type::attribute_view>{};
  auto found_attributes = declaration_record.find("attributes");
  if (found_attributes != declaration_record.end()) {
    const auto* attribute_list = try_as<list>(&found_attributes->second);
    if (attribute_list == nullptr)
      return caf::make_error(ec::parse_error, "the attribute list must be "
                                              "specified as a YAML list");
    for (const auto& attribute : *attribute_list) {
      const auto* attribute_string_ptr = try_as<std::string>(&attribute);
      if (attribute_string_ptr != nullptr)
        attributes.push_back({*attribute_string_ptr});
      else {
        const auto* attribute_record_ptr = try_as<record>(&attribute);
        if (attribute_record_ptr == nullptr)
          return caf::make_error(ec::parse_error, "attribute must be specified "
                                                  "as a YAML dictionary");
        const auto& attribute_record = *attribute_record_ptr;
        if (attribute_record.size() != 1)
          return caf::make_error(ec::parse_error, "attribute must have a "
                                                  "single field");
        const auto& attribute_key = attribute_record.begin()->first;
        const auto* attribute_value_ptr
          = try_as<std::string>(&attribute_record.begin()->second);
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
    const auto* record_list_ptr = try_as<list>(&found_record->second);
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
  auto int64_type_wo_attrs = record::value_type{
    "int_field",
    record{{"type", "integer"}},
  };
  auto result = unbox(to_type(known_types, int64_type_wo_attrs));
  auto expected_type = type{"int_field", int64_type{}};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing uint64_type) {
  std::vector<type> known_types;
  auto uint64_type_wo_attrs = record::value_type{
    "count_field",
    record{{"type", "count"}},
  };
  auto result = unbox(to_type(known_types, uint64_type_wo_attrs));
  auto expected_type = type{"count_field", uint64_type{}};
  CHECK_EQUAL(result, expected_type);
}

TEST(YAML Type - Parsing double_type) {
  std::vector<type> known_types;
  auto double_type_wo_attrs = record::value_type{
    "real_field",
    record{{"type", "real"}},
  };
  auto result = unbox(to_type(known_types, double_type_wo_attrs));
  auto expected_type = type{"real_field", double_type{}};
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

TEST(YAML Type - Parsing ip_type) {
  std::vector<type> known_types;
  auto ip_type_wo_attrs = record::value_type{
    "address_field",
    record{{"type", "address"}},
  };
  auto result = unbox(to_type(known_types, ip_type_wo_attrs));
  auto expected_type = type{"address_field", ip_type{}};
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
  auto expected_type = type{"list_field", list_type{uint64_type{}}};
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
    map_type{uint64_type{}, string_type{}},
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
