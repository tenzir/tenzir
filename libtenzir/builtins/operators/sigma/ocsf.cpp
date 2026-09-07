//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "ocsf.hpp"

#include <tenzir/arrow_memory_pool.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/concept/printable/tenzir/data.hpp>
#include <tenzir/concept/printable/to_string.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/series.hpp>

#include <arrow/array/array_base.h>
#include <arrow/array/array_nested.h>
#include <arrow/array/builder_primitive.h>
#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <iterator>
#include <ranges>
#include <string>
#include <utility>

namespace tenzir::plugins::sigma::ocsf {

namespace {

auto get_record_field(series const& input, std::string_view name)
  -> Option<series> {
  auto const records = input.as<record_type>();
  if (not records) {
    return None{};
  }
  auto index = int{0};
  for (auto const& field : records->type.fields()) {
    if (field.name == name) {
      auto child = check(
        records->array->GetFlattenedField(index, tenzir::arrow_memory_pool()));
      return series{field.type, std::move(child)};
    }
    ++index;
  }
  return None{};
}

auto string_at(series const& input, int64_t row) -> Option<std::string_view> {
  auto const strings = input.as<string_type>();
  if (not strings or strings->array->IsNull(row)) {
    return None{};
  }
  return strings->array->GetView(row);
}

auto integer_at(series const& input, int64_t row) -> Option<uint64_t> {
  if (auto const integers = input.as<int64_type>()) {
    if (integers->array->IsNull(row) or integers->array->Value(row) < 0) {
      return None{};
    }
    return static_cast<uint64_t>(integers->array->Value(row));
  }
  if (auto const integers = input.as<uint64_type>()) {
    if (integers->array->IsNull(row)) {
      return None{};
    }
    return integers->array->Value(row);
  }
  return None{};
}

auto make_string_series(int64_t length, auto&& value_at_row) -> series {
  auto builder = string_type::make_arrow_builder(arrow_memory_pool());
  check(builder->Reserve(length));
  for (auto row = int64_t{0}; row < length; ++row) {
    auto const value = value_at_row(row);
    if (value) {
      check(builder->Append(*value));
    } else {
      check(builder->AppendNull());
    }
  }
  return series{string_type{}, finish(*builder)};
}

auto combine_presence(series const& left, series const& right,
                      bool require_both) -> series {
  auto const lhs = left.as<bool_type>();
  auto const rhs = right.as<bool_type>();
  TENZIR_ASSERT(lhs and rhs);
  auto builder = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
  check(builder.Reserve(left.length()));
  for (auto row = int64_t{0}; row < left.length(); ++row) {
    auto const lhs_value
      = not lhs->array->IsNull(row) and lhs->array->Value(row);
    auto const rhs_value
      = not rhs->array->IsNull(row) and rhs->array->Value(row);
    check(builder.Append(require_both ? lhs_value and rhs_value
                                      : lhs_value or rhs_value));
  }
  return series{bool_type{}, finish(builder)};
}

struct PathFallback {
  series value;
  series path;
};

auto resolve_path_fallback(series const& input, std::string_view primary,
                           std::string_view fallback) -> PathFallback {
  auto const first = resolve_field(input, primary);
  auto const second = resolve_field(input, fallback);
  auto value = make_string_series(input.length(), [&](int64_t row) {
    auto const primary_value = string_at(first, row);
    return primary_value and not primary_value->empty()
             ? primary_value
             : string_at(second, row);
  });
  auto path = make_string_series(input.length(), [&](int64_t row) {
    auto const primary_value = string_at(first, row);
    if (primary_value and not primary_value->empty()) {
      return Option<std::string_view>{primary};
    }
    if (string_at(second, row)) {
      return Option<std::string_view>{fallback};
    }
    return Option<std::string_view>{};
  });
  return PathFallback{std::move(value), std::move(path)};
}

auto resolve_joined(series const& input, JoinedField const& projection)
  -> series {
  auto const left = resolve_field(input, projection.left);
  auto const right = resolve_field(input, projection.right);
  return make_string_series(input.length(), [&](int64_t row) {
    auto const lhs = string_at(left, row);
    auto const rhs = string_at(right, row);
    if (not lhs or not rhs) {
      return Option<std::string>{};
    }
    if (lhs->empty()) {
      return Option<std::string>{std::string{*rhs}};
    }
    if (rhs->empty()) {
      return Option<std::string>{std::string{*lhs}};
    }
    return Option<std::string>{
      fmt::format("{}{}{}", *lhs, projection.separator, *rhs)};
  });
}

auto resolve_principal(series const& input, PrincipalField const& projection)
  -> series {
  auto const domains = resolve_field(input, projection.domain);
  auto const names = resolve_field(input, projection.name);
  return make_string_series(input.length(), [&](int64_t row) {
    auto const name = string_at(names, row);
    if (not name) {
      return Option<std::string>{};
    }
    auto const domain = string_at(domains, row);
    if (not domain or domain->empty()) {
      return Option<std::string>{std::string{*name}};
    }
    return Option<std::string>{fmt::format("{}\\{}", *domain, *name)};
  });
}

auto source_identifier_at(series const& input, int64_t row)
  -> Option<std::string> {
  if (auto const value = string_at(input, row)) {
    return std::string{*value};
  }
  if (auto const integers = input.as<int64_type>();
      integers and not integers->array->IsNull(row)) {
    return fmt::to_string(integers->array->Value(row));
  }
  auto const integers = input.as<uint64_type>();
  if (integers and not integers->array->IsNull(row)) {
    return fmt::to_string(integers->array->Value(row));
  }
  return None{};
}

/// Maps every standard OCSF fingerprint algorithm ID to a source-style
/// token: uppercase with separators stripped, curated where a common form
/// exists. Sources such as Sysmon only emit a few of these, but conformant
/// OCSF events may carry any; a recognized ID never drops a fingerprint.
auto fingerprint_algorithm(uint64_t id) -> Option<std::string_view> {
  switch (id) {
    case 1:
      return "MD5";
    case 2:
      return "SHA1";
    case 3:
      return "SHA256";
    case 4:
      return "SHA512";
    case 5:
      return "CTPH";
    case 6:
      return "TLSH";
    case 7:
      return "QUICKXOR";
    case 8:
      return "SHA224";
    case 9:
      return "SHA384";
    case 10:
      return "SHA512224";
    case 11:
      return "SHA512256";
    case 12:
      return "SHA3224";
    case 13:
      return "SHA3256";
    case 14:
      return "SHA3384";
    case 15:
      return "SHA3512";
    case 16:
      return "XXHASH64";
    case 17:
      return "XXHASH128";
    case 18:
      return "IMPHASH";
    case 19:
      return "NPF";
    case 20:
      return "HASSH";
    default:
      return None{};
  }
}

auto resolve_fingerprints(series const& input, std::string_view path)
  -> series {
  auto const hashes = resolve_field(input, path);
  auto const lists = hashes.as<list_type>();
  if (not lists) {
    return series::null(string_type{}, input.length());
  }
  auto const fingerprints = lists->list_values();
  auto const algorithms
    = get_record_field(fingerprints, "algorithm_id")
        .value_or(series::null(null_type{}, fingerprints.length()));
  auto const algorithm_names
    = get_record_field(fingerprints, "algorithm")
        .value_or(series::null(null_type{}, fingerprints.length()));
  auto const values
    = get_record_field(fingerprints, "value")
        .value_or(series::null(null_type{}, fingerprints.length()));
  auto builder = arrow::StringBuilder{arrow_memory_pool()};
  check(builder.Reserve(input.length()));
  auto const base = lists->array->value_offset(0);
  for (auto row = int64_t{0}; row < input.length(); ++row) {
    if (lists->array->IsNull(row)) {
      check(builder.AppendNull());
      continue;
    }
    auto reconstructed = std::string{};
    auto const begin = lists->array->value_offset(row) - base;
    auto const end = lists->array->value_offset(row + 1) - base;
    for (auto index = begin; index < end; ++index) {
      auto const algorithm_id = integer_at(algorithms, index);
      auto const value = string_at(values, index);
      if (not algorithm_id or not value) {
        continue;
      }
      auto const standard_algorithm = fingerprint_algorithm(*algorithm_id);
      auto const sibling_algorithm = string_at(algorithm_names, index);
      auto const algorithm
        = standard_algorithm ? standard_algorithm : sibling_algorithm;
      if (not algorithm or algorithm->empty()) {
        continue;
      }
      if (not reconstructed.empty()) {
        reconstructed += ',';
      }
      reconstructed += fmt::format("{}={}", *algorithm, *value);
    }
    check(builder.Append(reconstructed));
  }
  return series{string_type{}, finish(builder)};
}

auto resolve_object_list(series const& input, ObjectListField const& projection)
  -> series {
  auto const objects = resolve_field(input, projection.path);
  auto const lists = objects.as<list_type>();
  if (not lists) {
    return series::null(list_type{string_type{}}, input.length());
  }
  auto const values = lists->list_values();
  auto member = get_record_field(values, projection.member)
                  .value_or(series::null(string_type{}, values.length()));
  return make_list_series_with_offsets(
    member, rebase_list_array_buffers(*lists->array));
}

auto resolve_type(type const& schema, std::string_view name) -> Option<type> {
  auto const* root = try_as<record_type>(&schema);
  if (not root) {
    return None{};
  }
  if (auto exact = root->field(name)) {
    return exact;
  }
  auto current = *root;
  auto result = Option<type>{};
  auto const parts = detail::split(name, ".");
  for (auto index = size_t{0}; index < parts.size(); ++index) {
    result = current.field(parts[index]);
    if (not result) {
      return None{};
    }
    if (index + 1 == parts.size()) {
      return result;
    }
    auto const* nested = try_as<record_type>(&*result);
    if (not nested) {
      return None{};
    }
    current = *nested;
  }
  return None{};
}

auto is_integral(type const& value) -> bool {
  return is<int64_type>(value) or is<uint64_type>(value);
}

auto validate_path(type const& schema, std::string_view path,
                   ProjectedKind kind) -> Option<ProjectionError> {
  auto const actual = resolve_type(schema, path);
  if (not actual or is<null_type>(*actual) or kind == ProjectedKind::any) {
    return None{};
  }
  auto valid = false;
  auto expected = std::string_view{"the projected value type"};
  switch (kind) {
    case ProjectedKind::any:
      TENZIR_UNREACHABLE();
    case ProjectedKind::string:
      valid = is<string_type>(*actual);
      expected = "string";
      break;
    case ProjectedKind::integral:
      valid = is_integral(*actual);
      expected = "int64 or uint64";
      break;
    case ProjectedKind::source_identifier:
      valid = is<string_type>(*actual) or is_integral(*actual);
      expected = "string, int64, or uint64";
      break;
  }
  if (valid) {
    return None{};
  }
  return ProjectionError{
    fmt::format("OCSF path `{}` has type `{}`, expected {}", path,
                actual->kind(), expected)};
}

auto validate_string_path(type const& schema, std::string_view path)
  -> Option<ProjectionError> {
  return validate_path(schema, path, ProjectedKind::string);
}

auto validate_fingerprints(type const& schema, std::string_view path)
  -> Option<ProjectionError> {
  auto const hashes = resolve_type(schema, path);
  if (not hashes or is<null_type>(*hashes)) {
    return None{};
  }
  auto const* list = try_as<list_type>(&*hashes);
  if (not list) {
    return ProjectionError{
      fmt::format("OCSF path `{}` must be a list of Fingerprints", path)};
  }
  auto const values = list->value_type();
  auto const* fingerprint = try_as<record_type>(&values);
  if (not fingerprint) {
    return ProjectionError{
      fmt::format("OCSF path `{}` must contain Fingerprint objects", path)};
  }
  if (auto const algorithm = fingerprint->field("algorithm_id");
      algorithm and not is<null_type>(*algorithm)
      and not is_integral(*algorithm)) {
    return ProjectionError{
      "Fingerprint `algorithm_id` must be int64 or uint64"};
  }
  if (auto const algorithm = fingerprint->field("algorithm");
      algorithm and not is<null_type>(*algorithm)
      and not is<string_type>(*algorithm)) {
    return ProjectionError{"Fingerprint `algorithm` must be a string"};
  }
  if (auto const value = fingerprint->field("value");
      value and not is<null_type>(*value) and not is<string_type>(*value)) {
    return ProjectionError{"Fingerprint `value` must be a string"};
  }
  return None{};
}

auto validate_object_list(type const& schema, ObjectListField const& projection)
  -> Option<ProjectionError> {
  auto const objects = resolve_type(schema, projection.path);
  if (not objects or is<null_type>(*objects)) {
    return None{};
  }
  auto const* list = try_as<list_type>(&*objects);
  if (not list) {
    return ProjectionError{
      fmt::format("OCSF path `{}` must be a list of objects", projection.path)};
  }
  auto const value_type = list->value_type();
  auto const* object = try_as<record_type>(&value_type);
  if (not object) {
    return ProjectionError{
      fmt::format("OCSF path `{}` must contain objects", projection.path)};
  }
  auto const member = object->field(projection.member);
  if (not member or is<null_type>(*member) or is<string_type>(*member)) {
    return None{};
  }
  return ProjectionError{
    fmt::format("OCSF path `{}.{}` has type `{}`, expected string",
                projection.path, projection.member, member->kind())};
}

/// Returns the accepted OCSF `os.type_id` values for an expected operating
/// system. Any other known type ID is a contradiction; the IDs 0 (Unknown)
/// and 99 (Other) always stay eligible.
auto accepted_os_types(OsKind os) -> std::span<uint64_t const> {
  static constexpr auto windows_ids = std::array<uint64_t, 2>{100, 101};
  static constexpr auto linux_ids = std::array<uint64_t, 1>{200};
  static constexpr auto macos_ids = std::array<uint64_t, 1>{300};
  switch (os) {
    case OsKind::windows:
      return windows_ids;
    case OsKind::linux_:
      return linux_ids;
    case OsKind::macos:
      return macos_ids;
  }
  TENZIR_UNREACHABLE();
}

/// Classifies a recognized `os.name` value; unknown names stay eligible.
auto classify_os_name(std::string_view name) -> Option<OsKind> {
  auto const normalized = detail::ascii_tolower(name);
  if (normalized == "windows") {
    return OsKind::windows;
  }
  if (normalized == "linux") {
    return OsKind::linux_;
  }
  if (normalized == "macos") {
    return OsKind::macos;
  }
  return None{};
}

auto known_os_contradiction(OsKind os, Option<uint64_t> os_type,
                            Option<std::string_view> os_name) -> bool {
  if (os_type and *os_type != 0 and *os_type != 99) {
    auto const accepted = accepted_os_types(os);
    return std::ranges::find(accepted, *os_type) == accepted.end();
  }
  if (not os_name) {
    return false;
  }
  auto const normalized = detail::ascii_tolower(*os_name);
  if (auto const named = classify_os_name(normalized)) {
    return *named != os;
  }
  // Mobile systems are known contradictions for every catalog OS.
  return normalized == "android" or normalized == "ios"
         or normalized == "ipados";
}

auto classify_source(std::string_view value) -> Option<SourceKind> {
  auto const normalized = detail::ascii_tolower(value);
  if (normalized.contains("sysmon")) {
    return SourceKind::sysmon;
  }
  if (normalized == "security" or normalized == "system"
      or normalized.contains("security-auditing")) {
    return SourceKind::windows_security;
  }
  if (normalized.contains("windows powershell")
      or normalized.contains("powershell/operational")) {
    return SourceKind::powershell;
  }
  if (normalized.contains("windows defender")) {
    return SourceKind::windows_defender;
  }
  if (normalized.contains("windows firewall")) {
    return SourceKind::windows_firewall;
  }
  if (normalized.contains("codeintegrity")) {
    return SourceKind::windows_codeintegrity;
  }
  if (normalized.contains("bits-client")) {
    return SourceKind::windows_bits;
  }
  if (normalized.contains("dns-client")) {
    return SourceKind::windows_dns_client;
  }
  if (normalized.contains("appxdeployment")) {
    return SourceKind::windows_appx;
  }
  if (normalized.contains("zeek")) {
    return SourceKind::zeek;
  }
  if (normalized.contains("suricata")) {
    return SourceKind::suricata;
  }
  if (normalized.contains("okta")) {
    return SourceKind::okta;
  }
  return None{};
}

auto known_source_contradiction(Option<std::string_view> log_name,
                                Option<std::string_view> product_name,
                                SourceKind expected) -> bool {
  auto contradicts = [&](Option<std::string_view> value) {
    auto const actual = value.and_then(classify_source);
    return actual and *actual != expected;
  };
  return contradicts(log_name) or contradicts(product_name);
}

auto contains(std::span<uint64_t const> values, uint64_t value) -> bool {
  return std::ranges::find(values, value) != values.end();
}

auto contains(std::span<std::string const> values, std::string_view value)
  -> bool {
  auto const normalized = detail::ascii_tolower(value);
  return std::ranges::any_of(values, [&](std::string_view candidate) {
    return detail::ascii_tolower(candidate) == normalized;
  });
}

} // namespace

auto find_mappings(tenzir::sigma::LogSource const& log_source)
  -> std::vector<Ref<Mapping const>> {
  auto result = std::vector<Ref<Mapping const>>{};
  for (auto const& mapping : catalog()) {
    auto const& selector = mapping.selector;
    auto const category_matches = selector.category.empty()
                                    ? not log_source.category
                                    : log_source.category == selector.category;
    auto const product_matches = log_source.product == selector.product;
    auto const service_matches
      = selector.optional_service
          ? not log_source.service or log_source.service == selector.service
        : selector.service.empty() ? not log_source.service
                                   : log_source.service == selector.service;
    if (not category_matches or not product_matches or not service_matches) {
      continue;
    }
    // Alternatives for one selector must target distinct OCSF classes so
    // that at most one alternative matches a given event.
    for (auto const existing : result) {
      TENZIR_ASSERT(existing->guard.class_uid != mapping.guard.class_uid,
                    "ambiguous built-in Sigma OCSF mapping");
    }
    result.emplace_back(mapping);
  }
  return result;
}

auto find_mapping(tenzir::sigma::LogSource const& log_source)
  -> Option<Mapping const&> {
  auto const mappings = find_mappings(log_source);
  if (mappings.empty()) {
    return None{};
  }
  return *mappings.front();
}

auto find_field(Mapping const& mapping, std::string_view field)
  -> Option<FieldProjection const&> {
  auto const result
    = std::ranges::find(mapping.fields, field, &FieldDescriptor::sigma_field);
  if (result == mapping.fields.end()) {
    return None{};
  }
  return result->projection;
}

auto make_guard(Mapping const& mapping, bool provenance) -> EvaluationGuard {
  auto event = mapping.guard;
  if (not provenance) {
    // Without a provenance-scoped field, the transported rule is a statement
    // about the OCSF event class, not about one producer. Only semantic
    // constraints remain.
    event.source = None{};
    event.log_name = None{};
    event.conflicting_log_names.clear();
  }
  return EvaluationGuard{.event = std::move(event)};
}

auto is_schema(type const& schema) -> bool {
  auto const version = resolve_type(schema, "metadata.version");
  auto const class_uid = resolve_type(schema, "class_uid");
  return version and is<string_type>(*version) and class_uid
         and is_integral(*class_uid);
}

auto resolve_field(series const& input, std::string_view name) -> series {
  if (auto exact = get_record_field(input, name)) {
    return std::move(*exact);
  }
  auto current = input;
  for (auto const& part : detail::split(name, ".")) {
    auto next = get_record_field(current, part);
    if (not next) {
      return series::null(null_type{}, input.length());
    }
    current = std::move(*next);
  }
  return current;
}

auto resolve_presence(series const& input, std::string_view name) -> series {
  auto builder = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
  check(builder.Reserve(input.length()));
  auto emit_present = [&](series const& parent) {
    auto const records = parent.as<record_type>();
    TENZIR_ASSERT(records);
    for (auto i = int64_t{0}; i < parent.length(); ++i) {
      check(builder.Append(not records->array->IsNull(i)));
    }
  };
  auto emit_absent = [&] {
    for (auto i = int64_t{0}; i < input.length(); ++i) {
      check(builder.Append(false));
    }
  };
  auto has_field = [](series const& value, std::string_view field) {
    auto const records = value.as<record_type>();
    if (not records) {
      return false;
    }
    return std::ranges::any_of(records->type.fields(), [&](auto const& entry) {
      return entry.name == field;
    });
  };
  if (has_field(input, name)) {
    emit_present(input);
    return series{bool_type{}, finish(builder)};
  }
  auto const parts = detail::split(name, ".");
  auto current = input;
  for (auto i = size_t{0}; i + 1 < parts.size(); ++i) {
    auto next = get_record_field(current, parts[i]);
    if (not next) {
      emit_absent();
      return series{bool_type{}, finish(builder)};
    }
    current = std::move(*next);
  }
  if (has_field(current, parts.back())) {
    emit_present(current);
    return series{bool_type{}, finish(builder)};
  }
  emit_absent();
  return series{bool_type{}, finish(builder)};
}

auto validate(type const& schema, std::string_view sigma_field,
              FieldProjection const& projection) -> Option<ProjectionError> {
  return match(
    projection.value,
    [&](LiteralField const&) -> Option<ProjectionError> {
      if (resolve_type(schema, sigma_field)) {
        return None{};
      }
      return ProjectionError{
        fmt::format("field `{}` is absent from the OCSF schema", sigma_field)};
    },
    [&](PathField const& value) -> Option<ProjectionError> {
      return validate_path(schema, value.path, projection.kind);
    },
    [&](FallbackField const& value) -> Option<ProjectionError> {
      if (auto error = validate_string_path(schema, value.primary)) {
        return error;
      }
      return validate_string_path(schema, value.fallback);
    },
    [&](JoinedField const& value) -> Option<ProjectionError> {
      if (auto error = validate_string_path(schema, value.left)) {
        return error;
      }
      return validate_string_path(schema, value.right);
    },
    [&](PrincipalField const& value) -> Option<ProjectionError> {
      if (auto error = validate_string_path(schema, value.domain)) {
        return error;
      }
      return validate_string_path(schema, value.name);
    },
    [&](FingerprintListField const& value) {
      return validate_fingerprints(schema, value.path);
    },
    [&](ObjectListField const& value) {
      return validate_object_list(schema, value);
    });
}

auto validated_paths(std::string_view sigma_field,
                     FieldProjection const& projection)
  -> std::vector<std::string> {
  auto result = std::vector<std::string>{};
  if (is<LiteralField>(projection.value)) {
    result.emplace_back(sigma_field);
    return result;
  }
  for (auto const path : projection_paths(projection.value)) {
    result.emplace_back(path);
  }
  return result;
}

auto schema_shape(type const& schema, std::span<std::string const> paths)
  -> std::string {
  auto result = std::string{};
  for (auto const& path : paths) {
    auto const resolved = resolve_type(schema, path);
    fmt::format_to(std::back_inserter(result), "{}={};", path,
                   resolved ? resolved->make_fingerprint() : "-");
  }
  return result;
}

auto project_rule_value(FieldProjection const& projection, data const& value)
  -> Result<data, std::string> {
  return match(
    projection.rule_value,
    [&](IdentityRuleValue const&) -> Result<data, std::string> {
      return value;
    },
    [&](StringifyRuleValue const&) -> Result<data, std::string> {
      // A null rule value tests for absence; stringify only concrete scalars.
      if (is<caf::none_t>(value) or is<std::string>(value)) {
        return value;
      }
      return data{to_string(value)};
    },
    [&](DictionaryRuleValue const& dictionary) -> Result<data, std::string> {
      // Stock rules spell boolean-valued source fields both as YAML booleans
      // and as strings; the dictionary keys are the lowercase spellings.
      auto name = std::string{};
      if (auto const* str = try_as<std::string>(&value)) {
        name = detail::ascii_tolower(*str);
      } else if (auto const* boolean = try_as<bool>(&value)) {
        name = *boolean ? "true" : "false";
      } else {
        return value;
      }
      auto const entry = std::ranges::find(
        dictionary.entries, name, &std::pair<std::string, int64_t>::first);
      if (entry == dictionary.entries.end()) {
        return Err{fmt::format("unsupported {} `{}`", dictionary.name, name)};
      }
      return data{entry->second};
    });
}

auto projection_paths(FieldValue const& value)
  -> std::vector<std::string_view> {
  return match(
    value,
    [&](LiteralField const&) -> std::vector<std::string_view> {
      return {};
    },
    [&](PathField const& field) -> std::vector<std::string_view> {
      return {field.path};
    },
    [&](FallbackField const& field) -> std::vector<std::string_view> {
      return {field.primary, field.fallback};
    },
    [&](JoinedField const& field) -> std::vector<std::string_view> {
      return {field.left, field.right, field.evidence_path};
    },
    [&](PrincipalField const& field) -> std::vector<std::string_view> {
      return {field.domain, field.name, field.evidence_path};
    },
    [&](FingerprintListField const& field) -> std::vector<std::string_view> {
      return {field.path};
    },
    [&](ObjectListField const& field) -> std::vector<std::string_view> {
      return {field.path};
    });
}

auto reads_free_form_data(std::string_view path) -> bool {
  auto const parts = detail::split(path, ".");
  if (parts.size() < 2) {
    return false;
  }
  return std::ranges::find(parts.begin(), std::prev(parts.end()), "data")
         != std::prev(parts.end());
}

auto constant_evidence_path(FieldValue const& value,
                            std::string_view sigma_field)
  -> Option<std::string> {
  return match(
    value,
    [&](LiteralField const&) -> Option<std::string> {
      return std::string{sigma_field};
    },
    [&](PathField const& field) -> Option<std::string> {
      return field.path;
    },
    [&](FallbackField const&) -> Option<std::string> {
      return None{};
    },
    [&](JoinedField const& field) -> Option<std::string> {
      return field.evidence_path;
    },
    [&](PrincipalField const& field) -> Option<std::string> {
      return field.evidence_path;
    },
    [&](FingerprintListField const& field) -> Option<std::string> {
      return field.path;
    },
    [&](ObjectListField const& field) -> Option<std::string> {
      return fmt::format("{}.{}", field.path, field.member);
    });
}

auto project(series const& input, std::string_view sigma_field,
             FieldProjection const& projection)
  -> Result<ProjectionSeries, ProjectionError> {
  return match(
    projection.value,
    [&](LiteralField const&) -> Result<ProjectionSeries, ProjectionError> {
      return ProjectionSeries{
        resolve_field(input, sigma_field),
        resolve_presence(input, sigma_field),
        None{},
      };
    },
    [&](PathField const& value) -> Result<ProjectionSeries, ProjectionError> {
      auto projected = resolve_field(input, value.path);
      if (projection.kind == ProjectedKind::source_identifier) {
        auto const source = std::move(projected);
        projected = make_string_series(input.length(), [&](int64_t row) {
          return source_identifier_at(source, row);
        });
      }
      return ProjectionSeries{
        std::move(projected),
        resolve_presence(input, value.path),
        None{},
      };
    },
    [&](
      FallbackField const& value) -> Result<ProjectionSeries, ProjectionError> {
      auto fallback
        = resolve_path_fallback(input, value.primary, value.fallback);
      return ProjectionSeries{
        std::move(fallback.value),
        combine_presence(resolve_presence(input, value.primary),
                         resolve_presence(input, value.fallback), false),
        std::move(fallback.path),
      };
    },
    [&](JoinedField const& value) -> Result<ProjectionSeries, ProjectionError> {
      return ProjectionSeries{
        resolve_joined(input, value),
        combine_presence(resolve_presence(input, value.left),
                         resolve_presence(input, value.right), true),
        None{},
      };
    },
    [&](PrincipalField const& value)
      -> Result<ProjectionSeries, ProjectionError> {
      return ProjectionSeries{
        resolve_principal(input, value),
        resolve_presence(input, value.name),
        None{},
      };
    },
    [&](FingerprintListField const& value)
      -> Result<ProjectionSeries, ProjectionError> {
      return ProjectionSeries{
        resolve_fingerprints(input, value.path),
        resolve_presence(input, value.path),
        None{},
      };
    },
    [&](ObjectListField const& value)
      -> Result<ProjectionSeries, ProjectionError> {
      return ProjectionSeries{
        resolve_object_list(input, value),
        resolve_presence(input, value.path),
        None{},
      };
    });
}

auto evaluate_guard(series const& input, EvaluationGuard const& guard)
  -> series {
  auto const class_uids = resolve_field(input, "class_uid");
  auto const activity_ids = resolve_field(input, "activity_id");
  auto const os_types = resolve_field(input, "device.os.type_id");
  auto const os_names = resolve_field(input, "device.os.name");
  auto const log_names = resolve_field(input, "metadata.log_name");
  auto const product_names = resolve_field(input, "metadata.product.name");
  auto builder = arrow::BooleanBuilder{arrow_memory_pool()};
  check(builder.Reserve(input.length()));
  for (auto row = int64_t{0}; row < input.length(); ++row) {
    auto eligible = integer_at(class_uids, row) == guard.event.class_uid;
    // An event with a specific activity outside the eligible set declares
    // itself to be something else; absent, Unknown (0), and Other (99) stay
    // eligible.
    auto const activity = integer_at(activity_ids, row);
    if (eligible and activity and *activity != 0 and *activity != 99) {
      eligible = contains(guard.event.activity_ids, *activity);
    }
    if (eligible and guard.event.os) {
      eligible = not known_os_contradiction(
        *guard.event.os, integer_at(os_types, row), string_at(os_names, row));
    }
    if (eligible and guard.event.source) {
      eligible = not known_source_contradiction(string_at(log_names, row),
                                                string_at(product_names, row),
                                                *guard.event.source);
    }
    if (eligible and guard.event.log_name) {
      auto const actual = string_at(log_names, row);
      if (actual and contains(guard.event.conflicting_log_names, *actual)) {
        eligible = false;
      }
    }
    check(builder.Append(eligible));
  }
  return series{bool_type{}, finish(builder)};
}

} // namespace tenzir::plugins::sigma::ocsf
