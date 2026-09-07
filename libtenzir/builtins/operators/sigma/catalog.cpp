//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// The built-in Sigma OCSF mapping catalog parser. Every mapping family is
// one declarative YAML document under `catalog/` that references the closed
// vocabulary of core transformations declared in `ocsf.hpp`. The build embeds
// the documents via `cmake/TenzirEmbedSigmaCatalog.cmake`. Adding a family
// means adding a document there plus a fixture directory under
// `test/inputs/sigma/families/`; no engine code changes.
//
// The documents are internal for now: the format is not part of the public
// interface and may change without notice until the vocabulary has proven
// itself across more logsource families.

#include <tenzir/data.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/panic.hpp>
#include <tenzir/ref.hpp>
#include <tenzir/try.hpp>

#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <string_view>
#include <vector>

#include "ocsf.hpp"

namespace tenzir::plugins::sigma::ocsf {

namespace generated {

/// Defined in the generated translation unit that embeds the YAML documents
/// under `catalog/`; see `cmake/TenzirEmbedSigmaCatalog.cmake`.
auto embedded_catalog_documents() -> std::span<std::string_view const>;

} // namespace generated

namespace {

template <class... Ts>
auto fail(fmt::format_string<Ts...> str, Ts&&... xs) -> Err<std::string> {
  return Err{fmt::format(str, std::forward<Ts>(xs)...)};
}

using ParseResult = Result<Mapping, std::string>;

auto find_value(record const& object, std::string_view key) -> data const* {
  auto const entry = object.find(key);
  if (entry == object.end()) {
    return nullptr;
  }
  return &entry->second;
}

auto require_record(record const& object, std::string_view key)
  -> Result<Ref<record const>, std::string> {
  auto const* value = find_value(object, key);
  if (not value) {
    return fail("missing `{}` section", key);
  }
  auto const* result = try_as<record>(value);
  if (not result) {
    return fail("`{}` must be a record", key);
  }
  return Ref{*result};
}

auto require_string(record const& object, std::string_view key)
  -> Result<std::string, std::string> {
  auto const* value = find_value(object, key);
  if (not value) {
    return fail("missing `{}` key", key);
  }
  auto const* result = try_as<std::string>(value);
  if (not result) {
    return fail("`{}` must be a string", key);
  }
  if (result->empty()) {
    return fail("`{}` must not be empty", key);
  }
  return *result;
}

auto optional_string(record const& object, std::string_view key)
  -> Result<Option<std::string>, std::string> {
  auto const* value = find_value(object, key);
  if (not value) {
    return Option<std::string>{};
  }
  auto const* result = try_as<std::string>(value);
  if (not result or result->empty()) {
    return fail("`{}` must be a non-empty string", key);
  }
  return Option<std::string>{*result};
}

auto optional_bool(record const& object, std::string_view key)
  -> Result<bool, std::string> {
  auto const* value = find_value(object, key);
  if (not value) {
    return false;
  }
  auto const* result = try_as<bool>(value);
  if (not result) {
    return fail("`{}` must be a boolean", key);
  }
  return *result;
}

auto require_uint(data const& value, std::string_view key)
  -> Result<uint64_t, std::string> {
  if (auto const* result = try_as<int64_t>(&value)) {
    if (*result < 0) {
      return fail("`{}` must not be negative", key);
    }
    return static_cast<uint64_t>(*result);
  }
  if (auto const* result = try_as<uint64_t>(&value)) {
    return *result;
  }
  return fail("`{}` must be an unsigned integer", key);
}

auto require_uints(record const& object, std::string_view key)
  -> Result<std::vector<uint64_t>, std::string> {
  auto const* value = find_value(object, key);
  if (not value) {
    return fail("missing `{}` key", key);
  }
  auto const* elements = try_as<list>(value);
  if (not elements) {
    return fail("`{}` must be a list", key);
  }
  auto result = std::vector<uint64_t>{};
  result.reserve(elements->size());
  for (auto const& element : *elements) {
    TRY(auto number, require_uint(element, key));
    result.push_back(number);
  }
  return result;
}

auto optional_strings(record const& object, std::string_view key)
  -> Result<std::vector<std::string>, std::string> {
  auto const* value = find_value(object, key);
  if (not value) {
    return std::vector<std::string>{};
  }
  auto const* elements = try_as<list>(value);
  if (not elements) {
    return fail("`{}` must be a list", key);
  }
  auto result = std::vector<std::string>{};
  result.reserve(elements->size());
  for (auto const& element : *elements) {
    auto const* text = try_as<std::string>(&element);
    if (not text or text->empty()) {
      return fail("`{}` must contain non-empty strings", key);
    }
    result.push_back(*text);
  }
  return result;
}

auto check_keys(record const& object, std::string_view section,
                std::span<std::string_view const> allowed)
  -> Result<void, std::string> {
  for (auto const& [key, value] : object) {
    TENZIR_UNUSED(value);
    if (std::ranges::find(allowed, key) == allowed.end()) {
      return fail("unknown key `{}` in `{}`", key, section);
    }
  }
  return {};
}

auto parse_source(std::string_view name) -> Result<SourceKind, std::string> {
  if (auto result = from_string<SourceKind>(name)) {
    return *result;
  }
  return fail("unknown source kind `{}`", name);
}

auto parse_kind(record const& spec) -> Result<ProjectedKind, std::string> {
  TRY(auto kind, optional_string(spec, "kind"));
  if (not kind or *kind == "any") {
    return ProjectedKind::any;
  }
  if (*kind == "string") {
    return ProjectedKind::string;
  }
  if (*kind == "integral") {
    return ProjectedKind::integral;
  }
  if (*kind == "source-identifier") {
    return ProjectedKind::source_identifier;
  }
  return fail("unknown projected kind `{}`", *kind);
}

auto parse_rule_value(record const& spec) -> Result<RuleValue, std::string> {
  auto const* value = find_value(spec, "rule-value");
  if (not value) {
    return RuleValue{IdentityRuleValue{}};
  }
  if (auto const* name = try_as<std::string>(value)) {
    if (*name == "identity") {
      return RuleValue{IdentityRuleValue{}};
    }
    if (*name == "stringify") {
      return RuleValue{StringifyRuleValue{}};
    }
    return fail("unknown rule-value `{}`", *name);
  }
  auto const* object = try_as<record>(value);
  if (not object) {
    return fail("`rule-value` must be a string or a record");
  }
  static constexpr auto allowed = std::array<std::string_view, 1>{
    "dictionary",
  };
  TRY(check_keys(*object, "rule-value", allowed));
  TRY(auto dictionary, require_record(*object, "dictionary"));
  static constexpr auto dictionary_keys = std::array<std::string_view, 2>{
    "name",
    "entries",
  };
  TRY(check_keys(*dictionary, "rule-value.dictionary", dictionary_keys));
  auto result = DictionaryRuleValue{};
  TRY(result.name, require_string(*dictionary, "name"));
  TRY(auto entries, require_record(*dictionary, "entries"));
  if (entries->empty()) {
    return fail("`rule-value.dictionary.entries` must not be empty");
  }
  for (auto const& [name, mapped] : *entries) {
    if (name.empty()) {
      return fail("dictionary keys must not be empty");
    }
    if (detail::ascii_tolower(name) != name) {
      return fail("dictionary key `{}` must be lowercase", name);
    }
    auto number = Option<int64_t>{};
    if (auto const* signed_value = try_as<int64_t>(&mapped)) {
      number = *signed_value;
    } else if (auto const* unsigned_value = try_as<uint64_t>(&mapped)) {
      number = static_cast<int64_t>(*unsigned_value);
    }
    if (not number) {
      return fail("dictionary entry `{}` must map to an integer", name);
    }
    result.entries.emplace_back(name, *number);
  }
  return RuleValue{std::move(result)};
}

auto parse_field_value(record const& spec, std::string_view field)
  -> Result<FieldValue, std::string> {
  TRY(auto primitive, require_string(spec, "primitive"));
  if (primitive == "literal") {
    return FieldValue{LiteralField{}};
  }
  if (primitive == "path") {
    auto result = PathField{};
    TRY(result.path, require_string(spec, "path"));
    return FieldValue{std::move(result)};
  }
  if (primitive == "fallback") {
    auto result = FallbackField{};
    TRY(result.primary, require_string(spec, "primary"));
    TRY(result.fallback, require_string(spec, "fallback"));
    return FieldValue{std::move(result)};
  }
  if (primitive == "join") {
    auto result = JoinedField{};
    TRY(result.left, require_string(spec, "left"));
    TRY(result.right, require_string(spec, "right"));
    TRY(result.separator, require_string(spec, "separator"));
    TRY(result.evidence_path, require_string(spec, "evidence"));
    return FieldValue{std::move(result)};
  }
  if (primitive == "principal") {
    auto result = PrincipalField{};
    TRY(result.domain, require_string(spec, "domain"));
    TRY(result.name, require_string(spec, "name"));
    TRY(result.evidence_path, require_string(spec, "evidence"));
    return FieldValue{std::move(result)};
  }
  if (primitive == "fingerprints") {
    auto result = FingerprintListField{};
    TRY(result.path, require_string(spec, "path"));
    return FieldValue{std::move(result)};
  }
  if (primitive == "list-member") {
    auto result = ObjectListField{};
    TRY(result.path, require_string(spec, "path"));
    TRY(result.member, require_string(spec, "member"));
    return FieldValue{std::move(result)};
  }
  return fail("field `{}` references unknown primitive `{}`", field, primitive);
}

auto field_keys(FieldValue const& value) -> std::vector<std::string_view> {
  auto result = std::vector<std::string_view>{
    "primitive", "kind", "rule-value", "notes", "provenance-scoped",
  };
  match(
    value, [&](LiteralField const&) {},
    [&](PathField const&) {
      result.push_back("path");
    },
    [&](FallbackField const&) {
      result.push_back("primary");
      result.push_back("fallback");
    },
    [&](JoinedField const&) {
      result.push_back("left");
      result.push_back("right");
      result.push_back("separator");
      result.push_back("evidence");
    },
    [&](PrincipalField const&) {
      result.push_back("domain");
      result.push_back("name");
      result.push_back("evidence");
    },
    [&](FingerprintListField const&) {
      result.push_back("path");
    },
    [&](ObjectListField const&) {
      result.push_back("path");
      result.push_back("member");
    });
  return result;
}

auto parse_field(std::string_view field, record const& spec)
  -> Result<FieldDescriptor, std::string> {
  auto result = FieldDescriptor{};
  result.sigma_field = std::string{field};
  TRY(result.projection.value, parse_field_value(spec, field));
  TRY(check_keys(spec, fmt::format("fields.{}", field),
                 field_keys(result.projection.value)));
  TRY(result.projection.rule_value, parse_rule_value(spec));
  TRY(result.projection.kind, parse_kind(spec));
  TRY(result.projection.provenance_scoped,
      optional_bool(spec, "provenance-scoped"));
  TRY(auto notes, optional_string(spec, "notes"));
  result.notes = notes.value_or("");
  return result;
}

auto parse_unmapped(record const& object)
  -> Result<std::vector<UnmappedField>, std::string> {
  auto const* value = find_value(object, "unmapped");
  if (not value) {
    return std::vector<UnmappedField>{};
  }
  auto const* elements = try_as<list>(value);
  if (not elements) {
    return fail("`unmapped` must be a list");
  }
  auto result = std::vector<UnmappedField>{};
  result.reserve(elements->size());
  for (auto const& element : *elements) {
    auto const* spec = try_as<record>(&element);
    if (not spec) {
      return fail("`unmapped` entries must be records");
    }
    static constexpr auto allowed = std::array<std::string_view, 2>{
      "field",
      "reason",
    };
    TRY(check_keys(*spec, "unmapped", allowed));
    auto entry = UnmappedField{};
    TRY(entry.field, require_string(*spec, "field"));
    TRY(entry.reason, require_string(*spec, "reason"));
    result.push_back(std::move(entry));
  }
  return result;
}

auto parse_selector(record const& object)
  -> Result<LogsourceSelector, std::string> {
  static constexpr auto allowed = std::array<std::string_view, 4>{
    "category",
    "product",
    "service",
    "optional-service",
  };
  TRY(check_keys(object, "logsource", allowed));
  auto result = LogsourceSelector{};
  TRY(auto category, optional_string(object, "category"));
  result.category = category.value_or("");
  TRY(result.product, require_string(object, "product"));
  TRY(auto service, optional_string(object, "service"));
  result.service = service.value_or("");
  TRY(result.optional_service, optional_bool(object, "optional-service"));
  if (result.optional_service and result.service.empty()) {
    return fail("`optional-service` requires a `service`");
  }
  return result;
}

auto parse_guard(record const& object) -> Result<EventGuard, std::string> {
  static constexpr auto allowed = std::array<std::string_view, 5>{
    "class-uid", "activities", "source", "log-name", "conflicting-log-names",
  };
  TRY(check_keys(object, "guard", allowed));
  auto result = EventGuard{};
  auto const* class_uid = find_value(object, "class-uid");
  if (not class_uid) {
    return fail("missing `class-uid` key");
  }
  TRY(result.class_uid, require_uint(*class_uid, "class-uid"));
  if (result.class_uid == 0) {
    return fail("`class-uid` must not be zero");
  }
  TRY(result.activity_ids, require_uints(object, "activities"));
  if (result.activity_ids.empty()) {
    return fail("`activities` must not be empty");
  }
  for (auto const activity : result.activity_ids) {
    if (activity == 0 or activity == 99) {
      return fail("`activities` must not list 0 (Unknown) or 99 (Other); "
                  "both are always eligible");
    }
  }
  TRY(auto source, optional_string(object, "source"));
  if (source) {
    TRY(auto kind, parse_source(*source));
    result.source = kind;
  }
  TRY(result.log_name, optional_string(object, "log-name"));
  TRY(result.conflicting_log_names,
      optional_strings(object, "conflicting-log-names"));
  if (not result.conflicting_log_names.empty() and not result.log_name) {
    return fail("`conflicting-log-names` requires a `log-name`");
  }
  return result;
}

} // namespace

auto catalog_documents() -> std::span<std::string_view const> {
  return generated::embedded_catalog_documents();
}

auto parse_mapping(std::string_view yaml) -> ParseResult {
  auto parsed = from_yaml(yaml);
  if (not parsed) {
    return fail("invalid YAML: {}", parsed.error());
  }
  auto const* document = try_as<record>(&*parsed);
  if (not document) {
    return fail("mapping document must be a record");
  }
  static constexpr auto allowed = std::array<std::string_view, 6>{
    "id", "title", "logsource", "guard", "fields", "unmapped",
  };
  TRY(check_keys(*document, "mapping", allowed));
  auto result = Mapping{};
  TRY(result.id, require_string(*document, "id"));
  TRY(result.title, require_string(*document, "title"));
  TRY(result.unmapped, parse_unmapped(*document));
  TRY(auto selector, require_record(*document, "logsource"));
  TRY(result.selector, parse_selector(*selector));
  TRY(auto guard, require_record(*document, "guard"));
  TRY(result.guard, parse_guard(*guard));
  // A logsource product that names an operating system implies an OS guard:
  // the event must not identify a known other system. Products such as
  // `zeek` or `aws` imply nothing.
  if (result.selector.product == "windows") {
    result.guard.os = OsKind::windows;
  } else if (result.selector.product == "linux") {
    result.guard.os = OsKind::linux_;
  } else if (result.selector.product == "macos") {
    result.guard.os = OsKind::macos;
  }
  TRY(auto fields, require_record(*document, "fields"));
  if (fields->empty()) {
    return fail("`fields` must not be empty");
  }
  result.fields.reserve(fields->size());
  for (auto const& [field, spec] : *fields) {
    auto const* object = try_as<record>(&spec);
    if (not object) {
      return fail("field `{}` must be a record", field);
    }
    auto descriptor = parse_field(field, *object);
    if (descriptor.is_err()) {
      return fail("field `{}`: {}", field, std::move(descriptor).unwrap_err());
    }
    result.fields.push_back(std::move(descriptor).unwrap());
  }
  return result;
}

auto catalog() -> std::span<Mapping const> {
  static const auto result = [] {
    auto mappings = std::vector<Mapping>{};
    auto const documents = catalog_documents();
    mappings.reserve(documents.size());
    for (auto const document : documents) {
      auto mapping = parse_mapping(document);
      if (mapping.is_err()) {
        panic("invalid built-in Sigma OCSF mapping: {}",
              std::move(mapping).unwrap_err());
      }
      mappings.push_back(std::move(mapping).unwrap());
    }
    return mappings;
  }();
  return result;
}

} // namespace tenzir::plugins::sigma::ocsf
