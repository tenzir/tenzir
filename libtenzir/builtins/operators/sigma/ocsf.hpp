//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/data.hpp>
#include <tenzir/detail/enum.hpp>
#include <tenzir/option.hpp>
#include <tenzir/ref.hpp>
#include <tenzir/result.hpp>
#include <tenzir/series.hpp>
#include <tenzir/sigma.hpp>
#include <tenzir/type.hpp>
#include <tenzir/variant.hpp>

#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace tenzir::plugins::sigma::ocsf {

// The types in this file form a closed vocabulary of core transformations
// ("primitives"). The built-in mapping catalog references them by name from
// declarative YAML documents in `catalog.cpp`. Extend the vocabulary only when
// a new logsource family requires a projection that no existing primitive
// expresses; every extension needs dedicated unit tests in
// `libtenzir/test/sigma_ocsf.cpp`.

/// Keeps the Sigma field as a literal path in the OCSF event.
struct LiteralField {
  friend auto operator==(LiteralField const&, LiteralField const&) -> bool
    = default;
};

/// Projects one OCSF path directly.
struct PathField {
  std::string path;

  friend auto operator==(PathField const&, PathField const&) -> bool = default;
};

/// Selects the first populated string from two ordered OCSF paths.
struct FallbackField {
  std::string primary;
  std::string fallback;

  friend auto operator==(FallbackField const&, FallbackField const&) -> bool
    = default;
};

/// Reconstructs a source field by joining two required string paths.
struct JoinedField {
  std::string left;
  std::string right;
  std::string separator;
  std::string evidence_path;

  friend auto operator==(JoinedField const&, JoinedField const&) -> bool
    = default;
};

/// Reconstructs a source principal from its domain and name.
struct PrincipalField {
  std::string domain;
  std::string name;
  std::string evidence_path;

  friend auto operator==(PrincipalField const&, PrincipalField const&) -> bool
    = default;
};

/// Reconstructs source-style `ALGORITHM=value` fingerprint lists.
struct FingerprintListField {
  std::string path;

  friend auto
  operator==(FingerprintListField const&, FingerprintListField const&) -> bool
    = default;
};

/// Extracts one member from every object in an OCSF list.
struct ObjectListField {
  std::string path;
  std::string member;

  friend auto operator==(ObjectListField const&, ObjectListField const&) -> bool
    = default;
};

using FieldValue
  = variant<LiteralField, PathField, FallbackField, JoinedField, PrincipalField,
            FingerprintListField, ObjectListField>;

/// Keeps the rule-side value unchanged.
struct IdentityRuleValue {
  friend auto operator==(IdentityRuleValue const&, IdentityRuleValue const&)
    -> bool
    = default;
};

/// Renders the rule-side value as a string before matching.
struct StringifyRuleValue {
  friend auto operator==(StringifyRuleValue const&, StringifyRuleValue const&)
    -> bool
    = default;
};

/// Translates rule-side names into projected values via a lookup table. The
/// lookup normalizes the rule value to lowercase ASCII before matching, so
/// dictionary keys must be lowercase.
struct DictionaryRuleValue {
  /// Human-readable description of the value domain for diagnostics.
  std::string name;
  std::vector<std::pair<std::string, int64_t>> entries;

  friend auto operator==(DictionaryRuleValue const&, DictionaryRuleValue const&)
    -> bool
    = default;
};

using RuleValue
  = variant<IdentityRuleValue, StringifyRuleValue, DictionaryRuleValue>;

enum class ProjectedKind {
  any,
  string,
  integral,
  source_identifier,
};

struct FieldProjection {
  FieldValue value;
  RuleValue rule_value = IdentityRuleValue{};
  ProjectedKind kind = ProjectedKind::any;
  /// Whether the projected value lives in a source-scoped namespace, such as
  /// Sysmon event IDs in `metadata.event_code`. A rule that uses such a field
  /// requires producer provenance; all other rules match any conformant
  /// producer of the event class.
  bool provenance_scoped = false;

  friend auto operator==(FieldProjection const&, FieldProjection const&) -> bool
    = default;
};

struct FieldDescriptor {
  std::string sigma_field;
  FieldProjection projection;
  /// Human-readable matching notes; used only for generated documentation.
  std::string notes;
};

/// Matches one exact default-taxonomy logsource. An empty category requires
/// the rule to omit it. `optional_service` lets a service-specific mapping
/// also cover the category-only form used by many stock Sigma rules.
struct LogsourceSelector {
  std::string category;
  std::string product;
  std::string service;
  bool optional_service = false;
};

/// An operating system that a Sigma logsource product implies. The guard
/// rejects events whose `device.os` identifies a known other system. The
/// trailing underscore avoids the `linux` macro that GCC predefines in GNU
/// mode; string conversion drops it.
TENZIR_ENUM(OsKind, windows, linux_, macos);

/// A recognized event producer used to reject known provenance conflicts.
TENZIR_ENUM(SourceKind, sysmon, windows_security, powershell, windows_defender,
            windows_firewall, windows_codeintegrity, windows_bits,
            windows_dns_client, windows_appx, zeek, suricata, okta);

/// Describes the OCSF event family and provenance eligible for a mapping.
struct EventGuard {
  uint64_t class_uid = 0;
  /// The activity IDs the Sigma category means. An event with a different
  /// specific activity declares itself to be something else and is rejected;
  /// an absent activity, `0` (Unknown), and `99` (Other) stay eligible.
  std::vector<uint64_t> activity_ids;
  /// Derived from the logsource selector's product; never set in a catalog
  /// document.
  Option<OsKind> os;
  Option<SourceKind> source;
  Option<std::string> log_name;
  std::vector<std::string> conflicting_log_names;

  friend auto operator==(EventGuard const&, EventGuard const&) -> bool
    = default;
};

/// A source field that the OCSF mapping provably cannot preserve. Rules
/// using it are skipped; the reason feeds diagnostics and documentation.
struct UnmappedField {
  std::string field;
  std::string reason;
};

struct Mapping {
  std::string id;
  std::string title;
  LogsourceSelector selector;
  EventGuard guard;
  std::vector<FieldDescriptor> fields;
  std::vector<UnmappedField> unmapped;
};

struct ProjectionError {
  std::string message;
};

struct ProjectionSeries {
  series value;
  series presence;
  /// Per-row evidence paths; only produced by projections whose evidence
  /// path varies per row, such as fallbacks. All other projections have a
  /// compile-time constant path available via `constant_evidence_path`.
  Option<series> evidence_path;
};

struct EvaluationGuard {
  EventGuard event;
};

/// Returns the built-in YAML mapping documents in deterministic order.
auto catalog_documents() -> std::span<std::string_view const>;

/// Parses one declarative YAML mapping document into a semantic mapping.
auto parse_mapping(std::string_view yaml) -> Result<Mapping, std::string>;

/// Returns all built-in semantic mapping families in deterministic order.
auto catalog() -> std::span<Mapping const>;

/// Resolves all semantic mapping alternatives for a Sigma logsource in
/// deterministic catalog order. Alternatives share a selector but target
/// distinct OCSF classes.
auto find_mappings(tenzir::sigma::LogSource const& log_source)
  -> std::vector<Ref<Mapping const>>;

/// Resolves the first semantic mapping alternative for a Sigma logsource.
auto find_mapping(tenzir::sigma::LogSource const& log_source)
  -> Option<Mapping const&>;

/// Looks up one source field in a semantic mapping family.
auto find_field(Mapping const& mapping, std::string_view field)
  -> Option<FieldProjection const&>;

/// Produces the guard applied for a rule and selected mapping family. The
/// producer-provenance constraints apply only when the rule uses a
/// provenance-scoped field; class, activity, and operating-system
/// constraints are semantic and always apply.
auto make_guard(Mapping const& mapping, bool provenance) -> EvaluationGuard;

/// Returns whether a schema has the structural OCSF discriminator fields.
auto is_schema(type const& schema) -> bool;

/// Resolves a Sigma field with exact-key precedence over nested traversal.
auto resolve_field(series const& input, std::string_view name) -> series;

/// Resolves projection-specific source presence with the same precedence.
auto resolve_presence(series const& input, std::string_view name) -> series;

/// Lists every OCSF path a projection reads; empty for literal fields.
auto projection_paths(FieldValue const& value) -> std::vector<std::string_view>;

/// Returns whether a path reads a member of one of OCSF's free-form `data`
/// objects, such as `entity.data.action_id`. Such members are producer
/// payloads, so a projection reading one must be provenance-scoped. A leaf
/// attribute named `data`, such as `reg_value.data`, is a real attribute.
auto reads_free_form_data(std::string_view path) -> bool;

/// Returns the compile-time constant evidence path of a projection, or none
/// when the path varies per row.
auto constant_evidence_path(FieldValue const& value,
                            std::string_view sigma_field)
  -> Option<std::string>;

/// Validates one projection against the concrete table-slice schema.
auto validate(type const& schema, std::string_view sigma_field,
              FieldProjection const& projection) -> Option<ProjectionError>;

/// Lists the schema paths whose types decide the outcome of `validate` for a
/// projection: the OCSF paths it reads, or the Sigma field itself for a
/// literal projection.
auto validated_paths(std::string_view sigma_field,
                     FieldProjection const& projection)
  -> std::vector<std::string>;

/// Renders the types a schema has at the given paths, resolved exactly as
/// `validate` resolves them, with absent paths marked. Two schemas with the
/// same shape validate every projection over those paths identically, so the
/// shape keys compiled plans without regard to fields no rule reads.
auto schema_shape(type const& schema, std::span<std::string const> paths)
  -> std::string;

/// Converts a rule-side value into the semantic projection representation.
auto project_rule_value(FieldProjection const& projection, data const& value)
  -> Result<data, std::string>;

/// Computes one semantic value, presence, and evidence-path series.
auto project(series const& input, std::string_view sigma_field,
             FieldProjection const& projection)
  -> Result<ProjectionSeries, ProjectionError>;

/// Computes one vectorized event guard for a table slice.
auto evaluate_guard(series const& input, EvaluationGuard const& guard)
  -> series;

} // namespace tenzir::plugins::sigma::ocsf
