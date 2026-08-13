//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/box.hpp"
#include "tenzir/data.hpp"
#include "tenzir/detail/stable_map.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/option.hpp"
#include "tenzir/result.hpp"
#include "tenzir/variant.hpp"

#include <string>
#include <string_view>
#include <vector>

/// A typed intermediate representation for Sigma documents.
///
/// Parsing proceeds in phases: YAML data becomes a `Document`, which is
/// validated (required fields, typed values, modifier chains, condition
/// references, and the declared specification major) before any lowering to
/// executable TQL happens. The IR preserves Sigma structure instead of
/// producing expressions immediately.
namespace tenzir::sigma {

/// The specification major that documents without an explicit `sigma-version`
/// resolve to. Existing rules are never silently reinterpreted.
constexpr auto version_floor_major = 2;

/// The highest specification major this implementation supports.
constexpr auto supported_major = 2;

/// A Sigma field selector. Under major 2, the raw name is interpreted at
/// lowering time, with dots denoting (potentially nested) field access.
struct FieldPath {
  std::string raw;

  friend auto operator==(FieldPath const&, FieldPath const&) -> bool = default;
};

/// One `field|modifiers: value(s)` entry of a detection, or a keyword
/// predicate without a field.
struct DetectionItem {
  enum class ItemKind {
    /// A predicate over a named field.
    field,
    /// A keyword predicate that applies to every string-valued leaf of the
    /// event, including strings inside nested records and lists.
    keyword,
  };

  ItemKind kind = ItemKind::field;
  FieldPath field;
  /// Validated modifier chain in declaration order.
  std::vector<std::string> modifiers;
  /// One or more typed Sigma values. Lists of values are OR-linked unless the
  /// `all` modifier is present.
  std::vector<data> values;
  /// Whether the values came from a YAML list (as opposed to a scalar).
  bool value_is_list = false;
};

/// A named search identifier. The outer vector is a disjunction (the YAML
/// list-of-maps form); each inner vector is a conjunction of items (one YAML
/// map).
struct Detection {
  std::vector<std::vector<DetectionItem>> groups;
};

struct Condition;
using ConditionPtr = Box<Condition>;

/// A reference to a named detection. May contain `*` wildcards, in which case
/// all matching identifiers are AND-linked at lowering time.
struct Identifier {
  std::string name;
};

enum class Quantifier {
  one, ///< `1 of ...`
  all, ///< `all of ...`
};

/// A quantified selector: `1 of x`, `all of them`, `1 of selection_*`, ...
struct Quantified {
  Quantifier quantifier = Quantifier::one;
  /// The search-identifier pattern; `them` selects every identifier.
  std::string pattern;
  bool all_identifiers = false;
};

struct Negation {
  ConditionPtr operand;
};

struct Conjunction {
  ConditionPtr left;
  ConditionPtr right;
};

struct Disjunction {
  ConditionPtr left;
  ConditionPtr right;
};

/// A parsed `condition` expression tree.
struct Condition {
  variant<Identifier, Quantified, Negation, Conjunction, Disjunction> node;
};

/// The rule's abstract log source. Participates in filter compatibility and
/// reference resolution; it does not filter input events.
struct LogSource {
  Option<std::string> category;
  Option<std::string> product;
  Option<std::string> service;
};

/// Identity and descriptive metadata of a rule.
struct RuleMetadata {
  Option<std::string> id;
  Option<std::string> name;
  Option<std::string> title;
  Option<std::string> level;
  /// The complete original document for lossless retention.
  record raw;
};

/// A Sigma detection rule.
struct DetectionRule {
  RuleMetadata metadata;
  LogSource log_source;
  /// Named detections in declaration order.
  detail::stable_map<std::string, Detection> detections;
  /// The parsed conditions. Sigma allows a list-valued `condition` whose
  /// entries are OR-linked queries; a plain string produces one entry.
  std::vector<Condition> conditions;
};

/// The target rules of a global filter, referenced by `id` or `name`, or
/// `any` for every logsource-compatible rule.
struct FilterTargets {
  bool any = false;
  /// Rule `id`s or `name`s, in declaration order.
  std::vector<std::string> rules;
};

/// A Sigma global filter rule. Its condition is added conjunctively to every
/// resolved target rule, scoped by the filter's mandatory `logsource`.
struct FilterRule {
  RuleMetadata metadata;
  LogSource log_source;
  FilterTargets targets;
  /// Named detections of the `filter` section in declaration order.
  detail::stable_map<std::string, Detection> detections;
  /// The parsed filter condition.
  Condition condition;
};

/// A Sigma correlation rule. Not yet supported; parsing preserves the raw
/// document and validation rejects it with an actionable diagnostic.
struct CorrelationRule {
  record raw;
};

/// One parsed Sigma YAML document.
struct Document {
  /// The resolved specification major.
  int major = version_floor_major;
  variant<DetectionRule, FilterRule, CorrelationRule> content;
};

/// Parses a `condition` string into an expression tree.
auto parse_condition(std::string_view condition)
  -> Result<Condition, diagnostic>;

/// Parses and validates one YAML document into the typed IR.
///
/// This resolves `sigma-version`, classifies the document as detection,
/// filter, or correlation rule, validates required fields, typed values, and
/// modifier chains, parses the condition, and checks that every exact
/// condition reference resolves to a named detection.
auto parse_document(data const& yaml) -> Result<Document, diagnostic>;

/// Returns whether a filter's log source is compatible with a target rule:
/// every classifier present in the filter must have the same value in the
/// target, while the target may be more specific.
auto compatible(LogSource const& filter, LogSource const& target) -> bool;

/// Applies a global filter to a detection rule by injecting the filter's
/// detections under a collision-free `_filt_<ordinal>_...` prefix and
/// AND-linking the rewritten filter condition to every rule condition.
auto apply_filter(DetectionRule rule, FilterRule const& filter, size_t ordinal)
  -> DetectionRule;

/// Matches a search-identifier pattern with `*` wildcards against a name.
auto wildcard_match(std::string_view pattern, std::string_view name) -> bool;

/// Matches a search-identifier pattern against a name with Sigma's reserved
/// underscore convention: identifiers beginning with `_` are only matched by
/// patterns that themselves begin with `_`.
auto pattern_matches(std::string_view pattern, std::string_view name) -> bool;

} // namespace tenzir::sigma
