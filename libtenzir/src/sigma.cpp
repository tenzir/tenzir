//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/sigma.hpp"

#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/variant_traits.hpp"

#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <cmath>
#include <limits>
#include <string_view>
#include <tuple>

namespace tenzir::sigma {

namespace {

template <class... Ts>
auto parse_failure(fmt::format_string<Ts...> str, Ts&&... xs)
  -> Err<diagnostic> {
  return Err{diagnostic::error(str, std::forward<Ts>(xs)...).done()};
}

/// All standard v2.1 modifiers, including `re` sub-modifiers.
constexpr auto known_modifiers = std::array<std::string_view, 31>{
  "all",    "lt",       "lte",        "gt",           "gte",
  "neq",    "contains", "startswith", "endswith",     "exists",
  "cased",  "windash",  "base64",     "base64offset", "utf16le",
  "wide",   "utf16be",  "utf16",      "re",           "i",
  "m",      "s",        "expand",     "fieldref",     "cidr",
  "minute", "hour",     "day",        "week",         "month",
  "year",
};

constexpr auto comparison_modifiers
  = std::array<std::string_view, 4>{"lt", "lte", "gt", "gte"};

constexpr auto time_part_modifiers = std::array<std::string_view, 6>{
  "minute", "hour", "day", "week", "month", "year",
};

constexpr auto string_transform_modifiers = std::array<std::string_view, 5>{
  "contains", "startswith", "endswith", "windash", "re",
};

auto is_utf16_modifier(std::string_view modifier) -> bool {
  return modifier == "utf16le" or modifier == "wide" or modifier == "utf16be"
         or modifier == "utf16";
}

/// Validates the complete modifier chain of one detection item: known names,
/// ordering constraints, type constraints, and conflicting combinations.
auto validate_modifiers(std::string_view field,
                        std::vector<std::string> const& modifiers,
                        std::vector<data> const& values, bool value_is_list)
  -> Result<void, diagnostic> {
  auto count = [&](auto&& predicate) {
    return std::ranges::count_if(modifiers, predicate);
  };
  auto has = [&](std::string_view name) {
    return std::ranges::contains(modifiers, name);
  };
  for (auto i = size_t{0}; i < modifiers.size(); ++i) {
    auto const& modifier = modifiers[i];
    if (not std::ranges::contains(known_modifiers, modifier)) {
      return Err{diagnostic::error("unknown Sigma modifier `{}` for field `{}`",
                                   modifier, field)
                   .hint("unsupported modifiers reject the rule; nothing is "
                         "silently ignored")
                   .done()};
    }
    // `i`, `m`, and `s` are sub-modifiers of `re` and must directly follow
    // `re` or another `re` sub-modifier.
    if (modifier == "i" or modifier == "m" or modifier == "s") {
      auto const valid
        = i > 0
          and (modifiers[i - 1] == "re" or modifiers[i - 1] == "i"
               or modifiers[i - 1] == "m" or modifiers[i - 1] == "s");
      if (not valid) {
        return parse_failure("Sigma modifier `{}` is a sub-modifier of `re` "
                             "and must directly follow it (field `{}`)",
                             modifier, field);
      }
    }
    // UTF-16 modifiers transform values into raw bytes; an immediately
    // following Base64 modifier is required to produce a matchable string.
    // Wildcard modifiers must follow the complete encoding chain because
    // lowering applies them to the encoded result.
    if (is_utf16_modifier(modifier)) {
      auto const followed = i + 1 < modifiers.size()
                            and (modifiers[i + 1] == "base64"
                                 or modifiers[i + 1] == "base64offset");
      if (not followed) {
        return parse_failure("Sigma modifier `{}` must be immediately followed "
                             "by `base64` or `base64offset` (field `{}`)",
                             modifier, field);
      }
    }
    if (std::ranges::contains(string_transform_modifiers, modifier)) {
      auto const encoding_follows = std::ranges::any_of(
        modifiers.begin() + detail::narrow<ptrdiff_t>(i + 1), modifiers.end(),
        [](std::string const& next) {
          return next == "base64" or next == "base64offset"
                 or is_utf16_modifier(next);
        });
      if (encoding_follows) {
        return parse_failure("Sigma modifier `{}` must follow encoding "
                             "modifiers (field `{}`)",
                             modifier, field);
      }
    }
  }
  // `exists` must be the only modifier and takes a single boolean.
  if (has("exists")) {
    if (modifiers.size() != 1) {
      return parse_failure("Sigma modifier `exists` cannot be combined with "
                           "other modifiers (field `{}`)",
                           field);
    }
    if (value_is_list or values.size() != 1 or not is<bool>(values[0])) {
      return parse_failure("Sigma modifier `exists` requires a single "
                           "boolean value (field `{}`)",
                           field);
    }
  }
  // At most one numeric comparison, and `neq` conflicts with them.
  auto const comparisons = count([](std::string const& modifier) {
    return std::ranges::contains(comparison_modifiers, modifier);
  });
  if (comparisons > 1) {
    return parse_failure("Sigma modifiers `lt`, `lte`, `gt`, and `gte` are "
                         "mutually exclusive (field `{}`)",
                         field);
  }
  if (has("neq") and comparisons > 0) {
    return parse_failure("Sigma modifier `neq` cannot be combined with "
                         "`lt`, `lte`, `gt`, or `gte` (field `{}`)",
                         field);
  }
  // At most one time-part extraction, and only comparison modifiers may
  // accompany it.
  auto const time_parts = count([](std::string const& modifier) {
    return std::ranges::contains(time_part_modifiers, modifier);
  });
  if (time_parts > 1) {
    return parse_failure("Sigma time modifiers are mutually exclusive "
                         "(field `{}`)",
                         field);
  }
  if (time_parts > 0) {
    for (auto const& modifier : modifiers) {
      if (std::ranges::contains(string_transform_modifiers, modifier)
          or modifier == "base64" or modifier == "base64offset"
          or is_utf16_modifier(modifier) or modifier == "cased"
          or modifier == "fieldref" or modifier == "cidr") {
        return parse_failure("Sigma time modifiers cannot be combined with "
                             "`{}` (field `{}`)",
                             modifier, field);
      }
    }
  }
  // `fieldref` takes string values naming fields; only `neq` may be added.
  if (has("fieldref")) {
    for (auto const& modifier : modifiers) {
      if (modifier != "fieldref" and modifier != "neq") {
        return parse_failure("Sigma modifier `fieldref` can only be combined "
                             "with `neq` (field `{}`)",
                             field);
      }
    }
    for (auto const& value : values) {
      if (not is<std::string>(value)) {
        return parse_failure("Sigma modifier `fieldref` requires string "
                             "values (field `{}`)",
                             field);
      }
    }
  }
  // `all` requires a value list per the specification.
  if (has("all") and not value_is_list) {
    return parse_failure("Sigma modifier `all` requires a list of values "
                         "(field `{}`)",
                         field);
  }
  // `re` cannot be combined with wildcard or encoding transformations.
  // Non-string scalars are stringified at lowering time: YAML scalar type
  // inference cannot distinguish `'46'` from `46`, so rejecting numbers
  // would make numeric-looking regular expressions unrepresentable.
  if (has("re")) {
    for (auto const& modifier : modifiers) {
      if (modifier == "base64" or modifier == "base64offset"
          or is_utf16_modifier(modifier) or modifier == "windash"
          or modifier == "contains" or modifier == "startswith"
          or modifier == "endswith" or modifier == "cased"
          or modifier == "fieldref" or modifier == "cidr") {
        return parse_failure("Sigma modifier `re` cannot be combined with "
                             "`{}` (field `{}`)",
                             modifier, field);
      }
    }
  }
  // Unresolved `expand` placeholders fail explicitly: this implementation
  // provides no placeholder mappings, and inventing them would silently
  // change match semantics.
  if (has("expand")) {
    return Err{
      diagnostic::error("Sigma modifier `expand` requires placeholder "
                        "mappings, which are not supported (field `{}`)",
                        field)
        .hint("replace the placeholder with concrete values or remove the "
              "rule from the set")
        .done()};
  }
  return {};
}

auto parse_detection_item(std::string_view key, data const& value)
  -> Result<DetectionItem, diagnostic> {
  auto keys = detail::split(key, "|");
  auto item = DetectionItem{};
  item.field = FieldPath{std::string{keys[0]}};
  for (auto i = keys.begin() + 1; i != keys.end(); ++i) {
    item.modifiers.emplace_back(*i);
  }
  if (is<record>(value)) {
    return parse_failure("nested records are not allowed in Sigma selections");
  }
  if (auto const* values = try_as<list>(&value)) {
    if (values->empty()) {
      return parse_failure(
        "empty value lists are not allowed in Sigma selections");
    }
    item.value_is_list = true;
    for (auto const& element : *values) {
      if (is<list>(element)) {
        return parse_failure(
          "nested lists are not allowed in Sigma selections");
      }
      if (is<record>(element)) {
        return parse_failure(
          "nested records are not allowed in Sigma selections");
      }
      item.values.push_back(element);
    }
  } else {
    item.values.push_back(value);
  }
  TRY(validate_modifiers(item.field.raw, item.modifiers, item.values,
                         item.value_is_list));
  return item;
}

auto parse_detection(data const& yaml) -> Result<Detection, diagnostic> {
  auto parse_group
    = [](record const& xs) -> Result<std::vector<DetectionItem>, diagnostic> {
    auto group = std::vector<DetectionItem>{};
    for (auto const& [key, value] : xs) {
      TRY(auto item, parse_detection_item(key, value));
      group.push_back(std::move(item));
    }
    return group;
  };
  auto result = Detection{};
  if (auto const* xs = try_as<record>(&yaml)) {
    TRY(auto group, parse_group(*xs));
    result.groups.push_back(std::move(group));
    return result;
  }
  if (auto const* xs = try_as<list>(&yaml)) {
    if (xs->empty()) {
      return parse_failure(
        "empty value lists are not allowed in Sigma selections");
    }
    // A list of maps is a disjunction of conjunctions; a list of scalars is
    // a keyword selection whose entries are OR-linked. Mixing both forms in
    // one list has no defined semantics.
    auto maps = size_t{0};
    auto scalars = size_t{0};
    for (auto const& element : *xs) {
      if (is<record>(element)) {
        ++maps;
      } else if (is<list>(element)) {
        return parse_failure(
          "nested lists are not allowed in Sigma selections");
      } else {
        ++scalars;
      }
    }
    if (maps > 0 and scalars > 0) {
      return parse_failure("Sigma search identifier must not mix keyword "
                           "values and maps in one list");
    }
    if (maps > 0) {
      for (auto const& element : *xs) {
        TRY(auto group, parse_group(as<record>(element)));
        result.groups.push_back(std::move(group));
      }
      return result;
    }
    // A list of scalars is a keyword selection: one OR-linked item matched
    // recursively against every string-valued leaf of the event.
    auto item = DetectionItem{};
    item.kind = DetectionItem::ItemKind::keyword;
    item.value_is_list = true;
    for (auto const& element : *xs) {
      item.values.push_back(element);
    }
    result.groups.push_back({std::move(item)});
    return result;
  }
  if (is<caf::none_t>(yaml)) {
    return parse_failure(
      "Sigma search identifier must be a list or record, got `{}`", yaml);
  }
  // A bare scalar is a single keyword.
  auto item = DetectionItem{};
  item.kind = DetectionItem::ItemKind::keyword;
  item.values.push_back(yaml);
  result.groups.push_back({std::move(item)});
  return result;
}

// -- condition parsing -------------------------------------------------

struct Token {
  enum class Kind {
    lparen,
    rparen,
    word,
  };

  Kind kind = Kind::word;
  std::string_view text;
};

auto tokenize(std::string_view input) -> std::vector<Token> {
  auto result = std::vector<Token>{};
  auto position = size_t{0};
  while (position < input.size()) {
    auto const c = input[position];
    if (c == ' ' or c == '\t' or c == '\n' or c == '\r') {
      ++position;
      continue;
    }
    if (c == '(') {
      result.push_back({Token::Kind::lparen, input.substr(position, 1)});
      ++position;
      continue;
    }
    if (c == ')') {
      result.push_back({Token::Kind::rparen, input.substr(position, 1)});
      ++position;
      continue;
    }
    auto const begin = position;
    while (position < input.size() and input[position] != ' '
           and input[position] != '\t' and input[position] != '\n'
           and input[position] != '\r' and input[position] != '('
           and input[position] != ')') {
      ++position;
    }
    result.push_back(
      {Token::Kind::word, input.substr(begin, position - begin)});
  }
  return result;
}

struct ConditionParser {
  std::vector<Token> tokens;
  size_t position = 0;

  auto peek() const -> Token const* {
    return position < tokens.size() ? &tokens[position] : nullptr;
  }

  auto accept_word(std::string_view text) -> bool {
    auto const* token = peek();
    if (token and token->kind == Token::Kind::word and token->text == text) {
      ++position;
      return true;
    }
    return false;
  }

  auto parse_expression() -> Result<Condition, diagnostic> {
    TRY(auto result, parse_term());
    while (accept_word("or")) {
      TRY(auto right, parse_term());
      result = Condition{Disjunction{ConditionPtr{std::move(result)},
                                     ConditionPtr{std::move(right)}}};
    }
    return result;
  }

  auto parse_term() -> Result<Condition, diagnostic> {
    TRY(auto result, parse_group());
    while (accept_word("and")) {
      TRY(auto right, parse_group());
      result = Condition{Conjunction{ConditionPtr{std::move(result)},
                                     ConditionPtr{std::move(right)}}};
    }
    return result;
  }

  auto parse_group() -> Result<Condition, diagnostic> {
    auto const* token = peek();
    if (not token) {
      return parse_failure("unexpected end of Sigma condition");
    }
    if (token->kind == Token::Kind::lparen) {
      ++position;
      TRY(auto result, parse_expression());
      if (not peek() or peek()->kind != Token::Kind::rparen) {
        return parse_failure("expected `)` in Sigma condition");
      }
      ++position;
      return result;
    }
    if (token->kind == Token::Kind::rparen) {
      return parse_failure("unexpected `)` in Sigma condition");
    }
    if (accept_word("not")) {
      TRY(auto operand, parse_group());
      return Condition{Negation{ConditionPtr{std::move(operand)}}};
    }
    if (token->text == "1" or token->text == "all") {
      // Quantified selector: `1 of x` / `all of them` / `all of selection_*`.
      auto const quantifier
        = token->text == "all" ? Quantifier::all : Quantifier::one;
      ++position;
      if (not accept_word("of")) {
        return parse_failure("expected `of` after `{}` in Sigma condition",
                             token->text);
      }
      auto const* target = peek();
      if (not target or target->kind != Token::Kind::word) {
        return parse_failure(
          "expected search identifier after `of` in Sigma condition");
      }
      ++position;
      return Condition{Quantified{
        .quantifier = quantifier,
        .pattern = std::string{target->text},
        .all_identifiers = target->text == "them",
      }};
    }
    // A plain (or wildcard) search identifier. Numeric quantifiers other than
    // `1` are not part of the documented v2.1 surface; reject them explicitly
    // instead of misreading them as identifiers.
    auto const* next
      = position + 1 < tokens.size() ? &tokens[position + 1] : nullptr;
    if (next and next->kind == Token::Kind::word and next->text == "of") {
      return parse_failure("unsupported quantifier `{}` in Sigma condition; "
                           "only `1 of` and `all of` are supported",
                           token->text);
    }
    if (token->text == "and" or token->text == "or" or token->text == "of") {
      return parse_failure("unexpected `{}` in Sigma condition", token->text);
    }
    ++position;
    return Condition{Identifier{std::string{token->text}}};
  }
};

// -- reference validation ----------------------------------------------

auto validate_references(Condition const& condition, DetectionRule const& rule)
  -> Result<void, diagnostic> {
  auto matches_any = [&](std::string_view pattern) {
    return std::ranges::any_of(rule.detections, [&](auto const& entry) {
      return pattern_matches(pattern, entry.first);
    });
  };
  return match(
    condition.node,
    [&](Identifier const& x) -> Result<void, diagnostic> {
      if (x.name.find('*') != std::string::npos) {
        if (not matches_any(x.name)) {
          return parse_failure("Sigma condition pattern `{}` matches no "
                               "search identifier",
                               x.name);
        }
        return {};
      }
      if (not rule.detections.contains(x.name)) {
        return parse_failure("Sigma condition references unknown search "
                             "identifier `{}`",
                             x.name);
      }
      return {};
    },
    [&](Quantified const& x) -> Result<void, diagnostic> {
      if (x.all_identifiers) {
        if (not matches_any("*")) {
          return parse_failure("Sigma condition `them` matches no search "
                               "identifier");
        }
        return {};
      }
      if (x.pattern.find('*') != std::string::npos) {
        if (not matches_any(x.pattern)) {
          return parse_failure("Sigma condition pattern `{}` matches no "
                               "search identifier",
                               x.pattern);
        }
        return {};
      }
      if (not rule.detections.contains(x.pattern)) {
        return parse_failure("Sigma condition references unknown search "
                             "identifier `{}`",
                             x.pattern);
      }
      return {};
    },
    [&](Negation const& x) -> Result<void, diagnostic> {
      return validate_references(*x.operand, rule);
    },
    [&](Conjunction const& x) -> Result<void, diagnostic> {
      TRY(validate_references(*x.left, rule));
      return validate_references(*x.right, rule);
    },
    [&](Disjunction const& x) -> Result<void, diagnostic> {
      TRY(validate_references(*x.left, rule));
      return validate_references(*x.right, rule);
    });
}

// -- document-level parsing --------------------------------------------

auto resolve_major(record const& document) -> Result<int, diagnostic> {
  auto i = document.find("sigma-version");
  if (i == document.end()) {
    return version_floor_major;
  }
  // Compare in the source type: narrowing before the supported-major check
  // would silently accept values such as 2^32 + 2 as major 2.
  auto major = Option<int64_t>{};
  if (auto const* number = try_as<int64_t>(&i->second)) {
    major = *number;
  } else if (auto const* number = try_as<uint64_t>(&i->second)) {
    if (*number <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
      major = detail::narrow<int64_t>(*number);
    } else {
      major = std::numeric_limits<int64_t>::max();
    }
  } else if (auto const* str = try_as<std::string>(&i->second)) {
    // A full release string such as `2.1.0` contributes only its major, but
    // the complete string must be a well-formed dotted version; a stray
    // suffix like `2garbage` must not slip through as major 2.
    auto parse_component
      = [](char const* first, char const* last, int64_t& value) -> char const* {
      auto [position, error] = std::from_chars(first, last, value);
      return error == std::errc{} and position != first and value >= 0
               ? position
               : nullptr;
    };
    auto const* first = str->data();
    auto const* last = str->data() + str->size();
    auto value = int64_t{};
    if (auto const* position = parse_component(first, last, value)) {
      auto valid = true;
      auto component = int64_t{};
      while (position != last) {
        if (*position != '.') {
          valid = false;
          break;
        }
        position = parse_component(position + 1, last, component);
        if (not position) {
          valid = false;
          break;
        }
      }
      if (valid) {
        major = value;
      }
    }
  } else if (auto const* number = try_as<double>(&i->second)) {
    // YAML type inference may turn `2.1` into a double. Reject non-finite
    // and out-of-range values before converting: the cast would be undefined
    // behavior, and overflowing YAML literals parse as infinity.
    constexpr auto bound = 9007199254740992.0; // 2^53, exact in a double
    if (std::isfinite(*number) and *number >= -bound and *number <= bound) {
      major = static_cast<int64_t>(*number);
    }
  }
  if (not major) {
    return parse_failure("invalid `sigma-version`: expected a version number, "
                         "got `{}`",
                         i->second);
  }
  if (*major != supported_major) {
    return Err{
      diagnostic::error("unsupported Sigma specification major {}", *major)
        .note("this implementation supports major {}", supported_major)
        .hint("documents declaring an unsupported major are never "
              "interpreted using older semantics")
        .done()};
  }
  return supported_major;
}

auto get_string(record const& document, std::string_view key)
  -> Option<std::string> {
  if (auto i = document.find(key); i != document.end()) {
    if (auto const* str = try_as<std::string>(&i->second)) {
      return *str;
    }
    // Sigma IDs are UUIDs and levels are words, but YAML type inference may
    // produce non-string scalars for e.g. numeric names; keep them verbatim.
    if (not is<record>(i->second) and not is<list>(i->second)
        and not is<caf::none_t>(i->second)) {
      return fmt::format("{}", i->second);
    }
  }
  return None{};
}

auto parse_log_source(record const& document) -> Result<LogSource, diagnostic> {
  auto result = LogSource{};
  auto i = document.find("logsource");
  if (i == document.end()) {
    return result;
  }
  auto const* fields = try_as<record>(&i->second);
  if (not fields) {
    return parse_failure("Sigma rule attribute `logsource` must be a record");
  }
  result.category = get_string(*fields, "category");
  result.product = get_string(*fields, "product");
  result.service = get_string(*fields, "service");
  return result;
}

auto parse_detection_rule(record const& document)
  -> Result<DetectionRule, diagnostic> {
  auto rule = DetectionRule{};
  rule.metadata.id = get_string(document, "id");
  rule.metadata.name = get_string(document, "name");
  rule.metadata.title = get_string(document, "title");
  rule.metadata.level = get_string(document, "level");
  rule.metadata.raw = document;
  TRY(rule.log_source, parse_log_source(document));
  auto detection_entry = document.find("detection");
  if (detection_entry == document.end()) {
    return parse_failure("Sigma rule has no `detection` attribute");
  }
  auto const* detection_section = try_as<record>(&detection_entry->second);
  if (not detection_section) {
    return parse_failure("Sigma rule attribute `detection` must be a record");
  }
  // Only the default `sigma` taxonomy is supported; compiling field names
  // from another taxonomy verbatim would silently change semantics.
  if (auto taxonomy = get_string(document, "taxonomy");
      taxonomy and *taxonomy != "sigma") {
    return parse_failure("Sigma rule uses unsupported taxonomy `{}`; only "
                         "the default `sigma` taxonomy is supported",
                         *taxonomy);
  }
  auto conditions = std::vector<std::string>{};
  auto have_condition = false;
  for (auto const& [key, value] : *detection_section) {
    if (key == "condition") {
      have_condition = true;
      // Sigma treats the entries of a list-valued condition as OR-linked
      // queries; a plain string is one query.
      if (auto const* condition_string = try_as<std::string>(&value)) {
        conditions.push_back(*condition_string);
      } else if (auto const* entries = try_as<list>(&value)) {
        for (auto const& element : *entries) {
          auto const* entry = try_as<std::string>(&element);
          if (not entry) {
            return parse_failure(
              "Sigma rule `condition` list entries must be strings");
          }
          conditions.push_back(*entry);
        }
        if (conditions.empty()) {
          return parse_failure("Sigma rule `condition` list must not be empty");
        }
      } else {
        return parse_failure(
          "Sigma rule `condition` must be a string or a list of strings");
      }
      continue;
    }
    TRY(auto detection, parse_detection(value));
    auto const inserted
      = rule.detections.try_emplace(key, std::move(detection)).second;
    TENZIR_ASSERT(inserted);
  }
  if (not have_condition) {
    return parse_failure("Sigma rule has no `condition` key");
  }
  for (auto const& condition : conditions) {
    TRY(auto parsed, parse_condition(condition));
    TRY(validate_references(parsed, rule));
    rule.conditions.push_back(std::move(parsed));
  }
  return rule;
}

/// Parses the `filter` section of a global filter document.
auto parse_filter_rule(record const& document)
  -> Result<FilterRule, diagnostic> {
  auto rule = FilterRule{};
  rule.metadata.id = get_string(document, "id");
  rule.metadata.name = get_string(document, "name");
  rule.metadata.title = get_string(document, "title");
  rule.metadata.raw = document;
  if (not rule.metadata.title) {
    return parse_failure("Sigma filter has no `title` attribute");
  }
  if (auto taxonomy = get_string(document, "taxonomy");
      taxonomy and *taxonomy != "sigma") {
    return parse_failure("Sigma filter uses unsupported taxonomy `{}`; only "
                         "the default `sigma` taxonomy is supported",
                         *taxonomy);
  }
  if (not document.contains("logsource")) {
    return parse_failure("Sigma filter has no `logsource` attribute");
  }
  TRY(rule.log_source, parse_log_source(document));
  auto const* section = [&]() -> record const* {
    auto entry = document.find("filter");
    return entry != document.end() ? try_as<record>(&entry->second) : nullptr;
  }();
  if (not section) {
    return parse_failure("Sigma filter attribute `filter` must be a record");
  }
  auto have_rules = false;
  auto condition = Option<std::string const&>{};
  for (auto const& [key, value] : *section) {
    if (key == "rules") {
      have_rules = true;
      // The v2.1 JSON schema only allows an array, but SigmaHQ documents
      // `rules: any` for every compatible rule; we accept both.
      if (auto const* str = try_as<std::string>(&value)) {
        if (*str != "any") {
          return parse_failure("Sigma filter `rules` must be `any` or a "
                               "list of rule ids or names");
        }
        rule.targets.any = true;
        continue;
      }
      auto const* entries = try_as<list>(&value);
      if (not entries or entries->empty()) {
        return parse_failure("Sigma filter `rules` must be a non-empty list "
                             "of rule ids or names");
      }
      for (auto const& element : *entries) {
        if (auto const* str = try_as<std::string>(&element)) {
          rule.targets.rules.push_back(*str);
          continue;
        }
        // YAML type inference may turn UUID-like or numeric references into
        // non-string scalars; keep their textual form.
        if (not is<record>(element) and not is<list>(element)
            and not is<caf::none_t>(element)) {
          rule.targets.rules.push_back(fmt::format("{}", element));
          continue;
        }
        return parse_failure("Sigma filter `rules` entries must be strings");
      }
      continue;
    }
    if (key == "condition") {
      auto const* condition_string = try_as<std::string>(&value);
      if (not condition_string) {
        return parse_failure("Sigma filter `condition` must be a string");
      }
      condition = *condition_string;
      continue;
    }
    TRY(auto detection, parse_detection(value));
    auto const inserted
      = rule.detections.try_emplace(key, std::move(detection)).second;
    TENZIR_ASSERT(inserted);
  }
  if (not have_rules) {
    return parse_failure("Sigma filter has no `rules` attribute");
  }
  if (rule.detections.empty()) {
    return parse_failure("Sigma filter has no selection");
  }
  if (not condition) {
    return parse_failure("Sigma filter has no `condition` key");
  }
  TRY(rule.condition, parse_condition(*condition));
  // Reference validation reuses the detection-rule logic via a shim.
  auto shim = DetectionRule{};
  shim.detections = rule.detections;
  TRY(validate_references(rule.condition, shim));
  return rule;
}

/// Rewrites every identifier and pattern of a filter condition to its
/// injected `_filt_<ordinal>_`-prefixed name.
auto prefix_condition(Condition const& condition, std::string const& prefix)
  -> Condition {
  return match(
    condition.node,
    [&](Identifier const& x) -> Condition {
      return Condition{Identifier{prefix + x.name}};
    },
    [&](Quantified const& x) -> Condition {
      // `them` inside a filter refers to the filter's own selections.
      auto result = Quantified{};
      result.quantifier = x.quantifier;
      result.pattern = x.all_identifiers ? prefix + "*" : prefix + x.pattern;
      result.all_identifiers = false;
      return Condition{std::move(result)};
    },
    [&](Negation const& x) -> Condition {
      return Condition{
        Negation{ConditionPtr{prefix_condition(*x.operand, prefix)}}};
    },
    [&](Conjunction const& x) -> Condition {
      return Condition{
        Conjunction{ConditionPtr{prefix_condition(*x.left, prefix)},
                    ConditionPtr{prefix_condition(*x.right, prefix)}}};
    },
    [&](Disjunction const& x) -> Condition {
      return Condition{
        Disjunction{ConditionPtr{prefix_condition(*x.left, prefix)},
                    ConditionPtr{prefix_condition(*x.right, prefix)}}};
    });
}

} // namespace

auto parse_condition(std::string_view condition)
  -> Result<Condition, diagnostic> {
  auto parser = ConditionParser{tokenize(condition)};
  if (parser.tokens.empty()) {
    return parse_failure("Sigma condition must not be empty");
  }
  TRY(auto result, parser.parse_expression());
  if (parser.position != parser.tokens.size()) {
    return parse_failure("trailing tokens in Sigma condition starting at `{}`",
                         parser.tokens[parser.position].text);
  }
  return result;
}

auto parse_document(data const& yaml) -> Result<Document, diagnostic> {
  auto const* document = try_as<record>(&yaml);
  if (not document) {
    return parse_failure("Sigma rule must be a record");
  }
  auto result = Document{};
  TRY(result.major, resolve_major(*document));
  if (document->contains("correlation")) {
    result.content = CorrelationRule{*document};
    return Err{diagnostic::error("Sigma correlation rules are not yet "
                                 "supported")
                 .done()};
  }
  if (document->contains("filter")) {
    TRY(result.content, parse_filter_rule(*document));
    return result;
  }
  TRY(result.content, parse_detection_rule(*document));
  return result;
}

auto pattern_matches(std::string_view pattern, std::string_view name) -> bool {
  // Search identifiers beginning with `_` are reserved (e.g. for filter
  // injection) and are only matched by patterns that themselves start with
  // `_`. Explicit patterns continue to match the names they name.
  if (not pattern.starts_with('_') and name.starts_with('_')) {
    return false;
  }
  return wildcard_match(pattern, name);
}

auto compatible(LogSource const& filter, LogSource const& target) -> bool {
  auto subset = [](Option<std::string> const& required,
                   Option<std::string> const& present) {
    return not required or (present and *required == *present);
  };
  return subset(filter.category, target.category)
         and subset(filter.product, target.product)
         and subset(filter.service, target.service);
}

auto apply_filter(DetectionRule rule, FilterRule const& filter, size_t ordinal)
  -> DetectionRule {
  // The prefix keeps injected selections out of the rule's own quantifier
  // patterns: identifiers beginning with `_` are only matched by patterns
  // that themselves begin with `_`. Extend it until the complete namespace is
  // free, so filter wildcard patterns cannot also select an existing rule
  // detection.
  auto prefix = fmt::format("_filt_{}_", ordinal);
  while (std::ranges::any_of(rule.detections, [&](auto const& entry) {
    return entry.first.starts_with(prefix);
  })) {
    prefix += '_';
  }
  for (auto const& [name, detection] : filter.detections) {
    std::ignore = rule.detections.try_emplace(prefix + name, detection);
  }
  auto const filter_condition = prefix_condition(filter.condition, prefix);
  // The filter applies conjunctively to every OR-linked condition entry.
  for (auto& condition : rule.conditions) {
    condition = Condition{Conjunction{ConditionPtr{std::move(condition)},
                                      ConditionPtr{filter_condition}}};
  }
  return rule;
}

auto to_string(Condition const& condition) -> std::string {
  auto render = [](this auto&& self, Condition const& node,
                   bool parenthesize) -> std::string {
    return match(
      node.node,
      [&](Identifier const& x) -> std::string {
        return x.name;
      },
      [&](Quantified const& x) -> std::string {
        auto const quantifier
          = x.quantifier == Quantifier::all ? "all of" : "1 of";
        auto const target = x.all_identifiers ? "them" : x.pattern.c_str();
        return fmt::format("{} {}", quantifier, target);
      },
      [&](Negation const& x) -> std::string {
        return fmt::format("not {}", self(*x.operand, true));
      },
      [&](Conjunction const& x) -> std::string {
        auto result
          = fmt::format("{} and {}", self(*x.left, true), self(*x.right, true));
        return parenthesize ? fmt::format("({})", result) : result;
      },
      [&](Disjunction const& x) -> std::string {
        auto result
          = fmt::format("{} or {}", self(*x.left, true), self(*x.right, true));
        return parenthesize ? fmt::format("({})", result) : result;
      });
  };
  return render(condition, false);
}

namespace {

auto to_data(DetectionItem const& item) -> std::pair<std::string, data> {
  auto key = item.field.raw;
  for (auto const& modifier : item.modifiers) {
    key += '|';
    key += modifier;
  }
  if (item.value_is_list) {
    auto values = list{};
    values.reserve(item.values.size());
    for (auto const& value : item.values) {
      values.push_back(value);
    }
    return {std::move(key), std::move(values)};
  }
  TENZIR_ASSERT(item.values.size() == 1);
  return {std::move(key), item.values[0]};
}

auto to_data(Detection const& detection) -> data {
  auto render_group = [](std::vector<DetectionItem> const& group) -> data {
    // A group of keyword items renders as a list of scalars.
    if (group.size() == 1
        and group[0].kind == DetectionItem::ItemKind::keyword) {
      if (group[0].value_is_list) {
        auto values = list{};
        for (auto const& value : group[0].values) {
          values.push_back(value);
        }
        return values;
      }
      return group[0].values[0];
    }
    auto result = record{};
    for (auto const& item : group) {
      auto [key, value] = to_data(item);
      result.emplace(std::move(key), std::move(value));
    }
    return result;
  };
  if (detection.groups.size() == 1) {
    return render_group(detection.groups[0]);
  }
  auto groups = list{};
  groups.reserve(detection.groups.size());
  for (auto const& group : detection.groups) {
    groups.push_back(render_group(group));
  }
  return groups;
}

} // namespace

auto to_record(DetectionRule const& rule) -> record {
  auto result = rule.metadata.raw;
  auto detection = record{};
  for (auto const& [name, value] : rule.detections) {
    detection.emplace(name, to_data(value));
  }
  if (rule.conditions.size() == 1) {
    detection.emplace("condition", to_string(rule.conditions[0]));
  } else {
    auto conditions = list{};
    conditions.reserve(rule.conditions.size());
    for (auto const& condition : rule.conditions) {
      conditions.push_back(to_string(condition));
    }
    detection.emplace("condition", std::move(conditions));
  }
  result["detection"] = std::move(detection);
  return result;
}

auto wildcard_match(std::string_view pattern, std::string_view name) -> bool {
  auto pattern_position = size_t{0};
  auto name_position = size_t{0};
  auto star_position = std::string_view::npos;
  auto backtrack_position = size_t{0};
  while (name_position < name.size()) {
    if (pattern_position < pattern.size()
        and pattern[pattern_position] == name[name_position]) {
      ++pattern_position;
      ++name_position;
    } else if (pattern_position < pattern.size()
               and pattern[pattern_position] == '*') {
      star_position = pattern_position++;
      backtrack_position = name_position;
    } else if (star_position != std::string_view::npos) {
      pattern_position = star_position + 1;
      name_position = ++backtrack_position;
    } else {
      return false;
    }
  }
  while (pattern_position < pattern.size()
         and pattern[pattern_position] == '*') {
    ++pattern_position;
  }
  return pattern_position == pattern.size();
}

} // namespace tenzir::sigma
