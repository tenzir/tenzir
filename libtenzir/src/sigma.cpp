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

namespace tenzir::sigma {

namespace {

template <class... Ts>
auto parse_failure(fmt::format_string<Ts...> str, Ts&&... xs)
  -> Err<diagnostic> {
  return Err{diagnostic::error(str, std::forward<Ts>(xs)...).done()};
}

/// Modifiers whose semantics are implemented by the lowering phase.
constexpr auto implemented_modifiers = std::array<std::string_view, 12>{
  "all",    "lt",           "lte",        "gt",       "gte", "contains",
  "base64", "base64offset", "startswith", "endswith", "re",  "cidr",
};

/// Standard v2.1 modifiers that are recognized but not yet implemented.
/// These fail explicitly instead of being approximated or silently ignored.
constexpr auto unimplemented_modifiers = std::array<std::string_view, 5>{
  "utf16le", "wide", "utf16be", "utf16", "expand",
};

auto validate_modifiers(std::string_view field,
                        std::vector<std::string> const& modifiers)
  -> Result<void, diagnostic> {
  for (auto const& modifier : modifiers) {
    if (std::ranges::contains(implemented_modifiers, modifier)) {
      continue;
    }
    if (std::ranges::contains(unimplemented_modifiers, modifier)) {
      return parse_failure("Sigma modifier `{}` is not yet implemented",
                           modifier);
    }
    return Err{diagnostic::error("unknown Sigma modifier `{}` for field `{}`",
                                 modifier, field)
                 .hint("unsupported modifiers reject the rule; nothing is "
                       "silently ignored")
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
  TRY(validate_modifiers(item.field.raw, item.modifiers));
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
    for (auto const& element : *xs) {
      auto const* map = try_as<record>(&element);
      if (not map) {
        return parse_failure("Sigma search identifier must be a list or "
                             "record, got `{}`",
                             element);
      }
      TRY(auto group, parse_group(*map));
      result.groups.push_back(std::move(group));
    }
    return result;
  }
  return parse_failure(
    "Sigma search identifier must be a list or record, got `{}`", yaml);
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
      return wildcard_match(pattern, entry.first);
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
  auto condition = Option<std::string const&>{};
  for (auto const& [key, value] : *detection_section) {
    if (key == "condition") {
      auto const* condition_string = try_as<std::string>(&value);
      if (not condition_string) {
        return parse_failure("Sigma rule `condition` must be a string");
      }
      condition = *condition_string;
      continue;
    }
    TRY(auto detection, parse_detection(value));
    auto const inserted
      = rule.detections.try_emplace(key, std::move(detection)).second;
    TENZIR_ASSERT(inserted);
  }
  if (not condition) {
    return parse_failure("Sigma rule has no `condition` key");
  }
  TRY(rule.condition, parse_condition(*condition));
  TRY(validate_references(rule.condition, rule));
  return rule;
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
    result.content = FilterRule{*document};
    return Err{
      diagnostic::error("Sigma global filter rules are not yet supported")
        .done()};
  }
  TRY(result.content, parse_detection_rule(*document));
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
