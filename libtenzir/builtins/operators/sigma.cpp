//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/plugin.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/bitmap.hpp>
#include <tenzir/concept/convertible/to.hpp>
#include <tenzir/concept/parseable/core.hpp>
#include <tenzir/concept/parseable/string.hpp>
#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/concept/printable/tenzir/data.hpp>
#include <tenzir/concept/printable/to_string.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/base64.hpp>
#include <tenzir/detail/flat_map.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/hash/hash.hpp>
#include <tenzir/io/read.hpp>
#include <tenzir/multi_series.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/result.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/session.hpp>
#include <tenzir/sigma.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/filter.hpp>
#include <tenzir/tql2/resolve.hpp>

#include <arrow/record_batch.h>
#include <fmt/format.h>
#include <re2/re2.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <ranges>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace tenzir::plugins::sigma {

// TODO: A lot of code in here is directly copied from
// src/concept/parseable/expression.cpp. We should factor the implementation in
// the future.

namespace {

namespace ir = tenzir::sigma;

template <class T>
using ParseResult = Result<T, diagnostic>;

template <class... Ts>
auto parse_failure(fmt::format_string<Ts...> str, Ts&&... xs)
  -> Err<diagnostic> {
  return Err{diagnostic::error(str, std::forward<Ts>(xs)...).done()};
}

ParseResult<std::string>
transform_sigma_string(std::string_view str, std::string_view fmt,
                       std::string_view key, bool case_insensitive,
                       bool windash);

ParseResult<std::string>
validate_regex(std::string regex, std::string_view key);

using ExpressionMap = detail::flat_map<std::string, ast::expression>;

/// Helpers to combine lowered sub-expressions when resolving search
/// identifiers and quantified selectors from the Sigma IR.
namespace expression_algebra {

/// Joins a set of sub-expressions into a conjunction or disjunction.
template <ast::binary_op Op>
auto join(std::vector<ast::expression> xs) -> ast::expression {
  if (xs.empty()) {
    return ast::constant{false, location::unknown};
  }
  auto result = std::move(xs[0]);
  for (auto i = size_t{1}; i < xs.size(); ++i) {
    result = ast::binary_expr{std::move(result), Op, std::move(xs[i])};
  }
  return result;
}

} // namespace expression_algebra

/// Appends a literal character to a regular expression. Escaping every ASCII
/// non-alphanumeric character is safe in RE2 and avoids an incomplete
/// metacharacter blocklist, while bytes of multi-byte UTF-8 sequences must
/// pass through unmodified.
auto append_regex_literal(std::string& regex, char c) -> void {
  auto const byte = static_cast<unsigned char>(c);
  if (byte < 0x80 and std::isalnum(byte) == 0) {
    regex += '\\';
  }
  regex += c;
}

/// Transforms a string that may contain Sigma glob wildcards into a regular
/// expression with respective metacharacters. Sigma patterns are
/// case-insensitive unless the `cased` modifier is present, and Sigma
/// wildcards match any character including newlines, so the regex enables
/// dot-matches-newline mode.
/// Returns whether a `windash` dash or slash at position `i` is
/// interchangeable: pySigma replaces `-` and `/` matching `\B[-/]\b`, i.e.
/// at the start of a word, and leaves other occurrences literal.
auto windash_interchangeable(std::string_view str, size_t i) -> bool {
  TENZIR_ASSERT(i < str.size() and (str[i] == '-' or str[i] == '/'));
  auto is_continuation = [](char c) {
    return (static_cast<unsigned char>(c) & 0xC0u) == 0x80u;
  };
  auto is_word = [](std::string_view code_point) {
    return code_point == "_" or detail::utf8_code_point_isalnum(code_point);
  };
  auto boundary_before = true;
  if (i > 0) {
    auto begin = i - 1;
    while (begin > 0 and is_continuation(str[begin])) {
      --begin;
    }
    boundary_before = not is_word(str.substr(begin, i - begin));
  }
  auto boundary_after = false;
  if (i + 1 < str.size()) {
    auto end = i + 2;
    while (end < str.size() and is_continuation(str[end])) {
      ++end;
    }
    boundary_after = is_word(str.substr(i + 1, end - i - 1));
  }
  return boundary_before and boundary_after;
}

ParseResult<std::string>
transform_sigma_string(std::string_view str, std::string_view fmt,
                       std::string_view key, bool case_insensitive,
                       bool windash) {
  // The following invariants apply according to the Sigma spec:
  // - All values are treated as case-insensitive strings by default
  // - You can use wildcard characters '*' and '?' in strings
  // - Wildcards can be escaped with \, e.g. \*. If some wildcard after a
  //   backslash should be searched, the backslash has to be escaped: \\*.
  auto f = str.begin();
  auto l = str.end();
  auto regex = std::string{};
  while (f != l) {
    // The `windash` modifier makes a word-initial `-` or `/` match any
    // dash-like character. A single character class avoids the combinatorial
    // explosion of expanding value permutations.
    if (windash and (*f == '-' or *f == '/')
        and windash_interchangeable(str, f - str.begin())) {
      regex += "[-/\u2013\u2014\u2015]";
      ++f;
      continue;
    }
    auto const c = *f++;
    switch (c) {
      case '*':
        regex += ".*";
        break;
      case '?':
        regex += '.';
        break;
      case '\\':
        if (f != l and (*f == '?' or *f == '*' or *f == '\\')) {
          // Edge-case: The user intended to escape the glob character.
          regex += '\\';
          regex += *f++;
          break;
        }
        regex += "\\\\";
        break;
      default:
        append_regex_literal(regex, c);
        break;
    }
  }
  auto const* flags = case_insensitive ? "is" : "s";
  auto result = fmt.empty()
                  ? fmt::format("(?{}:{})", flags, regex)
                  : fmt::format("(?{}:{})", flags,
                                fmt::format(TENZIR_FMT_RUNTIME(fmt), regex));
  return validate_regex(std::move(result), key);
}

ParseResult<std::string>
validate_regex(std::string regex, std::string_view key) {
  auto compiled = re2::RE2{regex, re2::RE2::CannedOptions::Quiet};
  if (not compiled.ok()) {
    return Err{
      diagnostic::error("invalid regular expression for Sigma field `{}`", key)
        .note("regex: {}", regex)
        .note("error: {}", compiled.error())
        .done()};
  }
  return regex;
}

auto make_function_expr(std::string_view name,
                        std::vector<ast::expression> args) -> ast::expression {
  return ast::function_call{
    ast::entity{{ast::identifier{std::string{name}, location::unknown}}},
    std::move(args), location::unknown, false};
}

/// Builds the expression that resolves a Sigma field name. Names without
/// dots become plain field accesses. Dotted names have deterministic
/// exact-key precedence: the complete name is first tried as an exact
/// top-level key, and only if it is absent, dots denote nested traversal.
/// This runtime decision lives in the internal `_sigma_field` function.
auto make_field_expr(std::string_view name) -> ast::expression {
  if (name.find('.') == std::string_view::npos) {
    return ast::expression{ast::root_field{
      ast::identifier{std::string{name}, location::unknown}, true}};
  }
  auto args = std::vector<ast::expression>{};
  args.emplace_back(ast::this_{location::unknown});
  args.emplace_back(ast::constant{std::string{name}, location::unknown});
  return make_function_expr("_sigma_field", std::move(args));
}

/// Builds the expression testing whether a Sigma field exists, following the
/// same exact-key precedence as `make_field_expr`.
auto make_field_exists_expr(std::string_view name) -> ast::expression {
  auto args = std::vector<ast::expression>{};
  args.emplace_back(ast::this_{location::unknown});
  args.emplace_back(ast::constant{std::string{name}, location::unknown});
  return make_function_expr("_sigma_has", std::move(args));
}

auto make_constant(data const& value) -> ast::expression {
  return match(
    value,
    [](pattern const&) -> ast::expression {
      TENZIR_UNREACHABLE();
    },
    []<class T>(T const& x) -> ast::expression
      requires(not std::same_as<T, pattern>)
    {
      return ast::constant{x, location::unknown};
    });
}

auto make_regex_expr(ast::expression field, std::string regex)
  -> ast::expression {
  return ast::function_call{
    ast::entity{{ast::identifier{"match_regex", location::unknown}}},
    {std::move(field), ast::constant{std::move(regex), location::unknown}},
    location::unknown,
    false};
}

auto make_binary_expr(ast::expression left, ast::binary_op op,
                      ast::expression right) -> ast::expression {
  return ast::binary_expr{std::move(left), op, std::move(right)};
}

// -- lowering: Sigma IR -> TQL expressions -------------------------------

/// Lowers one detection item (`field|modifiers: value(s)`) into a TQL
/// expression. The IR has already validated the modifier chain and value
/// types; this function only implements the executable semantics.
auto encode_utf16(std::string_view str, bool big_endian, bool bom)
  -> std::string {
  // Interpret the value as UTF-8 and produce UTF-16 code units. Sigma values
  // are overwhelmingly ASCII; non-BMP code points produce surrogate pairs.
  auto units = std::vector<uint16_t>{};
  if (bom) {
    units.push_back(0xFEFF);
  }
  auto i = size_t{0};
  while (i < str.size()) {
    auto const c = static_cast<unsigned char>(str[i]);
    auto code_point = uint32_t{0};
    auto length = size_t{1};
    if (c < 0x80) {
      code_point = c;
    } else if ((c >> 5) == 0x6 and i + 1 < str.size()) {
      code_point = ((c & 0x1Fu) << 6u)
                   | (static_cast<unsigned char>(str[i + 1]) & 0x3Fu);
      length = 2;
    } else if ((c >> 4) == 0xE and i + 2 < str.size()) {
      code_point = ((c & 0x0Fu) << 12u)
                   | ((static_cast<unsigned char>(str[i + 1]) & 0x3Fu) << 6u)
                   | (static_cast<unsigned char>(str[i + 2]) & 0x3Fu);
      length = 3;
    } else if ((c >> 3) == 0x1E and i + 3 < str.size()) {
      code_point = ((c & 0x07u) << 18u)
                   | ((static_cast<unsigned char>(str[i + 1]) & 0x3Fu) << 12u)
                   | ((static_cast<unsigned char>(str[i + 2]) & 0x3Fu) << 6u)
                   | (static_cast<unsigned char>(str[i + 3]) & 0x3Fu);
      length = 4;
    } else {
      code_point = 0xFFFD;
    }
    i += length;
    if (code_point >= 0x10000) {
      code_point -= 0x10000;
      units.push_back(static_cast<uint16_t>(0xD800 + (code_point >> 10u)));
      units.push_back(static_cast<uint16_t>(0xDC00 + (code_point & 0x3FFu)));
    } else {
      units.push_back(static_cast<uint16_t>(code_point));
    }
  }
  auto result = std::string{};
  result.reserve(units.size() * 2);
  for (auto const unit : units) {
    if (big_endian) {
      result.push_back(static_cast<char>(unit >> 8u));
      result.push_back(static_cast<char>(unit & 0xFFu));
    } else {
      result.push_back(static_cast<char>(unit & 0xFFu));
      result.push_back(static_cast<char>(unit >> 8u));
    }
  }
  return result;
}

/// The declarative matching semantics of one detection item, derived from
/// its validated modifier chain.
struct ItemSemantics {
  ast::binary_op op = ast::binary_op::eq;
  /// Set by `neq`.
  bool negate = false;
  /// Disabled by `cased`.
  bool case_insensitive = true;
  /// Set by `all`.
  bool value_list_conjunction = false;
  /// Set by `re`.
  bool raw_regex = false;
  /// The `re` sub-modifiers `i`, `m`, and `s`.
  std::string regex_flags;
  bool fieldref = false;
  bool exists = false;
  bool windash = false;
  bool contains = false;
  bool wildcard_prefix = false;
  bool wildcard_suffix = false;
  bool stringify_for_regex = false;
  Option<std::string> time_part;
  std::vector<std::function<ParseResult<std::vector<data>>(data const&)>>
    transforms;
};

auto parse_semantics(ir::DetectionItem const& item)
  -> ParseResult<ItemSemantics> {
  auto result = ItemSemantics{};
  for (auto const& modifier : item.modifiers) {
    if (modifier == "all") {
      result.value_list_conjunction = true;
    } else if (modifier == "lt") {
      result.op = ast::binary_op::lt;
    } else if (modifier == "lte") {
      result.op = ast::binary_op::leq;
    } else if (modifier == "gt") {
      result.op = ast::binary_op::gt;
    } else if (modifier == "gte") {
      result.op = ast::binary_op::geq;
    } else if (modifier == "neq") {
      result.negate = true;
    } else if (modifier == "cased") {
      result.case_insensitive = false;
    } else if (modifier == "exists") {
      result.exists = true;
    } else if (modifier == "fieldref") {
      result.fieldref = true;
    } else if (modifier == "contains") {
      result.contains = true;
      result.wildcard_prefix = true;
      result.wildcard_suffix = true;
    } else if (modifier == "startswith") {
      result.wildcard_suffix = true;
      result.stringify_for_regex = true;
    } else if (modifier == "endswith") {
      result.wildcard_prefix = true;
      result.stringify_for_regex = true;
    } else if (modifier == "re") {
      result.raw_regex = true;
    } else if (modifier == "i") {
      result.regex_flags += 'i';
    } else if (modifier == "m") {
      result.regex_flags += 'm';
    } else if (modifier == "s") {
      result.regex_flags += 's';
    } else if (modifier == "cidr") {
      result.op = ast::binary_op::in;
    } else if (modifier == "windash") {
      result.windash = true;
    } else if (modifier == "minute" or modifier == "hour" or modifier == "day"
               or modifier == "week" or modifier == "month"
               or modifier == "year") {
      result.time_part = modifier;
    } else if (modifier == "base64") {
      result.transforms.emplace_back(
        [](data const& x) -> ParseResult<std::vector<data>> {
          if (auto const* str = try_as<std::string>(&x)) {
            return std::vector<data>{detail::base64::encode(*str)};
          }
          return parse_failure(
            "Sigma modifier `base64` only works with strings");
        });
    } else if (modifier == "base64offset") {
      result.transforms.emplace_back(
        [](data const& x) -> ParseResult<std::vector<data>> {
          auto const* str = try_as<std::string>(&x);
          if (not str) {
            return parse_failure(
              "Sigma modifier `base64offset` only works with strings");
          }
          static constexpr auto start = std::array<size_t, 3>{{0, 2, 3}};
          static constexpr auto end = std::array<size_t, 3>{{0, 3, 2}};
          auto variants = std::vector<data>{};
          for (auto i = size_t{0}; i < 3; ++i) {
            auto padded = std::string(i, ' ') + *str;
            auto b64 = detail::base64::encode(padded);
            auto length = b64.size() - end[(str->size() + i) % 3];
            variants.emplace_back(b64.substr(start[i], length - start[i]));
          }
          return variants;
        });
    } else if (modifier == "utf16le" or modifier == "wide"
               or modifier == "utf16be" or modifier == "utf16") {
      auto const big_endian = modifier == "utf16be";
      auto const bom = modifier == "utf16";
      result.transforms.emplace_back(
        [big_endian, bom](data const& x) -> ParseResult<std::vector<data>> {
          if (auto const* str = try_as<std::string>(&x)) {
            return std::vector<data>{encode_utf16(*str, big_endian, bom)};
          }
          return parse_failure("Sigma UTF-16 modifiers only work with strings");
        });
    } else {
      // The IR validates modifier chains before lowering.
      return parse_failure("Sigma modifier `{}` is not yet implemented",
                           modifier);
    }
  }
  return result;
}

/// Lowers one detection item (`field|modifiers: value(s)`) into a TQL
/// expression. The IR has already validated the modifier chain and value
/// types; this function only implements the executable semantics.
auto lower_item(ir::DetectionItem const& item) -> ParseResult<ast::expression> {
  auto const& field = item.field.raw;
  auto key = field;
  for (auto const& modifier : item.modifiers) {
    key += '|';
    key += modifier;
  }
  TRY(auto semantics, parse_semantics(item));
  // Keyword items match every string-valued leaf of the event recursively.
  if (item.kind == ir::DetectionItem::ItemKind::keyword) {
    auto disjuncts = std::vector<ast::expression>{};
    for (auto const& value : item.values) {
      auto const str
        = is<std::string>(value) ? as<std::string>(value) : to_string(value);
      TRY(auto regex,
          transform_sigma_string(str, ".*{}.*", "<keywords>", true, false));
      auto args = std::vector<ast::expression>{};
      args.emplace_back(ast::this_{location::unknown});
      args.emplace_back(ast::constant{std::move(regex), location::unknown});
      disjuncts.push_back(
        make_function_expr("_sigma_keywords", std::move(args)));
    }
    return expression_algebra::join<ast::binary_op::or_>(std::move(disjuncts));
  }
  // `exists` is the sole modifier and tests field presence.
  if (semantics.exists) {
    TENZIR_ASSERT(item.values.size() == 1);
    auto expr = make_field_exists_expr(field);
    if (as<bool>(item.values[0])) {
      return expr;
    }
    return ast::expression{
      ast::unary_expr{{ast::unary_op::not_, {}}, std::move(expr)}};
  }
  // Applies the value transforms (UTF-16, Base64) with fan-out.
  auto modify = [&](data const& x) -> ParseResult<std::vector<data>> {
    auto variants = std::vector<data>{x};
    for (auto const& transform : semantics.transforms) {
      auto next = std::vector<data>{};
      for (auto const& variant : variants) {
        TRY(auto expanded, transform(variant));
        std::ranges::move(expanded, std::back_inserter(next));
      }
      variants = std::move(next);
    }
    return variants;
  };
  // Builds the field expression, wrapping it in a time-part extraction when
  // a time modifier is present.
  auto make_lhs = [&]() -> ast::expression {
    auto expr = make_field_expr(field);
    if (not semantics.time_part) {
      return expr;
    }
    if (*semantics.time_part == "week") {
      // There is no dedicated week extractor; use ISO week via formatting.
      auto format_args = std::vector<ast::expression>{};
      format_args.emplace_back(std::move(expr));
      format_args.emplace_back(
        ast::constant{std::string{"%V"}, location::unknown});
      auto formatted
        = make_function_expr("format_time", std::move(format_args));
      auto int_args = std::vector<ast::expression>{};
      int_args.emplace_back(std::move(formatted));
      return make_function_expr("int", std::move(int_args));
    }
    auto args = std::vector<ast::expression>{};
    args.emplace_back(std::move(expr));
    return make_function_expr(*semantics.time_part, std::move(args));
  };
  // Builds the predicate for one concrete (transformed) value.
  auto make_predicate_expr
    = [&](data const& value) -> ParseResult<ast::expression> {
    if (semantics.fieldref) {
      TENZIR_ASSERT(is<std::string>(value));
      auto const& referenced_field = as<std::string>(value);
      auto predicates = std::vector<ast::expression>{};
      predicates.push_back(make_binary_expr(make_lhs(), ast::binary_op::neq,
                                            make_constant(caf::none)));
      predicates.push_back(make_binary_expr(make_field_expr(referenced_field),
                                            ast::binary_op::neq,
                                            make_constant(caf::none)));
      predicates.push_back(make_binary_expr(
        make_lhs(), semantics.negate ? ast::binary_op::neq : ast::binary_op::eq,
        make_field_expr(referenced_field)));
      return expression_algebra::join<ast::binary_op::and_>(
        std::move(predicates));
    }
    if (semantics.raw_regex) {
      // Non-string scalars are stringified: YAML scalar type inference
      // cannot distinguish `'46'` from `46`.
      auto regex
        = is<std::string>(value) ? as<std::string>(value) : to_string(value);
      if (not semantics.regex_flags.empty()) {
        regex
          = fmt::format("(?{}:{})", semantics.regex_flags, std::move(regex));
      }
      TRY(auto valid, validate_regex(std::move(regex), key));
      return make_regex_expr(make_lhs(), std::move(valid));
    }
    auto make_string_predicate
      = [&](std::string str) -> ParseResult<ast::expression> {
      auto const fmt = semantics.wildcard_prefix and semantics.wildcard_suffix
                         ? ".*{}.*"
                       : semantics.wildcard_prefix ? ".*{}$"
                       : semantics.wildcard_suffix ? "^{}.*"
                                                   : "^{}$";
      TRY(auto pattern,
          transform_sigma_string(str, fmt, key, semantics.case_insensitive,
                                 semantics.windash));
      return make_regex_expr(make_lhs(), std::move(pattern));
    };
    if (auto const* str = try_as<std::string>(&value)) {
      if (semantics.op == ast::binary_op::eq) {
        return make_string_predicate(*str);
      }
      // Ordered comparisons on strings compare values directly.
      return make_binary_expr(make_lhs(), semantics.op, make_constant(value));
    }
    if (semantics.stringify_for_regex) {
      return make_string_predicate(to_string(value));
    }
    if (semantics.contains and is<subnet>(value)) {
      return make_binary_expr(make_lhs(), ast::binary_op::in,
                              make_constant(value));
    }
    return make_binary_expr(make_lhs(), semantics.op, make_constant(value));
  };
  // Lowers one source value: transform fan-out produces a disjunction.
  auto lower_value = [&](data const& value) -> ParseResult<ast::expression> {
    TRY(auto variants, modify(value));
    auto disjuncts = std::vector<ast::expression>{};
    for (auto const& variant : variants) {
      TRY(auto expr, make_predicate_expr(variant));
      disjuncts.push_back(std::move(expr));
    }
    return expression_algebra::join<ast::binary_op::or_>(std::move(disjuncts));
  };
  auto lower_source_value
    = [&](data const& value) -> ParseResult<ast::expression> {
    TRY(auto result, lower_value(value));
    if (semantics.negate and not semantics.fieldref) {
      result = ast::expression{
        ast::unary_expr{{ast::unary_op::not_, {}}, std::move(result)}};
    }
    return result;
  };
  if (not item.value_is_list) {
    TENZIR_ASSERT(item.values.size() == 1);
    return lower_source_value(item.values[0]);
  }
  auto connective = std::vector<ast::expression>{};
  for (auto const& value : item.values) {
    TRY(auto expr, lower_source_value(value));
    connective.emplace_back(std::move(expr));
  }
  // A list with `neq` requires every per-value inequality to hold. In
  // particular, `all|neq` must not negate a conjunction of equalities, which
  // would turn it into an almost-always-true disjunction of inequalities.
  if (semantics.negate or semantics.value_list_conjunction) {
    return expression_algebra::join<ast::binary_op::and_>(
      std::move(connective));
  }
  return expression_algebra::join<ast::binary_op::or_>(std::move(connective));
}

/// Lowers a named detection: items within a group are AND-linked, groups are
/// OR-linked (the YAML list-of-maps form).
auto lower_detection(ir::Detection const& detection)
  -> ParseResult<ast::expression> {
  auto disjuncts = std::vector<ast::expression>{};
  for (auto const& group : detection.groups) {
    auto conjuncts = std::vector<ast::expression>{};
    for (auto const& item : group) {
      TRY(auto expression, lower_item(item));
      conjuncts.emplace_back(std::move(expression));
    }
    disjuncts.emplace_back(
      expression_algebra::join<ast::binary_op::and_>(std::move(conjuncts)));
  }
  return expression_algebra::join<ast::binary_op::or_>(std::move(disjuncts));
}

/// Resolves all search identifiers matching a wildcard pattern, in the
/// deterministic order of the expression map.
auto search(ExpressionMap const& expressions, std::string_view pattern)
  -> std::vector<ast::expression> {
  auto result = std::vector<ast::expression>{};
  for (auto const& [name, expression] : expressions) {
    if (ir::pattern_matches(pattern, name)) {
      result.push_back(expression);
    }
  }
  return result;
}

/// Lowers a parsed condition tree by substituting search identifiers.
auto lower_condition(ir::Condition const& condition,
                     ExpressionMap const& expressions)
  -> ParseResult<ast::expression> {
  return match(
    condition.node,
    [&](ir::Identifier const& x) -> ParseResult<ast::expression> {
      if (auto i = expressions.find(x.name); i != expressions.end()) {
        return i->second;
      }
      // A bare wildcard pattern AND-links all matching identifiers.
      return expression_algebra::join<ast::binary_op::and_>(
        search(expressions, x.name));
    },
    [&](ir::Quantified const& x) -> ParseResult<ast::expression> {
      // A quantifier combines the matching search identifiers as-is; it
      // never rewrites the internal OR/AND linking of a single selection.
      // `1 of x` over exactly one match is therefore just `x`.
      auto const pattern
        = x.all_identifiers ? std::string_view{"*"} : x.pattern;
      auto matches = search(expressions, pattern);
      return x.quantifier == ir::Quantifier::all
               ? expression_algebra::join<ast::binary_op::and_>(
                   std::move(matches))
               : expression_algebra::join<ast::binary_op::or_>(
                   std::move(matches));
    },
    [&](ir::Negation const& x) -> ParseResult<ast::expression> {
      TRY(auto operand, lower_condition(*x.operand, expressions));
      return ast::expression{
        ast::unary_expr{{ast::unary_op::not_, {}}, std::move(operand)}};
    },
    [&](ir::Conjunction const& x) -> ParseResult<ast::expression> {
      TRY(auto left, lower_condition(*x.left, expressions));
      TRY(auto right, lower_condition(*x.right, expressions));
      return make_binary_expr(std::move(left), ast::binary_op::and_,
                              std::move(right));
    },
    [&](ir::Disjunction const& x) -> ParseResult<ast::expression> {
      TRY(auto left, lower_condition(*x.left, expressions));
      TRY(auto right, lower_condition(*x.right, expressions));
      return make_binary_expr(std::move(left), ast::binary_op::or_,
                              std::move(right));
    });
}

/// Compiles one YAML document through the typed Sigma IR into an executable
/// expression: parse and validate, lower named detections, then substitute
/// them into the condition.
auto compile_rule(data const& yaml) -> ParseResult<ast::expression> {
  TRY(auto document, ir::parse_document(yaml));
  auto const* rule = try_as<ir::DetectionRule>(document.content);
  TENZIR_ASSERT(rule); // unsupported kinds fail in `parse_document`
  auto expressions = ExpressionMap{};
  for (auto const& [name, detection] : rule->detections) {
    TRY(auto expression, lower_detection(detection));
    expressions[name] = std::move(expression);
  }
  // List-valued conditions are OR-linked queries.
  auto disjuncts = std::vector<ast::expression>{};
  for (auto const& condition : rule->conditions) {
    TRY(auto expr, lower_condition(condition, expressions));
    disjuncts.push_back(std::move(expr));
  }
  return expression_algebra::join<ast::binary_op::or_>(std::move(disjuncts));
}

struct RuleEntry {
  data yaml;
  ast::expression rule;
};

/// The collision-free identity of one rule document.
struct RuleKey {
  std::string origin;
  size_t document = 0;

  friend auto operator==(RuleKey const&, RuleKey const&) -> bool = default;
};

struct RuleKeyHash {
  auto operator()(RuleKey const& key) const noexcept -> size_t {
    auto const origin = std::hash<std::string>{}(key.origin);
    auto const document = std::hash<size_t>{}(key.document);
    return origin ^ (document + 0x9e3779b9 + (origin << 6) + (origin >> 2));
  }
};

/// Compiled rules in deterministic source order with constant-time updates.
struct RuleMap {
  auto insert_or_assign(RuleKey key, RuleEntry entry) -> void {
    if (auto existing = index.find(key); existing != index.end()) {
      entries[existing->second].second = std::move(entry);
      return;
    }
    index.emplace(key, entries.size());
    entries.emplace_back(std::move(key), std::move(entry));
  }

  /// Argument order, then file-discovery order, then YAML document order.
  std::vector<std::pair<RuleKey, RuleEntry>> entries;
  std::unordered_map<RuleKey, size_t, RuleKeyHash> index;
};

/// Tracks failed source and document revisions to avoid repeating the same
/// warning on every refresh.
struct ReloadState {
  auto should_emit(RuleKey const& key, uint64_t revision) -> bool {
    if (auto entry = failing_documents.find(key);
        entry != failing_documents.end() and entry->second == revision) {
      return false;
    }
    failing_documents.insert_or_assign(key, revision);
    return true;
  }

  auto should_emit(std::string const& origin, uint64_t revision) -> bool {
    if (auto entry = failing_sources.find(origin);
        entry != failing_sources.end() and entry->second == revision) {
      return false;
    }
    failing_sources.insert_or_assign(origin, revision);
    return true;
  }

  auto succeeded(RuleKey const& key) -> void {
    failing_documents.erase(key);
  }

  auto succeeded(std::string const& origin) -> void {
    failing_sources.erase(origin);
  }

  auto prune_documents(std::string const& origin, size_t count) -> void {
    std::erase_if(failing_documents, [&](auto const& entry) {
      return entry.first.origin == origin and entry.first.document >= count;
    });
  }

  auto reconcile(std::unordered_set<std::string> const& origins) -> void {
    std::erase_if(failing_documents, [&](auto const& entry) {
      return not origins.contains(entry.first.origin);
    });
    std::erase_if(failing_sources, [&](auto const& entry) {
      return not origins.contains(entry.first);
    });
  }

  std::unordered_map<RuleKey, uint64_t, RuleKeyHash> failing_documents;
  std::unordered_map<std::string, uint64_t> failing_sources;
};

/// The normalized rule sources of one operator instance.
struct SigmaSources {
  /// File and directory paths, in argument order.
  std::vector<std::string> paths;
  /// Inline YAML rule content, in argument order.
  std::vector<std::string> rules;
  /// The source argument, used to locate runtime diagnostics.
  location source = location::unknown;

  friend auto inspect(auto& f, SigmaSources& x) -> bool {
    return f.object(x)
      .pretty_name("SigmaSources")
      .fields(f.field("paths", x.paths), f.field("rules", x.rules),
              f.field("source", x.source));
  }
};

/// Adds the source argument to diagnostics that do not already have a known
/// location, including diagnostics produced by nested compilation helpers.
auto make_source_diagnostic_handler(diagnostic_handler& dh, location source)
  -> transforming_diagnostic_handler {
  return transforming_diagnostic_handler{
    dh, [source](diagnostic diag) {
      auto const has_location
        = std::ranges::any_of(diag.annotations, [](auto const& annotation) {
            return annotation.source != location::unknown;
          });
      if (not has_location and source != location::unknown) {
        diag.annotations.emplace_back(true, "", source);
      }
      return diag;
    }};
}

auto append_reason(diagnostic_builder builder, diagnostic const& reason)
  -> diagnostic_builder {
  builder = std::move(builder).note("{}", reason.message);
  for (auto const& note : reason.notes) {
    switch (note.kind) {
      case diagnostic_note_kind::note:
        builder = std::move(builder).note("{}", note.message);
        break;
      case diagnostic_note_kind::usage:
        builder = std::move(builder).usage("{}", note.message);
        break;
      case diagnostic_note_kind::hint:
        builder = std::move(builder).hint("{}", note.message);
        break;
      case diagnostic_note_kind::docs:
        builder = std::move(builder).docs("{}", note.message);
        break;
    }
  }
  return builder;
}

/// Copies every previous document artifact of a source into the next rule set.
/// Returns whether any artifact was retained.
auto retain_source(RuleMap const& previous, std::string const& origin,
                   RuleMap& next) -> bool {
  auto retained = false;
  for (auto const& [key, entry] : previous.entries) {
    if (key.origin == origin) {
      next.insert_or_assign(key, entry);
      retained = true;
    }
  }
  return retained;
}

/// Compiles every YAML document of one source. A failing initial load keeps
/// valid siblings, while a failing reload retains the previous source as one
/// unit so positional document identities cannot shift.
auto compile_source(std::string_view content, std::string const& origin,
                    RuleMap& next, RuleMap const& previous, ReloadState& state,
                    diagnostic_handler& dh) -> void {
  auto const revision = hash(content);
  auto documents = from_yaml_documents(content);
  if (not documents or documents->empty()) {
    auto const retained = retain_source(previous, origin, next);
    if (state.should_emit(origin, revision)) {
      auto builder
        = retained
            ? diagnostic::warning("sigma operator retains last known good "
                                  "version of '{}'",
                                  origin)
            : diagnostic::warning("sigma operator ignores source '{}'", origin);
      if (documents) {
        std::move(builder).note("source contains no YAML documents").emit(dh);
      } else {
        std::move(builder)
          .note("failed to parse yaml: {}", documents.error())
          .emit(dh);
      }
    }
    return;
  }
  state.succeeded(origin);
  state.prune_documents(origin, documents->size());
  auto const had_previous
    = std::ranges::any_of(previous.entries, [&](auto const& entry) {
        return entry.first.origin == origin;
      });
  auto replacements = RuleMap{};
  auto failed = false;
  auto const multiple = documents->size() > 1;
  for (auto index = size_t{0}; index < documents->size(); ++index) {
    auto const key = RuleKey{origin, index};
    auto const label = multiple ? fmt::format("{}#{}", origin, index) : origin;
    auto handle_failure = [&](auto&& emit_reason) {
      failed = true;
      if (not state.should_emit(key, revision)) {
        return;
      }
      auto builder
        = had_previous
            ? diagnostic::warning("sigma operator retains last known good "
                                  "version of source '{}'",
                                  origin)
            : diagnostic::warning("sigma operator ignores rule '{}'", label);
      emit_reason(std::move(builder));
    };
    auto& document = (*documents)[index];
    if (not is<record>(document)) {
      handle_failure([&](diagnostic_builder builder) {
        std::move(builder).note("rule is not a YAML dictionary").emit(dh);
      });
      continue;
    }
    auto compiled = compile_rule(document);
    if (compiled.is_err()) {
      auto reason = std::move(compiled).unwrap_err();
      handle_failure([&](diagnostic_builder builder) {
        append_reason(std::move(builder), reason).emit(dh);
      });
      continue;
    }
    auto rule = std::move(compiled).unwrap();
    auto provider = session_provider::make(dh);
    if (not resolve_entities(rule, provider.as_session())) {
      handle_failure([&](diagnostic_builder builder) {
        std::move(builder).note("failed to resolve sigma rule").emit(dh);
      });
      continue;
    }
    state.succeeded(key);
    replacements.insert_or_assign(key, RuleEntry{std::move(document),
                                                 std::move(rule)});
  }
  if (failed and had_previous) {
    std::ignore = retain_source(previous, origin, next);
    return;
  }
  for (auto& [key, entry] : replacements.entries) {
    next.insert_or_assign(std::move(key), std::move(entry));
  }
}

/// Discovers rule files across all path entries deterministically:
/// explicitly named files are always loaded, directories are traversed
/// recursively in lexicographic order with a `.yaml`/`.yml` filter, and
/// files reachable through overlapping entries are deduplicated.
auto collect_rule_files(std::vector<std::string> const& paths,
                        diagnostic_handler& dh)
  -> Option<std::vector<std::filesystem::path>> {
  auto result = std::vector<std::filesystem::path>{};
  auto seen = std::unordered_set<std::string>{};
  auto add_file = [&](std::filesystem::path const& path) {
    auto ec = std::error_code{};
    auto canonical = std::filesystem::weakly_canonical(path, ec);
    auto key = ec ? path.string() : canonical.string();
    if (seen.insert(std::move(key)).second) {
      result.push_back(path);
    }
  };
  auto visit = [&](auto&& self, std::filesystem::path const& path,
                   bool explicit_entry) -> bool {
    auto ec = std::error_code{};
    auto const is_directory = std::filesystem::is_directory(path, ec);
    if (ec) {
      diagnostic::warning("sigma operator failed to reload rules")
        .note("failed to inspect path '{}': {}", path.string(), ec.message())
        .emit(dh);
      return false;
    }
    if (is_directory) {
      auto entries = std::vector<std::filesystem::path>{};
      auto iterator = std::filesystem::directory_iterator{path, ec};
      if (ec) {
        diagnostic::warning("sigma operator failed to reload rules")
          .note("failed to enumerate directory '{}': {}", path.string(),
                ec.message())
          .emit(dh);
        return false;
      }
      auto const end = std::filesystem::directory_iterator{};
      while (iterator != end) {
        entries.push_back(iterator->path());
        iterator.increment(ec);
        if (ec) {
          diagnostic::warning("sigma operator failed to reload rules")
            .note("failed to enumerate directory '{}': {}", path.string(),
                  ec.message())
            .emit(dh);
          return false;
        }
      }
      std::ranges::sort(entries);
      for (auto const& entry : entries) {
        if (not self(self, entry, false)) {
          return false;
        }
      }
      return true;
    }
    if (explicit_entry or path.extension() == ".yaml"
        or path.extension() == ".yml") {
      add_file(path);
    }
    return true;
  };
  for (auto const& path : paths) {
    if (not visit(visit, path, true)) {
      return None{};
    }
  }
  return result;
}

/// Loads and compiles all rules from the given sources. Returns false when
/// file discovery fails, allowing the caller to keep the previous rule set.
auto load_rules(SigmaSources const& sources, RuleMap& next,
                RuleMap const& previous, ReloadState& state,
                diagnostic_handler& dh) -> bool {
  auto files = collect_rule_files(sources.paths, dh);
  if (not files) {
    return false;
  }
  auto origins = std::unordered_set<std::string>{};
  for (auto const& file : *files) {
    auto const origin = file.string();
    origins.insert(origin);
    auto content = tenzir::io::read(file);
    if (not content) {
      auto const retained = retain_source(previous, origin, next);
      if (state.should_emit(origin, hash(to_string(content.error())))) {
        auto builder
          = retained
              ? diagnostic::warning("sigma operator retains last known good "
                                    "version of '{}'",
                                    origin)
              : diagnostic::warning("sigma operator ignores rule '{}'", origin);
        std::move(builder)
          .note("failed to read file: {}", content.error())
          .emit(dh);
      }
      continue;
    }
    auto const view = std::string_view{
      reinterpret_cast<char const*>(content->data()), content->size()};
    compile_source(view, origin, next, previous, state, dh);
  }
  for (auto index = size_t{0}; index < sources.rules.size(); ++index) {
    auto const origin = fmt::format("<rules[{}]>", index);
    origins.insert(origin);
    compile_source(sources.rules[index], origin, next, previous, state, dh);
  }
  state.reconcile(origins);
  return true;
}

auto update_rules(SigmaSources const& sources, RuleMap& rules,
                  ReloadState& state, diagnostic_handler& dh) -> void {
  auto next = RuleMap{};
  if (load_rules(sources, next, rules, state, dh)) {
    rules = std::move(next);
  }
}

auto make_sigma_slice(const table_slice& input, const data& yaml,
                      const ast::expression& rule, diagnostic_handler& dh)
  -> Option<table_slice> {
  auto event = filter2(input, rule, dh, false);
  if (event.rows() == 0) {
    return None{};
  }
  auto [event_schema, event_array] = offset{}.get(event);
  auto [rule_schema, rule_array] = [&] {
    auto rule_builder = series_builder{};
    for (auto i = size_t{0}; i < event.rows(); ++i) {
      rule_builder.data(yaml);
    }
    return rule_builder.finish_assert_one_array();
  }();
  const auto result_schema = type{
    "tenzir.sigma",
    record_type{
      {"event", event_schema},
      {"rule", rule_schema},
    },
  };
  auto batch
    = arrow::RecordBatch::Make(result_schema.to_arrow_schema(),
                               detail::narrow<int64_t>(event.rows()),
                               {std::move(event_array), std::move(rule_array)});
  return table_slice{batch, result_schema};
}

// -- internal runtime functions ------------------------------------------

/// Recursively matches a regular expression against every string-valued leaf
/// of a series, including strings inside records and lists. Sets `matches[i]`
/// when any string leaf of row `i` matches. Non-string values are never
/// coerced.
auto keyword_match(series const& input, re2::RE2 const& regex,
                   std::vector<bool>& matches) -> void {
  TENZIR_ASSERT(std::cmp_equal(input.length(), matches.size()));
  if (auto const strings = input.as<string_type>()) {
    auto const& array = *strings->array;
    for (auto i = int64_t{0}; i < array.length(); ++i) {
      if (matches[i] or array.IsNull(i)) {
        continue;
      }
      auto const value = array.GetView(i);
      matches[i] = re2::RE2::FullMatch({value.data(), value.size()}, regex);
    }
    return;
  }
  if (auto const records = input.as<record_type>()) {
    // Flattening propagates parent-level nulls into each child so that
    // values inside null records can never match.
    auto index = int{0};
    for (auto const& field : records->type.fields()) {
      auto child = check(
        records->array->GetFlattenedField(index, tenzir::arrow_memory_pool()));
      keyword_match({field.type, std::move(child)}, regex, matches);
      ++index;
    }
    return;
  }
  if (auto const lists = input.as<list_type>()) {
    // `values()` spans the complete child array even for sliced lists. Scan
    // only the visible child range and translate the row offsets into it.
    auto const base = lists->array->value_offset(0);
    auto values = lists->list_values();
    auto nested = std::vector<bool>{};
    nested.resize(values.length());
    keyword_match(values, regex, nested);
    for (auto i = int64_t{0}; i < lists->length(); ++i) {
      if (matches[i] or lists->array->IsNull(i)) {
        continue;
      }
      auto const begin = nested.begin() + lists->array->value_offset(i) - base;
      auto const end
        = nested.begin() + lists->array->value_offset(i + 1) - base;
      matches[i] = std::ranges::any_of(begin, end, std::identity{});
    }
  }
}

/// Returns the series of the field with the given name, with parent-level
/// nulls propagated into the child, or `None` if the field does not exist.
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

/// Resolves a Sigma field name against a record series with deterministic
/// exact-key precedence: the complete name is first tried as an exact
/// top-level key; only if it is absent, dots denote nested traversal.
auto resolve_sigma_field(series const& input, std::string_view name) -> series {
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

/// Tests whether a Sigma field exists, following the same precedence as
/// `resolve_sigma_field`. Presence is defined by the field being part of the
/// schema with a non-null enclosing record, independent of its value.
auto resolve_sigma_has(series const& input, std::string_view name) -> series {
  auto builder = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
  check(builder.Reserve(input.length()));
  auto emit_present = [&](series const& parent) {
    // Present wherever the enclosing record is non-null.
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
  auto has_field = [](series const& s, std::string_view field) {
    auto const records = s.as<record_type>();
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

/// Internal function implementing Sigma keyword selections: matches a regex
/// recursively against every string-valued leaf of the input.
class SigmaKeywordsFunction final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "_sigma_keywords";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    auto pattern = located<std::string>{};
    TRY(argument_parser2::function(name())
          .positional("input", expr, "any")
          .positional("regex", pattern)
          .parse(inv, ctx));
    auto regex = std::make_shared<re2::RE2>(pattern.inner,
                                            re2::RE2::CannedOptions::Quiet);
    if (not regex->ok()) {
      diagnostic::error("failed to parse regex: {}", regex->error())
        .primary(pattern)
        .emit(ctx);
      return failure::promise();
    }
    return function_use::make(
      [expr = std::move(expr),
       regex = std::move(regex)](evaluator eval, session ctx) -> multi_series {
        TENZIR_UNUSED(ctx);
        return map_series(eval(expr), [&](series input) -> multi_series {
          auto matches = std::vector<bool>(input.length(), false);
          keyword_match(input, *regex, matches);
          auto builder = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
          check(builder.Reserve(input.length()));
          for (auto const value : matches) {
            check(builder.Append(value));
          }
          return series{bool_type{}, finish(builder)};
        });
      });
  }
};

/// Internal function implementing Sigma field resolution with exact-key
/// precedence over nested traversal.
class SigmaFieldFunction final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "_sigma_field";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    auto field = located<std::string>{};
    TRY(argument_parser2::function(name())
          .positional("input", expr, "record")
          .positional("field", field)
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr),
       field = std::move(field)](evaluator eval, session ctx) -> multi_series {
        TENZIR_UNUSED(ctx);
        return map_series(eval(expr), [&](series input) -> multi_series {
          return resolve_sigma_field(input, field.inner);
        });
      });
  }
};

/// Internal function implementing the Sigma `exists` modifier with the same
/// field-resolution precedence as `_sigma_field`.
class SigmaHasFunction final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "_sigma_has";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    auto field = located<std::string>{};
    TRY(argument_parser2::function(name())
          .positional("input", expr, "record")
          .positional("field", field)
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr),
       field = std::move(field)](evaluator eval, session ctx) -> multi_series {
        TENZIR_UNUSED(ctx);
        return map_series(eval(expr), [&](series input) -> multi_series {
          return resolve_sigma_has(input, field.inner);
        });
      });
  }
};

// -- argument handling -----------------------------------------------------

constexpr auto default_refresh_interval = std::chrono::seconds{5};

/// Converts a `string | list<string>` argument into a non-empty string list.
auto to_string_list(located<data> const& value, std::string_view name)
  -> Result<std::vector<std::string>, diagnostic> {
  if (auto const* str = try_as<std::string>(&value.inner)) {
    return std::vector<std::string>{*str};
  }
  auto const* elements = try_as<list>(&value.inner);
  if (not elements) {
    return Err{
      diagnostic::error("`{}` expected `string` or `list<string>`", name)
        .primary(value.source)
        .done()};
  }
  if (elements->empty()) {
    return Err{diagnostic::error("`{}` must not be an empty list", name)
                 .primary(value.source)
                 .done()};
  }
  auto result = std::vector<std::string>{};
  result.reserve(elements->size());
  for (auto const& element : *elements) {
    auto const* str = try_as<std::string>(&element);
    if (not str) {
      return Err{
        diagnostic::error("`{}` expected `string` or `list<string>`", name)
          .primary(value.source)
          .done()};
    }
    result.push_back(*str);
  }
  return result;
}

/// Validates the operator arguments and normalizes them into rule sources.
/// Exactly one source form must be present; `refresh_interval` only applies
/// to filesystem-backed sources.
auto normalize_sources(Option<located<std::string>> const& legacy_path,
                       Option<located<data>> const& path,
                       Option<located<data>> const& rules,
                       Option<located<duration>> const& refresh_interval,
                       location operator_location)
  -> Result<SigmaSources, diagnostic> {
  auto const source_count = (legacy_path.is_some() ? 1 : 0)
                            + (path.is_some() ? 1 : 0)
                            + (rules.is_some() ? 1 : 0);
  if (source_count != 1) {
    return Err{diagnostic::error("`sigma` requires exactly one rule source")
                 .primary(operator_location)
                 .hint("pass `path=` for rule files and directories or "
                       "`rules=` for inline YAML rules")
                 .done()};
  }
  auto result = SigmaSources{.source = operator_location};
  if (legacy_path) {
    result.paths.push_back(legacy_path->inner);
    if (legacy_path->source != location::unknown) {
      result.source = legacy_path->source;
    }
  }
  if (path) {
    TRY(result.paths, to_string_list(*path, "path"));
    if (path->source != location::unknown) {
      result.source = path->source;
    }
  }
  if (rules) {
    TRY(result.rules, to_string_list(*rules, "rules"));
    if (rules->source != location::unknown) {
      result.source = rules->source;
    }
    if (refresh_interval) {
      return Err{
        diagnostic::error("`refresh_interval` cannot be used with `rules`")
          .primary(refresh_interval->source)
          .note("embedded rule content cannot change independently of the "
                "pipeline")
          .done()};
    }
  }
  if (refresh_interval and refresh_interval->inner <= duration::zero()) {
    return Err{diagnostic::error("`refresh_interval` must be a positive "
                                 "duration")
                 .primary(refresh_interval->source)
                 .done()};
  }
  return result;
}

/// Compiles inline rule content at pipeline-construction time so that
/// diagnostics anchor at the TQL argument and carry the list element and
/// YAML document indices.
auto validate_inline_rules(std::vector<std::string> const& rules,
                           location source) -> Option<diagnostic> {
  for (auto index = size_t{0}; index < rules.size(); ++index) {
    auto documents = from_yaml_documents(rules[index]);
    if (not documents) {
      return diagnostic::error("invalid YAML in `rules`")
        .primary(source)
        .note("list element {}: {}", index, documents.error())
        .done();
    }
    if (documents->empty()) {
      return diagnostic::error("invalid Sigma rule in `rules`")
        .primary(source)
        .note("list element {}: source contains no YAML documents", index)
        .done();
    }
    for (auto doc = size_t{0}; doc < documents->size(); ++doc) {
      auto const& document = (*documents)[doc];
      if (not is<record>(document)) {
        return diagnostic::error("invalid Sigma rule in `rules`")
          .primary(source)
          .note("list element {}, document {}: rule is not a YAML dictionary",
                index, doc)
          .done();
      }
      auto compiled = compile_rule(document);
      if (compiled.is_err()) {
        auto reason = std::move(compiled).unwrap_err();
        return diagnostic::error("invalid Sigma rule in `rules`")
          .primary(source)
          .note("list element {}, document {}: {}", index, doc, reason.message)
          .done();
      }
    }
  }
  return None{};
}

class sigma_operator final : public crtp_operator<sigma_operator> {
public:
  sigma_operator() = default;

  sigma_operator(duration refresh_interval, SigmaSources sources)
    : refresh_interval_{refresh_interval}, sources_{std::move(sources)} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto rules = RuleMap{};
    auto reload_state = ReloadState{};
    auto diagnostics
      = make_source_diagnostic_handler(ctrl.diagnostics(), sources_.source);
    update_rules(sources_, rules, reload_state, diagnostics);
    auto last_update = std::chrono::steady_clock::now();
    co_yield {}; // signal that we're done initializing
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      // Inline rules are part of the operator plan and never change.
      auto const now = std::chrono::steady_clock::now();
      if (not sources_.paths.empty()
          and now - last_update > refresh_interval_) {
        update_rules(sources_, rules, reload_state, diagnostics);
        last_update = now;
      }
      for (auto const& [_, entry] : rules.entries) {
        if (auto result
            = make_sigma_slice(slice, entry.yaml, entry.rule, diagnostics)) {
          co_yield std::move(*result);
        }
      }
    }
  }

  auto name() const -> std::string override {
    return "sigma";
  }

  auto location() const -> operator_location override {
    // Filesystem paths are relative to the process constructing the pipeline.
    // Inline rules have no local resources and can remain with upstream.
    return sources_.paths.empty() ? operator_location::anywhere
                                  : operator_location::local;
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, sigma_operator& x) -> bool {
    return f.object(x)
      .pretty_name("sigma_operator")
      .fields(f.field("refresh_interval", x.refresh_interval_),
              f.field("sources", x.sources_));
  }

private:
  duration refresh_interval_ = {};
  SigmaSources sources_;
};

struct SigmaArgs {
  Option<located<std::string>> legacy_path;
  Option<located<data>> path;
  Option<located<data>> rules;
  Option<located<duration>> refresh_interval;
  location operator_location = location::unknown;
};

class Sigma final : public Operator<table_slice, table_slice> {
public:
  explicit Sigma(SigmaArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto sources
      = normalize_sources(args_.legacy_path, args_.path, args_.rules,
                          args_.refresh_interval, args_.operator_location);
    // Argument validation already ran at pipeline-construction time.
    TENZIR_ASSERT(sources.is_ok());
    sources_ = std::move(sources).unwrap();
    refresh_interval_ = args_.refresh_interval ? args_.refresh_interval->inner
                                               : default_refresh_interval;
    auto diagnostics
      = make_source_diagnostic_handler(ctx.dh(), sources_.source);
    update_rules(sources_, rules_, reload_state_, diagnostics);
    last_update_ = std::chrono::steady_clock::now();
    co_return;
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    // Inline rules are part of the operator plan and never change.
    auto diagnostics
      = make_source_diagnostic_handler(ctx.dh(), sources_.source);
    auto const now = std::chrono::steady_clock::now();
    if (not sources_.paths.empty() and now - last_update_ > refresh_interval_) {
      update_rules(sources_, rules_, reload_state_, diagnostics);
      last_update_ = now;
    }
    for (auto const& [_, entry] : rules_.entries) {
      if (auto result
          = make_sigma_slice(input, entry.yaml, entry.rule, diagnostics)) {
        co_await push(std::move(*result));
      }
    }
  }

private:
  SigmaArgs args_;
  SigmaSources sources_;
  duration refresh_interval_ = default_refresh_interval;
  RuleMap rules_;
  ReloadState reload_state_;
  // Rules are reloaded from disk in `start()`, and `last_update_` uses
  // `steady_clock`, so the default no-op snapshot behavior is sufficient.
  std::chrono::steady_clock::time_point last_update_ = {};
};

class plugin final : public virtual operator_plugin<sigma_operator>,
                     public virtual operator_factory_plugin,
                     public virtual OperatorPlugin {
public:
  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto legacy_path = Option<located<std::string>>{};
    auto path = Option<located<data>>{};
    auto rules = Option<located<data>>{};
    auto refresh_interval = Option<located<duration>>{};
    TRY(argument_parser2::operator_("sigma")
          .positional("legacy_path", legacy_path)
          .named("path", path)
          .named("rules", rules)
          .named("refresh_interval", refresh_interval)
          .parse(inv, ctx));
    auto sources = normalize_sources(legacy_path, path, rules, refresh_interval,
                                     inv.self.get_location());
    if (sources.is_err()) {
      std::move(sources).unwrap_err().modify().emit(ctx);
      return failure::promise();
    }
    if (legacy_path) {
      diagnostic::warning("passing the path positionally is deprecated")
        .primary(legacy_path->source)
        .hint("use `path={:?}` instead", legacy_path->inner)
        .emit(ctx);
    }
    if (rules) {
      if (auto error
          = validate_inline_rules(sources.unwrap().rules, rules->source)) {
        std::move(*error).modify().emit(ctx);
        return failure::promise();
      }
    }
    auto const interval
      = refresh_interval ? refresh_interval->inner : default_refresh_interval;
    return std::make_unique<sigma_operator>(interval,
                                            std::move(sources).unwrap());
  }

  auto describe() const -> Description override {
    auto d = Describer<SigmaArgs, Sigma>{};
    d.parallelizable();
    auto legacy_path = d.positional("legacy_path", &SigmaArgs::legacy_path);
    auto path = d.named("path", &SigmaArgs::path);
    auto rules = d.named("rules", &SigmaArgs::rules);
    auto refresh_interval
      = d.named("refresh_interval", &SigmaArgs::refresh_interval);
    d.operator_location(&SigmaArgs::operator_location);
    d.validate(
      [legacy_path, path, rules, refresh_interval](DescribeCtx& ctx) -> Empty {
        auto const legacy_value = ctx.get(legacy_path);
        auto const path_value = ctx.get(path);
        auto const rules_value = ctx.get(rules);
        auto const refresh_value = ctx.get(refresh_interval);
        auto to_option = [](auto const& value) {
          using Value = std::remove_cvref_t<decltype(*value)>;
          return value ? Option<Value>{*value} : Option<Value>{};
        };
        auto sources
          = normalize_sources(to_option(legacy_value), to_option(path_value),
                              to_option(rules_value), to_option(refresh_value),
                              ctx.operator_location());
        if (sources.is_err()) {
          static_cast<diagnostic_handler&>(ctx).emit(
            std::move(sources).unwrap_err());
          return {};
        }
        if (legacy_value) {
          diagnostic::warning("passing the path positionally is deprecated")
            .primary(legacy_value->source)
            .hint("use `path={:?}` instead", legacy_value->inner)
            .emit(ctx);
        }
        if (rules_value) {
          if (auto error = validate_inline_rules(sources.unwrap().rules,
                                                 rules_value->source)) {
            static_cast<diagnostic_handler&>(ctx).emit(std::move(*error));
          }
        }
        return {};
      });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::sigma

TENZIR_REGISTER_PLUGIN(tenzir::plugins::sigma::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::sigma::SigmaKeywordsFunction)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::sigma::SigmaFieldFunction)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::sigma::SigmaHasFunction)
