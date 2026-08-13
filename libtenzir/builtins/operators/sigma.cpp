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

#include <array>
#include <cctype>
#include <chrono>
#include <filesystem>
#include <functional>
#include <ranges>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
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

auto flatten(ast::expression x, ast::binary_op op,
             std::vector<ast::expression>& result) -> void {
  if (auto* binary = try_as<ast::binary_expr>(x); binary and binary->op == op) {
    flatten(std::move(binary->left), op, result);
    flatten(std::move(binary->right), op, result);
    return;
  }
  result.push_back(std::move(x));
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

using RuleMap = std::unordered_map<std::string, RuleEntry>;

auto emit_ignored_rule(const std::filesystem::path& path, diagnostic reason,
                       diagnostic_handler& dh) -> void {
  auto builder
    = diagnostic::warning("sigma operator ignores rule '{}'", path.string())
        .note("{}", reason.message);
  for (const auto& note : reason.notes) {
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
  std::move(builder).emit(dh);
}

auto load_rules(const std::filesystem::path& path, RuleMap& rules,
                diagnostic_handler& dh) -> void {
  if (std::filesystem::is_directory(path)) {
    for (const auto& entry : std::filesystem::directory_iterator(path)) {
      load_rules(entry.path(), rules, dh);
    }
    return;
  }
  if (path.extension() != ".yml" and path.extension() != ".yaml") {
    // We silently ignore non-yaml files.
    return;
  }
  auto query = tenzir::io::read(path);
  if (not query) {
    diagnostic::warning("sigma operator ignores rule '{}'", path.string())
      .note("failed to read file: {}", query.error())
      .emit(dh);
    return;
  }
  auto query_str = std::string_view{
    reinterpret_cast<const char*>(query->data()),
    reinterpret_cast<const char*>(query->data() + query->size())}; // NOLINT
  auto yaml = from_yaml(query_str);
  if (not yaml) {
    diagnostic::warning("sigma operator ignores rule '{}'", path.string())
      .note("failed to parse yaml: {}", yaml.error())
      .emit(dh);
    return;
  }
  if (not is<record>(*yaml)) {
    diagnostic::warning("sigma operator ignores rule '{}'", path.string())
      .note("rule is not a YAML dictionary")
      .emit(dh);
    return;
  }
  auto parsed_rule = compile_rule(*yaml);
  if (parsed_rule.is_err()) {
    emit_ignored_rule(path, std::move(parsed_rule).unwrap_err(), dh);
    return;
  }
  auto rule = std::move(parsed_rule).unwrap();
  auto provider = session_provider::make(dh);
  if (not resolve_entities(rule, provider.as_session())) {
    diagnostic::warning("sigma operator ignores rule '{}'", path.string())
      .note("failed to resolve sigma rule")
      .emit(dh);
    return;
  }
  rules[path.string()] = {std::move(*yaml), std::move(rule)};
}

auto update_rules(const std::filesystem::path& path, RuleMap& rules,
                  diagnostic_handler& dh) -> void {
  auto old_rules = std::exchange(rules, {});
  load_rules(path, rules, dh);
  for (const auto& [rule_path, rule] : rules) {
    const auto old_rule = old_rules.find(rule_path);
    if (old_rule == old_rules.end()) {
      TENZIR_VERBOSE("added Sigma rule {}", rule_path);
    } else if (old_rule->second.yaml != rule.yaml) {
      TENZIR_VERBOSE("updated Sigma rule {}", rule_path);
    }
  }
  for (const auto& [rule_path, _] : old_rules) {
    if (not rules.contains(rule_path)) {
      TENZIR_VERBOSE("removed Sigma rule {}", rule_path);
    }
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

class sigma_operator final : public crtp_operator<sigma_operator> {
public:
  sigma_operator() = default;

  explicit sigma_operator(duration refresh_interval, std::string path)
    : refresh_interval_{refresh_interval}, path_{std::move(path)} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto rules = RuleMap{};
    auto path = std::filesystem::path{path_};
    update_rules(path, rules, ctrl.diagnostics());
    auto last_update = std::chrono::steady_clock::now();
    co_yield {}; // signal that we're done initializing
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto now = std::chrono::steady_clock::now();
      if (now - last_update > refresh_interval_) {
        update_rules(path, rules, ctrl.diagnostics());
        last_update = now;
      }
      for (const auto& [_, entry] : rules) {
        if (auto result = make_sigma_slice(slice, entry.yaml, entry.rule,
                                           ctrl.diagnostics())) {
          co_yield std::move(*result);
        }
      }
    }
  }

  auto name() const -> std::string override {
    return "sigma";
  }

  auto location() const -> operator_location override {
    // The operator is referring to files, and the user likely assumes that to
    // be relative to the current process, so we default to local here.
    return operator_location::local;
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
              f.field("path", x.path_));
  }

private:
  duration refresh_interval_ = {};
  std::string path_ = {};
};

struct SigmaArgs {
  std::string path;
  duration refresh_interval = std::chrono::seconds{5};
};

class Sigma final : public Operator<table_slice, table_slice> {
public:
  explicit Sigma(SigmaArgs args) : args_{std::move(args)}, path_{args_.path} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    update_rules(path_, rules_, ctx.dh());
    last_update_ = std::chrono::steady_clock::now();
    co_return;
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto now = std::chrono::steady_clock::now();
    if (now - last_update_ > args_.refresh_interval) {
      update_rules(path_, rules_, ctx.dh());
      last_update_ = now;
    }
    for (const auto& [_, entry] : rules_) {
      if (auto result
          = make_sigma_slice(input, entry.yaml, entry.rule, ctx.dh())) {
        co_await push(std::move(*result));
      }
    }
  }

private:
  SigmaArgs args_;
  std::filesystem::path path_;
  RuleMap rules_;
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
    auto refresh_interval = Option<located<duration>>{};
    auto path = std::string{};
    argument_parser2::operator_("sigma")
      .positional("path", path)
      .named("refresh_interval", refresh_interval)
      .parse(inv, ctx)
      .ignore();
    auto interval
      = refresh_interval ? refresh_interval->inner : std::chrono::seconds{5};
    if (refresh_interval and interval <= duration::zero()) {
      diagnostic::error("`refresh_interval` must be a positive duration")
        .primary(refresh_interval.value())
        .emit(ctx);
      return failure::promise();
    }
    return std::make_unique<sigma_operator>(interval, std::move(path));
  }

  auto describe() const -> Description override {
    auto d = Describer<SigmaArgs, Sigma>{};
    d.parallelizable();
    d.positional("path", &SigmaArgs::path);
    auto refresh_interval
      = d.named_optional("refresh_interval", &SigmaArgs::refresh_interval);
    d.validate([refresh_interval](DescribeCtx& ctx) -> Empty {
      if (auto value = ctx.get(refresh_interval);
          value and *value <= duration::zero()) {
        diagnostic::error("`refresh_interval` must be a positive duration")
          .primary(ctx.get_location(refresh_interval).value())
          .emit(ctx);
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
