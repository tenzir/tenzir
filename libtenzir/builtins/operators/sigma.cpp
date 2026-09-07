//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "sigma/ocsf.hpp"
#include "sigma/plan_cache.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/async/metrics.hpp>
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
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/filter.hpp>
#include <tenzir/tql2/resolve.hpp>
#include <tenzir/uuid.hpp>

#include <arrow/compute/api_vector.h>
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
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace tenzir::plugins::sigma {

TENZIR_ENUM(sigma_format, ocsf, plain);
TENZIR_ENUM(sigma_mapping, automatic, direct);

// TODO: A lot of code in here is directly copied from
// src/concept/parseable/expression.cpp. We should factor the implementation in
// the future.

namespace {

namespace ir = tenzir::sigma;

using FieldSource = ocsf::FieldProjection;

struct FieldBinding {
  FieldSource source;
  std::string value_column;
  std::string presence_column;
  /// Set when the evidence path is the same for every row; the common case.
  Option<std::string> constant_evidence_path;
  /// Non-empty only when the evidence path varies per row, e.g. fallbacks.
  std::string evidence_path_column;
};

using FieldBindings = detail::flat_map<std::string, FieldBinding>;

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

/// Builds a direct reference to a private evaluation column.
auto make_private_field_expr(std::string_view name) -> ast::expression {
  return ast::expression{ast::root_field{
    ast::identifier{std::string{name}, location::unknown}, true}};
}

/// Builds the expression that resolves a Sigma field name. Planned fields
/// reference their private evaluation columns. Otherwise, names without dots
/// become plain field accesses, while dotted names use `_sigma_field` for
/// deterministic exact-key precedence over nested traversal.
auto make_field_expr(std::string_view name,
                     Option<FieldBindings const&> bindings = None{})
  -> ast::expression {
  if (bindings) {
    auto const binding = bindings->find(name);
    TENZIR_ASSERT(binding != bindings->end());
    return make_private_field_expr(binding->second.value_column);
  }
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
auto make_field_exists_expr(std::string_view name,
                            Option<FieldBindings const&> bindings = None{})
  -> ast::expression {
  if (bindings) {
    auto const binding = bindings->find(name);
    TENZIR_ASSERT(binding != bindings->end());
    return make_private_field_expr(binding->second.presence_column);
  }
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
    ast::entity{{ast::identifier{"_sigma_regex", location::unknown}}},
    {std::move(field), ast::constant{std::move(regex), location::unknown}},
    location::unknown,
    false};
}

auto make_binary_expr(ast::expression left, ast::binary_op op,
                      ast::expression right) -> ast::expression {
  return ast::binary_expr{std::move(left), op, std::move(right)};
}

// -- lowering: Sigma IR -> TQL expressions -------------------------------

/// Encodes one UTF-8 Sigma string as UTF-16 code units in the requested byte
/// order, optionally with a byte-order mark.
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

/// Converts a rule-side value to the representation produced by a semantic
/// field projection. Literal fields and projections without special value
/// semantics preserve the original value.
auto lower_rule_value(FieldSource const& source, data const& value)
  -> ParseResult<data> {
  auto projected = ocsf::project_rule_value(source, value);
  if (projected.is_err()) {
    return parse_failure("{}", std::move(projected).unwrap_err());
  }
  return std::move(projected).unwrap();
}

/// Lowers one detection item (`field|modifiers: value(s)`) into a TQL
/// predicate. Optional bindings redirect fields to semantic evaluation
/// columns, and the keyword limit keeps private columns out of keyword scans.
auto lower_item_predicate(ir::DetectionItem const& item,
                          Option<FieldBindings const&> bindings,
                          Option<size_t> keyword_field_count)
  -> ParseResult<ast::expression> {
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
      args.emplace_back(ast::constant{
        keyword_field_count ? detail::narrow<int64_t>(*keyword_field_count)
                            : int64_t{-1},
        location::unknown});
      disjuncts.push_back(
        make_function_expr("_sigma_keywords", std::move(args)));
    }
    return expression_algebra::join<ast::binary_op::or_>(std::move(disjuncts));
  }
  // `exists` is the sole modifier and tests field presence.
  if (semantics.exists) {
    TENZIR_ASSERT(item.values.size() == 1);
    auto expr = make_field_exists_expr(field, bindings);
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
    auto expr = make_field_expr(field, bindings);
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
      predicates.push_back(
        make_binary_expr(make_field_expr(referenced_field, bindings),
                         ast::binary_op::neq, make_constant(caf::none)));
      predicates.push_back(make_binary_expr(
        make_lhs(), semantics.negate ? ast::binary_op::neq : ast::binary_op::eq,
        make_field_expr(referenced_field, bindings)));
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
    = [&](data const& source_value) -> ParseResult<ast::expression> {
    auto value = source_value;
    if (bindings and not semantics.fieldref) {
      auto const binding = bindings->find(field);
      TENZIR_ASSERT(binding != bindings->end());
      // A stringified projection compares lexicographically, which breaks
      // ordered predicates; fail closed instead of matching wrongly.
      auto const ordered = semantics.op == ast::binary_op::lt
                           or semantics.op == ast::binary_op::leq
                           or semantics.op == ast::binary_op::gt
                           or semantics.op == ast::binary_op::geq;
      if (ordered
          and is<ocsf::StringifyRuleValue>(binding->second.source.rule_value)) {
        return parse_failure("ordered comparison on `{}` is not supported: "
                             "the projected value is a string",
                             field);
      }
      TRY(value, lower_rule_value(binding->second.source, source_value));
    }
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

/// Lowers one detection item into a two-valued TQL expression. A predicate
/// over an absent or null field is null in TQL's three-valued logic, and
/// `not null` stays null. Sigma is two-valued: an item whose field is absent
/// does not match, so `selection and not filter` fires when the filter's
/// field is missing from the event.
auto lower_item(ir::DetectionItem const& item,
                Option<FieldBindings const&> bindings = None{},
                Option<size_t> keyword_field_count = None{})
  -> ParseResult<ast::expression> {
  TRY(auto predicate,
      lower_item_predicate(item, bindings, keyword_field_count));
  auto args = std::vector<ast::expression>{};
  args.push_back(std::move(predicate));
  args.emplace_back(ast::constant{false, location::unknown});
  return make_function_expr("otherwise", std::move(args));
}

/// Lowers a named detection: items within a group are AND-linked, groups are
/// OR-linked (the YAML list-of-maps form).
auto lower_detection(ir::Detection const& detection,
                     Option<FieldBindings const&> bindings = None{},
                     Option<size_t> keyword_field_count = None{})
  -> ParseResult<ast::expression> {
  auto disjuncts = std::vector<ast::expression>{};
  for (auto const& group : detection.groups) {
    auto conjuncts = std::vector<ast::expression>{};
    for (auto const& item : group) {
      TRY(auto expression, lower_item(item, bindings, keyword_field_count));
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

// -- OCSF detection findings -------------------------------------------------

/// Compiled matching metadata of one detection item, used to derive match
/// provenance without re-parsing modifiers.
struct ItemArtifact {
  ast::expression expression;
  std::string field;
  std::string matcher;
  bool case_insensitive = true;
  bool negated = false;
  bool keyword = false;
};

/// Compiled artifacts of one named search identifier: the identifier-level
/// expression plus per-item expressions in group structure (OR of ANDs).
struct IdentifierArtifact {
  std::string name;
  ast::expression expression;
  std::vector<std::vector<ItemArtifact>> groups;
};

/// Derives the matcher-kind label of one detection item.
auto matcher_kind(ir::DetectionItem const& item, ItemSemantics const& semantics)
  -> std::string {
  if (item.kind == ir::DetectionItem::ItemKind::keyword) {
    return "keyword";
  }
  if (semantics.exists) {
    return "exists";
  }
  if (semantics.fieldref) {
    return "fieldref";
  }
  if (semantics.raw_regex) {
    return "regex";
  }
  if (semantics.time_part) {
    return *semantics.time_part;
  }
  if (semantics.windash) {
    return "windash";
  }
  if (semantics.contains) {
    return "contains";
  }
  if (semantics.wildcard_suffix and not semantics.wildcard_prefix) {
    return "startswith";
  }
  if (semantics.wildcard_prefix and not semantics.wildcard_suffix) {
    return "endswith";
  }
  switch (semantics.op) {
    case ast::binary_op::lt:
      return "lt";
    case ast::binary_op::leq:
      return "lte";
    case ast::binary_op::gt:
      return "gt";
    case ast::binary_op::geq:
      return "gte";
    case ast::binary_op::in:
      return "cidr";
    default:
      break;
  }
  return "equals";
}

/// Lowers one named detection into its identifier artifact.
auto lower_identifier_artifact(std::string name, ir::Detection const& detection,
                               Option<FieldBindings const&> bindings = None{},
                               Option<size_t> keyword_field_count = None{})
  -> ParseResult<IdentifierArtifact> {
  auto result = IdentifierArtifact{};
  result.name = std::move(name);
  auto disjuncts = std::vector<ast::expression>{};
  for (auto const& group : detection.groups) {
    auto artifacts = std::vector<ItemArtifact>{};
    auto conjuncts = std::vector<ast::expression>{};
    for (auto const& item : group) {
      TRY(auto expression, lower_item(item, bindings, keyword_field_count));
      TRY(auto semantics, parse_semantics(item));
      artifacts.push_back(ItemArtifact{
        expression,
        item.field.raw,
        matcher_kind(item, semantics),
        semantics.case_insensitive,
        semantics.negate or (semantics.exists and not as<bool>(item.values[0])),
        item.kind == ir::DetectionItem::ItemKind::keyword,
      });
      conjuncts.push_back(std::move(expression));
    }
    disjuncts.push_back(
      expression_algebra::join<ast::binary_op::and_>(std::move(conjuncts)));
    result.groups.push_back(std::move(artifacts));
  }
  result.expression
    = expression_algebra::join<ast::binary_op::or_>(std::move(disjuncts));
  return result;
}

/// Static MITRE ATT&CK tactic identifiers, keyed by their Sigma tag form.
constexpr auto attack_tactics
  = std::array<std::pair<std::string_view, std::string_view>, 14>{{
    {"reconnaissance", "TA0043"},
    {"resource_development", "TA0042"},
    {"initial_access", "TA0001"},
    {"execution", "TA0002"},
    {"persistence", "TA0003"},
    {"privilege_escalation", "TA0004"},
    {"defense_evasion", "TA0005"},
    {"credential_access", "TA0006"},
    {"discovery", "TA0007"},
    {"lateral_movement", "TA0008"},
    {"collection", "TA0009"},
    {"command_and_control", "TA0011"},
    {"exfiltration", "TA0010"},
    {"impact", "TA0040"},
  }};

/// Derives the OCSF `attacks` list from a rule's `attack.*` tags.
auto make_attacks(data const& yaml) -> data {
  auto const* document = try_as<record>(&yaml);
  if (not document) {
    return list{};
  }
  auto entry = document->find("tags");
  if (entry == document->end()) {
    return list{};
  }
  auto const* tags = try_as<list>(&entry->second);
  if (not tags) {
    return list{};
  }
  auto tactics = list{};
  auto techniques = std::vector<std::string>{};
  for (auto const& tag : *tags) {
    auto const* str = try_as<std::string>(&tag);
    if (not str or not str->starts_with("attack.")) {
      continue;
    }
    auto const name = std::string_view{*str}.substr(7);
    if ((name.starts_with('t') or name.starts_with('T')) and name.size() > 1
        and std::isdigit(static_cast<unsigned char>(name[1])) != 0) {
      auto technique = std::string{name};
      technique[0] = 'T';
      techniques.push_back(std::move(technique));
      continue;
    }
    for (auto const& [tactic, uid] : attack_tactics) {
      if (name == tactic) {
        tactics.emplace_back(record{{"uid", std::string{uid}}});
        break;
      }
    }
  }
  auto result = list{};
  if (techniques.empty()) {
    if (not tactics.empty()) {
      result.emplace_back(record{{"tactics", tactics}});
    }
    return result;
  }
  for (auto& technique : techniques) {
    auto attack = record{};
    if (not tactics.empty()) {
      attack.emplace("tactics", tactics);
    }
    attack.emplace("technique", record{{"uid", std::move(technique)}});
    result.push_back(std::move(attack));
  }
  return result;
}

/// Rule-derived finding fragments, cached once per compiled rule revision.
struct FindingTemplate {
  int64_t severity_id = 0;
  Option<std::string> title;
  std::string analytic_uid;
  data analytic;
  data policy;
  data attacks;
  data data_sources;
};

auto make_finding_template(ir::DetectionRule const& rule, data const& yaml)
  -> FindingTemplate {
  auto result = FindingTemplate{};
  // Sigma `level` maps to `severity_id`; a missing level is Unknown.
  auto const level = rule.metadata.level;
  if (level == "informational") {
    result.severity_id = 1;
  } else if (level == "low") {
    result.severity_id = 2;
  } else if (level == "medium") {
    result.severity_id = 3;
  } else if (level == "high") {
    result.severity_id = 4;
  } else if (level == "critical") {
    result.severity_id = 5;
  }
  result.title = rule.metadata.title;
  // The analytic identity is the Sigma `id` or a deterministic content
  // fingerprint.
  result.analytic_uid = rule.metadata.id.unwrap_or_else([&] {
    return fmt::format("sigma:{:016x}", hash(yaml));
  });
  auto analytic = record{};
  analytic.emplace("type_id", int64_t{1});
  analytic.emplace("type", "Rule");
  analytic.emplace("uid", result.analytic_uid);
  if (rule.metadata.name) {
    analytic.emplace("name", *rule.metadata.name);
  } else if (rule.metadata.title) {
    analytic.emplace("name", *rule.metadata.title);
  }
  result.analytic = std::move(analytic);
  // `policy` records the exact applied rule; `data` holds the complete
  // parsed document, including filter adjustments.
  auto policy = record{};
  policy.emplace("uid", result.analytic_uid);
  if (rule.metadata.title) {
    policy.emplace("name", *rule.metadata.title);
  }
  policy.emplace("is_applied", true);
  policy.emplace("data", ir::to_record(rule));
  result.policy = std::move(policy);
  result.attacks = make_attacks(yaml);
  auto data_sources = list{};
  if (rule.log_source.category) {
    data_sources.emplace_back(
      fmt::format("category={}", *rule.log_source.category));
  }
  if (rule.log_source.product) {
    data_sources.emplace_back(
      fmt::format("product={}", *rule.log_source.product));
  }
  if (rule.log_source.service) {
    data_sources.emplace_back(
      fmt::format("service={}", *rule.log_source.service));
  }
  result.data_sources = std::move(data_sources);
  return result;
}

struct RuleEntry {
  data yaml;
  std::string label;
  uint64_t revision = 0;
  /// The parsed rule before filter application, used to recombine retained
  /// rules with refreshed filters.
  ir::DetectionRule detection;
  /// The filter-adjusted rule that actually executes; provenance and the
  /// applied `policy` derive from it.
  ir::DetectionRule adjusted;
  ast::expression rule;
  std::vector<IdentifierArtifact> identifiers;
  FindingTemplate finding;
  /// The location of the rule's YAML document inside an inline `rules`
  /// argument, or unknown for rules loaded from files.
  location source = location::unknown;
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
  uint64_t revision = 0;
};

auto rule_map_revision(RuleMap const& rules) -> uint64_t {
  auto result = uint64_t{0};
  for (auto const& [key, entry] : rules.entries) {
    result = hash(result, key.origin, key.document, entry.revision,
                  entry.finding.policy);
  }
  return result;
}

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

  auto
  reconcile_filter_applications(std::unordered_set<std::string> const& active)
    -> void {
    std::erase_if(failing_sources, [&](auto const& entry) {
      return entry.first.starts_with("filter:")
             and not active.contains(entry.first);
    });
  }

  auto reconcile(std::unordered_set<std::string> const& origins) -> void {
    std::erase_if(failing_documents, [&](auto const& entry) {
      return not origins.contains(entry.first.origin);
    });
    std::erase_if(failing_sources, [&](auto const& entry) {
      // Active filter-application memos were reconciled during assembly.
      return not entry.first.starts_with("filter:")
             and not origins.contains(entry.first);
    });
  }

  std::unordered_map<RuleKey, uint64_t, RuleKeyHash> failing_documents;
  std::unordered_map<std::string, uint64_t> failing_sources;
};

/// A global filter retained across refreshes so that a broken filter update
/// keeps its last-known-good application.
struct RetainedFilter {
  std::string origin;
  data yaml;
  ir::FilterRule filter;
};

/// Last-known-good filters keyed by their document label, persistent across
/// refreshes, in deterministic first-seen order.
struct FilterBank {
  auto insert_or_assign(std::string label, RetainedFilter filter) -> void {
    if (auto existing = index.find(label); existing != index.end()) {
      entries[existing->second].second = std::move(filter);
      return;
    }
    index.emplace(label, entries.size());
    entries.emplace_back(std::move(label), std::move(filter));
  }

  template <class Predicate>
  auto erase_if(Predicate predicate) -> void {
    auto kept = std::vector<std::pair<std::string, RetainedFilter>>{};
    kept.reserve(entries.size());
    for (auto& entry : entries) {
      if (not predicate(entry)) {
        kept.push_back(std::move(entry));
      }
    }
    entries = std::move(kept);
    index.clear();
    for (auto position = size_t{0}; position < entries.size(); ++position) {
      index.emplace(entries[position].first, position);
    }
  }

  std::vector<std::pair<std::string, RetainedFilter>> entries;
  std::unordered_map<std::string, size_t> index;
};

/// How an inline rule string was written in TQL. Only the literal syntax
/// decides whether the source span maps one-to-one onto the decoded content:
/// a raw string always does, a plain string only without escapes, and a
/// value computed by any other expression never does.
TENZIR_ENUM(InlineLiteral, unknown, plain, raw);

/// The TQL origin of one inline `rules` entry.
struct InlineRuleOrigin {
  /// The location of the entry's expression, or unknown when the entry came
  /// from a list whose elements have no individual location.
  location source = location::unknown;
  InlineLiteral literal = InlineLiteral::unknown;

  friend auto inspect(auto& f, InlineRuleOrigin& x) -> bool {
    return f.object(x)
      .pretty_name("InlineRuleOrigin")
      .fields(f.field("source", x.source), f.field("literal", x.literal));
  }
};

/// The normalized rule sources of one operator instance.
struct SigmaSources {
  /// File and directory paths, in argument order.
  std::vector<std::string> paths;
  /// Inline YAML rule content, in argument order.
  std::vector<std::string> rules;
  /// The source argument, used to locate runtime diagnostics.
  location source = location::unknown;
  /// The origin of each inline `rules` entry.
  std::vector<InlineRuleOrigin> rule_sources;

  friend auto inspect(auto& f, SigmaSources& x) -> bool {
    return f.object(x)
      .pretty_name("SigmaSources")
      .fields(f.field("paths", x.paths), f.field("rules", x.rules),
              f.field("source", x.source),
              f.field("rule_sources", x.rule_sources));
  }
};

/// Locates every YAML document of an inline rule string inside its TQL
/// string literal, pointing at the document's `title` line or, failing that,
/// its first line of content. Offsets into the content only locate the
/// source when the literal's span provably maps one-to-one onto the content;
/// otherwise every document maps to the complete expression.
auto inline_document_locations(std::string_view content,
                               InlineRuleOrigin const& origin)
  -> std::vector<location> {
  // Split at YAML document markers: a `---` line starts a new document.
  auto starts = std::vector<size_t>{};
  auto ends = std::vector<size_t>{};
  auto seen_content = false;
  auto current = size_t{0};
  auto offset = size_t{0};
  while (offset <= content.size()) {
    auto line_end = content.find('\n', offset);
    if (line_end == std::string_view::npos) {
      line_end = content.size();
    }
    auto line = content.substr(offset, line_end - offset);
    while (not line.empty() and (line.back() == '\r' or line.back() == ' ')) {
      line.remove_suffix(1);
    }
    if (line == "---") {
      if (seen_content) {
        starts.push_back(current);
        ends.push_back(offset);
        seen_content = false;
      }
      current = line_end + 1;
    } else if (not seen_content and not line.empty()
               and not line.starts_with('#')) {
      seen_content = true;
    }
    if (line_end == content.size()) {
      break;
    }
    offset = line_end + 1;
  }
  if (seen_content) {
    starts.push_back(current);
    ends.push_back(content.size());
  }
  auto const source = origin.source;
  auto result = std::vector<location>(starts.size(), source);
  if (not source) {
    return result;
  }
  // The opening delimiter offsets every document. The span exceeds the
  // content by the delimiters alone only when the literal contains its
  // content verbatim: a raw string `r#"…"#` always does and adds three
  // characters plus two per hash, and a plain string does only without
  // escapes, adding exactly the two quotes. Every escape decodes to fewer
  // characters than it occupies, so a plain string with escapes has a longer
  // span, and no arithmetic on the decoded content recovers the source
  // offsets: the whole literal is the honest location. The span alone
  // cannot tell `r"…"` from `"…"` with one escape, which is why the syntax
  // comes from the AST rather than being inferred.
  auto const span = size_t{source.end - source.begin};
  auto const extra = span > content.size() ? span - content.size() : 0;
  auto prefix = size_t{0};
  switch (origin.literal) {
    case InlineLiteral::unknown:
      return result;
    case InlineLiteral::plain:
      if (extra != 2) {
        return result;
      }
      prefix = 1;
      break;
    case InlineLiteral::raw:
      if (extra < 3 or extra % 2 == 0) {
        return result;
      }
      prefix = (extra - 1) / 2 + 1;
      break;
  }
  for (auto index = size_t{0}; index < starts.size(); ++index) {
    auto const document
      = content.substr(starts[index], ends[index] - starts[index]);
    // Prefer the title line; otherwise the first line with content.
    auto line_start = size_t{0};
    auto chosen = Option<std::pair<size_t, size_t>>{};
    auto first = Option<std::pair<size_t, size_t>>{};
    while (line_start < document.size()) {
      auto line_end = document.find('\n', line_start);
      if (line_end == std::string_view::npos) {
        line_end = document.size();
      }
      auto line = document.substr(line_start, line_end - line_start);
      auto const trimmed_end = line.find_last_not_of(" \r\t");
      auto const length
        = trimmed_end == std::string_view::npos ? 0 : trimmed_end + 1;
      if (length > 0 and not line.starts_with('#')) {
        if (not first) {
          first = std::pair{line_start, length};
        }
        if (line.starts_with("title:")) {
          chosen = std::pair{line_start, length};
          break;
        }
      }
      line_start = line_end + 1;
    }
    if (not chosen) {
      chosen = first;
    }
    if (chosen) {
      result[index] = source.subloc(
        detail::narrow<uint32_t>(prefix + starts[index] + chosen->first),
        detail::narrow<uint32_t>(chosen->second));
    }
  }
  return result;
}

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

/// One successfully parsed YAML document of a source.
struct ParsedDocument {
  RuleKey key;
  std::string label;
  data yaml;
  ir::Document document;
  /// The location of the document inside an inline `rules` argument.
  location source = location::unknown;
};

/// The parse-phase result of one source.
struct SourceSnapshot {
  std::string origin;
  uint64_t revision = 0;
  bool had_previous = false;
  bool failed = false;
  bool assembling_retained = false;
  bool replacement_failed = false;
  std::vector<ParsedDocument> documents;
  /// Lowered artifacts, filled by the assembly phase.
  RuleMap replacements;
};

auto lower_rule(ir::DetectionRule const& rule,
                Option<FieldBindings const&> bindings = None{},
                Option<std::string_view> guard_column = None{},
                Option<size_t> keyword_field_count = None{})
  -> ParseResult<ast::expression>;

/// Lowers a filter independently so that only executable revisions enter the
/// persistent filter bank.
auto lower_filter(ir::FilterRule const& filter)
  -> ParseResult<ast::expression> {
  auto expressions = ExpressionMap{};
  for (auto const& [name, detection] : filter.detections) {
    TRY(auto expression, lower_detection(detection));
    expressions[name] = std::move(expression);
  }
  return lower_condition(filter.condition, expressions);
}

/// Parses and validates every YAML document of one source. Cross-document
/// resolution and final lowering with applied filters happen in the assembly
/// phase. Loading is failure-isolated: a source whose new revision breaks
/// retains its last-known-good artifacts, and diagnostics are emitted once per
/// failing revision.
auto parse_source(std::string_view content, std::string const& origin,
                  RuleMap const& previous, FilterBank const& filter_bank,
                  ReloadState& state, diagnostic_handler& dh,
                  std::span<location const> document_locations = {})
  -> SourceSnapshot {
  auto snapshot = SourceSnapshot{};
  snapshot.origin = origin;
  snapshot.revision = hash(content);
  snapshot.had_previous
    = std::ranges::any_of(previous.entries,
                          [&](auto const& entry) {
                            return entry.first.origin == origin;
                          })
      or std::ranges::any_of(filter_bank.entries, [&](auto const& entry) {
           return entry.second.origin == origin;
         });
  auto documents = from_yaml_documents(content);
  if (not documents or documents->empty()) {
    snapshot.failed = true;
    if (state.should_emit(origin, snapshot.revision)) {
      auto builder
        = snapshot.had_previous
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
    return snapshot;
  }
  state.succeeded(origin);
  state.prune_documents(origin, documents->size());
  auto const multiple = documents->size() > 1;
  for (auto index = size_t{0}; index < documents->size(); ++index) {
    auto const key = RuleKey{origin, index};
    auto const label = multiple ? fmt::format("{}#{}", origin, index) : origin;
    auto const doc_source = index < document_locations.size()
                              ? document_locations[index]
                              : location::unknown;
    auto handle_failure = [&](auto&& emit_reason) {
      snapshot.failed = true;
      if (not state.should_emit(key, snapshot.revision)) {
        return;
      }
      auto builder
        = snapshot.had_previous
            ? diagnostic::warning("sigma operator retains last known good "
                                  "version of source '{}'",
                                  origin)
            : diagnostic::warning("sigma operator ignores rule '{}'", label);
      if (doc_source) {
        builder = std::move(builder).primary(doc_source);
      }
      emit_reason(std::move(builder));
    };
    auto& document = (*documents)[index];
    if (not is<record>(document)) {
      handle_failure([&](diagnostic_builder builder) {
        std::move(builder).note("rule is not a YAML dictionary").emit(dh);
      });
      continue;
    }
    auto parsed = ir::parse_document(document);
    if (parsed.is_err()) {
      auto reason = std::move(parsed).unwrap_err();
      handle_failure([&](diagnostic_builder builder) {
        append_reason(std::move(builder), reason).emit(dh);
      });
      continue;
    }
    auto parsed_document = std::move(parsed).unwrap();
    auto lowered = [&]() -> ParseResult<ast::expression> {
      if (auto const* rule
          = try_as<ir::DetectionRule>(parsed_document.content)) {
        return lower_rule(*rule);
      }
      if (auto const* filter
          = try_as<ir::FilterRule>(parsed_document.content)) {
        return lower_filter(*filter);
      }
      TENZIR_UNREACHABLE();
    }();
    if (lowered.is_err()) {
      auto reason = std::move(lowered).unwrap_err();
      handle_failure([&](diagnostic_builder builder) {
        append_reason(std::move(builder), reason).emit(dh);
      });
      continue;
    }
    auto expression = std::move(lowered).unwrap();
    auto provider = session_provider::make(dh);
    if (not resolve_entities(expression, provider.as_session())) {
      handle_failure([&](diagnostic_builder builder) {
        std::move(builder).note("failed to resolve sigma rule").emit(dh);
      });
      continue;
    }
    snapshot.documents.push_back(ParsedDocument{
      key, label, std::move(document), std::move(parsed_document), doc_source});
  }
  return snapshot;
}

/// Lowers a detection rule into an executable expression plus the
/// per-identifier artifacts needed for match provenance.
auto lower_rule_with_artifacts(ir::DetectionRule const& rule,
                               Option<FieldBindings const&> bindings = None{},
                               Option<std::string_view> guard_column = None{},
                               Option<size_t> keyword_field_count = None{})
  -> ParseResult<std::pair<ast::expression, std::vector<IdentifierArtifact>>> {
  auto artifacts = std::vector<IdentifierArtifact>{};
  auto expressions = ExpressionMap{};
  for (auto const& [name, detection] : rule.detections) {
    TRY(auto artifact, lower_identifier_artifact(name, detection, bindings,
                                                 keyword_field_count));
    expressions[name] = artifact.expression;
    artifacts.push_back(std::move(artifact));
  }
  // List-valued conditions are OR-linked queries.
  auto disjuncts = std::vector<ast::expression>{};
  for (auto const& condition : rule.conditions) {
    TRY(auto expr, lower_condition(condition, expressions));
    disjuncts.push_back(std::move(expr));
  }
  auto expression
    = expression_algebra::join<ast::binary_op::or_>(std::move(disjuncts));
  if (guard_column) {
    expression = make_binary_expr(make_private_field_expr(*guard_column),
                                  ast::binary_op::and_, std::move(expression));
  }
  return std::pair{std::move(expression), std::move(artifacts)};
}

/// Lowers a detection rule into an executable expression.
auto lower_rule(ir::DetectionRule const& rule,
                Option<FieldBindings const&> bindings,
                Option<std::string_view> guard_column,
                Option<size_t> keyword_field_count)
  -> ParseResult<ast::expression> {
  TRY(auto lowered, lower_rule_with_artifacts(rule, bindings, guard_column,
                                              keyword_field_count));
  return std::move(lowered.first);
}

/// Resolves global filters against detection rules and lowers every rule
/// into its snapshot's replacement set.
///
/// Identity conflicts and filter failures are isolated: duplicate rule
/// identities invalidate only the conflicting documents, an unresolvable or
/// incompatible filter target leaves the target running unfiltered with a
/// diagnostic, and a broken filter revision retains its last-known-good
/// application from the filter bank.
auto assemble_rules(std::vector<SourceSnapshot>& snapshots,
                    RuleMap const& previous, FilterBank& filter_bank,
                    ReloadState& state, diagnostic_handler& dh) -> void {
  struct DetectionSlot {
    SourceSnapshot* snapshot;
    RuleKey key;
    std::string const* label;
    data const* yaml;
    ir::DetectionRule const* rule;
    bool retained = false;
    std::vector<ir::FilterRule const*> filters;
    std::vector<uint64_t> filter_revisions;
    location source = location::unknown;
  };
  // Resolve identities to a fixed point. An identity conflict can roll a source
  // back to documents with different identities, which can expose another
  // conflict with a source that was valid in the previous pass.
  auto detections = std::vector<DetectionSlot>{};
  auto origins = std::unordered_set<std::string>{};
  auto invalid_documents = std::unordered_set<RuleKey, RuleKeyHash>{};
  for (auto& snapshot : snapshots) {
    origins.insert(snapshot.origin);
  }
  while (true) {
    detections.clear();
    for (auto& snapshot : snapshots) {
      if (snapshot.failed and snapshot.had_previous) {
        snapshot.assembling_retained = true;
        for (auto const& [key, entry] : previous.entries) {
          if (key.origin == snapshot.origin) {
            detections.push_back(DetectionSlot{&snapshot, key, &entry.label,
                                               &entry.yaml, &entry.detection,
                                               true});
            detections.back().source = entry.source;
          }
        }
        continue;
      }
      for (auto& document : snapshot.documents) {
        if (invalid_documents.contains(document.key)) {
          continue;
        }
        if (auto const* rule
            = try_as<ir::DetectionRule>(document.document.content)) {
          detections.push_back(DetectionSlot{
            &snapshot, document.key, &document.label, &document.yaml, rule});
          detections.back().source = document.source;
        }
      }
    }
    auto by_identity = std::unordered_map<std::string, std::vector<size_t>>{};
    for (auto index = size_t{0}; index < detections.size(); ++index) {
      auto const& metadata = detections[index].rule->metadata;
      if (metadata.id) {
        by_identity[*metadata.id].push_back(index);
      }
      if (metadata.name and metadata.name != metadata.id) {
        by_identity[*metadata.name].push_back(index);
      }
    }
    auto changed = false;
    for (auto const& [identity, indices] : by_identity) {
      if (indices.size() < 2) {
        continue;
      }
      for (auto const index : indices) {
        auto& slot = detections[index];
        // Retained slots describe already active rules. Reject conflicting new
        // revisions rather than dropping a last-known-good rule.
        if (slot.retained) {
          continue;
        }
        changed = invalid_documents.insert(slot.key).second or changed;
        if (not slot.snapshot->failed) {
          slot.snapshot->failed = true;
          changed = true;
        }
        if (state.should_emit(slot.key,
                              slot.snapshot->revision ^ hash(identity))) {
          auto builder = diagnostic::warning("sigma operator ignores rule '{}'",
                                             *slot.label)
                           .note("duplicate rule identity `{}`", identity);
          if (slot.source) {
            builder = std::move(builder).primary(slot.source);
          }
          std::move(builder).emit(dh);
        }
      }
    }
    if (not changed) {
      break;
    }
  }
  // Publish filters only after parsing, lowering, entity resolution, and
  // cross-document identity validation established the source's final state.
  auto refreshed_origins = std::unordered_set<std::string>{};
  auto parsed_filter_labels = std::unordered_set<std::string>{};
  for (auto& snapshot : snapshots) {
    // A failed reload is transactional: keep every banked filter from the
    // previous source revision instead of publishing only its valid siblings.
    auto const refresh_filters
      = not snapshot.failed or not snapshot.had_previous;
    if (refresh_filters) {
      refreshed_origins.insert(snapshot.origin);
    }
    for (auto& document : snapshot.documents) {
      if (auto const* filter
          = try_as<ir::FilterRule>(document.document.content);
          filter and refresh_filters) {
        parsed_filter_labels.insert(document.label);
        filter_bank.insert_or_assign(document.label,
                                     RetainedFilter{snapshot.origin,
                                                    document.yaml, *filter});
      }
    }
  }
  // Remove filters with vanished sources and filters absent from successfully
  // validated source revisions. Failed reloads retain their complete bank state.
  filter_bank.erase_if([&](auto const& entry) {
    return not origins.contains(entry.second.origin)
           or (refreshed_origins.contains(entry.second.origin)
               and not parsed_filter_labels.contains(entry.first));
  });
  // Apply filters in deterministic bank order.
  auto active_filter_memos = std::unordered_set<std::string>{};
  for (auto const& [label, retained] : filter_bank.entries) {
    auto const& filter = retained.filter;
    auto const filter_revision = hash(retained.yaml);
    auto apply_to = [&](DetectionSlot& slot) {
      slot.filters.push_back(&filter);
      slot.filter_revisions.push_back(filter_revision);
    };
    if (filter.targets.any) {
      for (auto& slot : detections) {
        if (ir::compatible(filter.log_source, slot.rule->log_source)) {
          apply_to(slot);
        }
      }
      continue;
    }
    for (auto const& reference : filter.targets.rules) {
      auto const memo_key = "filter:" + label + ":" + reference;
      active_filter_memos.insert(memo_key);
      auto matches = std::vector<size_t>{};
      for (auto index = size_t{0}; index < detections.size(); ++index) {
        auto const& slot = detections[index];
        auto const& metadata = slot.rule->metadata;
        if (metadata.id == reference or metadata.name == reference) {
          matches.push_back(index);
        }
      }
      auto warn = [&](std::string_view problem) {
        if (state.should_emit(memo_key, filter_revision)) {
          diagnostic::warning("sigma operator cannot apply filter '{}'", label)
            .note("reference `{}`: {}", reference, problem)
            .note("affected rules run unfiltered, which may increase matches")
            .emit(dh);
        }
      };
      if (matches.empty()) {
        warn("no rule with this id or name");
        continue;
      }
      if (matches.size() > 1) {
        warn("ambiguous: multiple rules share this identity");
        continue;
      }
      auto& slot = detections[matches[0]];
      if (not ir::compatible(filter.log_source, slot.rule->log_source)) {
        warn("incompatible log source");
        continue;
      }
      state.succeeded(memo_key);
      apply_to(slot);
    }
  }
  state.reconcile_filter_applications(active_filter_memos);
  // Lower every valid detection with its filter applications.
  for (auto& slot : detections) {
    auto revision = slot.snapshot->revision;
    for (auto const filter_revision : slot.filter_revisions) {
      revision ^= filter_revision;
    }
    auto handle_failure = [&](auto&& emit_reason) {
      slot.snapshot->failed = true;
      slot.snapshot->replacement_failed = true;
      if (not state.should_emit(slot.key, revision)) {
        return;
      }
      auto builder
        = slot.snapshot->had_previous
            ? diagnostic::warning("sigma operator retains last known good "
                                  "version of source '{}'",
                                  slot.snapshot->origin)
            : diagnostic::warning("sigma operator ignores rule '{}'",
                                  *slot.label);
      if (slot.source) {
        builder = std::move(builder).primary(slot.source);
      }
      emit_reason(std::move(builder));
    };
    auto rule = *slot.rule;
    for (auto index = size_t{0}; index < slot.filters.size(); ++index) {
      rule = ir::apply_filter(std::move(rule), *slot.filters[index], index);
    }
    auto lowered = lower_rule_with_artifacts(rule);
    if (lowered.is_err()) {
      auto reason = std::move(lowered).unwrap_err();
      handle_failure([&](diagnostic_builder builder) {
        append_reason(std::move(builder), reason).emit(dh);
      });
      continue;
    }
    auto [expression, artifacts] = std::move(lowered).unwrap();
    auto provider = session_provider::make(dh);
    auto resolved = bool{resolve_entities(expression, provider.as_session())};
    for (auto& artifact : artifacts) {
      resolved = resolved
                 and bool{resolve_entities(artifact.expression,
                                           provider.as_session())};
      for (auto& group : artifact.groups) {
        for (auto& item : group) {
          resolved = resolved
                     and bool{resolve_entities(item.expression,
                                               provider.as_session())};
        }
      }
    }
    if (not resolved) {
      handle_failure([&](diagnostic_builder builder) {
        std::move(builder).note("failed to resolve sigma rule").emit(dh);
      });
      continue;
    }
    if (not slot.retained) {
      state.succeeded(slot.key);
    }
    auto finding = make_finding_template(rule, *slot.yaml);
    auto const effective_revision = hash(finding.policy);
    slot.snapshot->replacements.insert_or_assign(
      slot.key,
      RuleEntry{*slot.yaml, *slot.label, effective_revision, *slot.rule,
                std::move(rule), std::move(expression), std::move(artifacts),
                std::move(finding), slot.source});
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
                RuleMap const& previous, FilterBank& filter_bank,
                ReloadState& state, diagnostic_handler& dh) -> bool {
  auto files = collect_rule_files(sources.paths, dh);
  if (not files) {
    return false;
  }
  // Phase 1: parse the complete source snapshot.
  auto snapshots = std::vector<SourceSnapshot>{};
  auto origins = std::unordered_set<std::string>{};
  for (auto const& file : *files) {
    auto const origin = file.string();
    origins.insert(origin);
    auto content = tenzir::io::read(file);
    if (not content) {
      auto snapshot = SourceSnapshot{};
      snapshot.origin = origin;
      snapshot.failed = true;
      snapshot.had_previous
        = std::ranges::any_of(previous.entries,
                              [&](auto const& entry) {
                                return entry.first.origin == origin;
                              })
          or std::ranges::any_of(filter_bank.entries, [&](auto const& entry) {
               return entry.second.origin == origin;
             });
      if (state.should_emit(origin, hash(to_string(content.error())))) {
        auto builder
          = snapshot.had_previous
              ? diagnostic::warning("sigma operator retains last known good "
                                    "version of '{}'",
                                    origin)
              : diagnostic::warning("sigma operator ignores rule '{}'", origin);
        std::move(builder)
          .note("failed to read file: {}", content.error())
          .emit(dh);
      }
      snapshots.push_back(std::move(snapshot));
      continue;
    }
    auto const view = std::string_view{
      reinterpret_cast<char const*>(content->data()), content->size()};
    snapshots.push_back(
      parse_source(view, origin, previous, filter_bank, state, dh));
  }
  for (auto index = size_t{0}; index < sources.rules.size(); ++index) {
    auto const origin = fmt::format("<rules[{}]>", index);
    origins.insert(origin);
    auto const rule_source = index < sources.rule_sources.size()
                               ? sources.rule_sources[index]
                               : InlineRuleOrigin{};
    auto const document_locations
      = inline_document_locations(sources.rules[index], rule_source);
    snapshots.push_back(parse_source(sources.rules[index], origin, previous,
                                     filter_bank, state, dh,
                                     document_locations));
  }
  // Phase 2: resolve identities and filters, then lower.
  assemble_rules(snapshots, previous, filter_bank, state, dh);
  // Phase 3: publish per-source, retaining failed sources transactionally.
  for (auto& snapshot : snapshots) {
    if (snapshot.failed and snapshot.had_previous
        and (not snapshot.assembling_retained or snapshot.replacement_failed)) {
      std::ignore = retain_source(previous, snapshot.origin, next);
      continue;
    }
    for (auto& [key, entry] : snapshot.replacements.entries) {
      next.insert_or_assign(std::move(key), std::move(entry));
    }
  }
  state.reconcile(origins);
  return true;
}

auto update_rules(SigmaSources const& sources, RuleMap& rules,
                  FilterBank& filter_bank, ReloadState& state,
                  diagnostic_handler& dh) -> bool {
  auto next = RuleMap{};
  if (not load_rules(sources, next, rules, filter_bank, state, dh)) {
    return false;
  }
  next.revision = rule_map_revision(next);
  auto const changed = next.revision != rules.revision;
  rules = std::move(next);
  return changed;
}

const auto sigma_metrics_type = type{
  "tenzir.metrics.sigma",
  record_type{
    {"events", uint64_type{}},
    {"rule_evaluations", uint64_type{}},
    {"matches", uint64_type{}},
  },
};

/// Emits the processing work and results for one input batch.
auto emit_processing_metrics(metric_handler& handler, uint64_t events,
                             uint64_t rules, uint64_t matches) -> void {
  handler.emit({
    {"events", events},
    {"rule_evaluations", events * rules},
    {"matches", matches},
  });
}

/// One causal decision of the condition trace.
struct TraceDecision {
  std::string identifier;
  bool matched = false;
};

/// Evaluates a condition tree over per-identifier boolean values.
template <class ValueOf>
auto evaluate_condition(ir::Condition const& condition,
                        ir::DetectionRule const& rule, ValueOf&& value_of)
  -> bool {
  return match(
    condition.node,
    [&](ir::Identifier const& x) -> bool {
      if (rule.detections.contains(x.name)) {
        return value_of(x.name);
      }
      // A bare wildcard pattern AND-links all matching identifiers.
      auto result = false;
      for (auto const& [name, detection] : rule.detections) {
        if (not ir::pattern_matches(x.name, name)) {
          continue;
        }
        if (not value_of(name)) {
          return false;
        }
        result = true;
      }
      return result;
    },
    [&](ir::Quantified const& x) -> bool {
      auto const pattern = x.all_identifiers ? std::string_view{"*"}
                                             : std::string_view{x.pattern};
      auto const all = x.quantifier == ir::Quantifier::all;
      auto result = all;
      for (auto const& [name, detection] : rule.detections) {
        if (not ir::pattern_matches(pattern, name)) {
          continue;
        }
        if (all and not value_of(name)) {
          return false;
        }
        if (not all and value_of(name)) {
          return true;
        }
      }
      return result;
    },
    [&](ir::Negation const& x) -> bool {
      return not evaluate_condition(*x.operand, rule, value_of);
    },
    [&](ir::Conjunction const& x) -> bool {
      return evaluate_condition(*x.left, rule, value_of)
             and evaluate_condition(*x.right, rule, value_of);
    },
    [&](ir::Disjunction const& x) -> bool {
      return evaluate_condition(*x.left, rule, value_of)
             or evaluate_condition(*x.right, rule, value_of);
    });
}

/// Computes the deterministic causal trace of a condition outcome in IR
/// order: all required conjunction children, the first successful
/// disjunction child, and the first required number of successful
/// quantified identifiers. Negation flips the expected outcome, under which
/// the dual rules apply. Negative and absence-based decisions are preserved.
template <class ValueOf>
auto causal_trace(ir::Condition const& condition, ir::DetectionRule const& rule,
                  ValueOf&& value_of, bool expected,
                  std::vector<TraceDecision>& out) -> void {
  auto emit = [&](std::string_view name) {
    // Record every identifier once, keeping the first decision.
    for (auto const& decision : out) {
      if (decision.identifier == name) {
        return;
      }
    }
    out.push_back(TraceDecision{std::string{name}, value_of(name)});
  };
  match(
    condition.node,
    [&](ir::Identifier const& x) {
      if (rule.detections.contains(x.name)) {
        emit(x.name);
        return;
      }
      for (auto const& [name, detection] : rule.detections) {
        if (not ir::pattern_matches(x.name, name)) {
          continue;
        }
        // Bare wildcard patterns AND-link their identifiers. A successful
        // conjunction requires all identifiers, while a failed one is
        // explained by its first failing identifier.
        if (expected) {
          emit(name);
          continue;
        }
        if (not value_of(name)) {
          emit(name);
          break;
        }
      }
    },
    [&](ir::Quantified const& x) {
      auto const pattern = x.all_identifiers ? std::string_view{"*"}
                                             : std::string_view{x.pattern};
      auto const all = x.quantifier == ir::Quantifier::all;
      // `all of` requires every identifier; `1 of` is satisfied by the first
      // matching one. Under an unexpected outcome the duals apply: a failed
      // `1 of` requires every identifier, a failed `all of` is explained by
      // the first failing one.
      auto const exhaustive = all == expected;
      for (auto const& [name, detection] : rule.detections) {
        if (not ir::pattern_matches(pattern, name)) {
          continue;
        }
        if (exhaustive) {
          emit(name);
          continue;
        }
        if (value_of(name) == expected) {
          emit(name);
          break;
        }
      }
    },
    [&](ir::Negation const& x) {
      causal_trace(*x.operand, rule, value_of, not expected, out);
    },
    [&](ir::Conjunction const& x) {
      if (expected) {
        causal_trace(*x.left, rule, value_of, true, out);
        causal_trace(*x.right, rule, value_of, true, out);
        return;
      }
      // A failed conjunction is explained by its first failing child.
      if (not evaluate_condition(*x.left, rule, value_of)) {
        causal_trace(*x.left, rule, value_of, false, out);
        return;
      }
      causal_trace(*x.right, rule, value_of, false, out);
    },
    [&](ir::Disjunction const& x) {
      if (not expected) {
        causal_trace(*x.left, rule, value_of, false, out);
        causal_trace(*x.right, rule, value_of, false, out);
        return;
      }
      // A satisfied disjunction is explained by its first successful child.
      if (evaluate_condition(*x.left, rule, value_of)) {
        causal_trace(*x.left, rule, value_of, true, out);
        return;
      }
      causal_trace(*x.right, rule, value_of, true, out);
    });
}

/// Evaluates an expression over a slice into per-row booleans.
auto eval_boolean(ast::expression const& expression, table_slice const& slice,
                  diagnostic_handler& dh) -> std::vector<bool> {
  auto result = std::vector<bool>{};
  result.reserve(slice.rows());
  for (auto series : eval(expression, slice, dh).parts()) {
    if (auto const booleans = series.as<bool_type>()) {
      for (auto const value : booleans->values()) {
        result.push_back(value.has_value() and *value);
      }
      continue;
    }
    result.resize(result.size() + series.length(), false);
  }
  TENZIR_ASSERT(std::cmp_equal(result.size(), slice.rows()));
  return result;
}

auto eval_mask(ast::expression const& expression, table_slice const& slice,
               diagnostic_handler& dh) -> std::shared_ptr<arrow::BooleanArray> {
  auto builder = arrow::BooleanBuilder{arrow_memory_pool()};
  check(builder.Reserve(detail::narrow<int64_t>(slice.rows())));
  for (auto part : eval(expression, slice, dh).parts()) {
    if (auto const booleans = part.as<bool_type>()) {
      for (auto const value : booleans->values()) {
        check(builder.Append(value.has_value() and *value));
      }
      continue;
    }
    for (auto index = int64_t{0}; index < part.length(); ++index) {
      check(builder.Append(false));
    }
  }
  return finish(builder);
}

/// Wraps matching events and the original Sigma rule in `tenzir.sigma`.
auto build_plain_matches(table_slice matched, data const& rule)
  -> std::vector<table_slice> {
  TENZIR_ASSERT(matched.rows() > 0);
  auto [event_schema, event_array] = offset{}.get(matched);
  auto rule_series
    = data_to_series(rule, detail::narrow<int64_t>(matched.rows()));
  auto rule_schema = std::move(rule_series.type);
  auto rule_array = std::move(rule_series.array);
  auto const result_schema = type{
    "tenzir.sigma",
    record_type{
      {"event", event_schema},
      {"rule", rule_schema},
    },
  };
  auto batch
    = arrow::RecordBatch::Make(result_schema.to_arrow_schema(),
                               detail::narrow<int64_t>(matched.rows()),
                               {std::move(event_array), std::move(rule_array)});
  auto result = std::vector<table_slice>{};
  result.emplace_back(batch, result_schema);
  return result;
}

/// Builds one OCSF 1.9.0 Detection Finding per matching row of the input.
auto build_findings(table_slice const& matched_input,
                    table_slice const& matched_evaluation,
                    RuleEntry const& entry,
                    std::vector<IdentifierArtifact> const& identifiers,
                    Option<FieldBindings const&> bindings,
                    diagnostic_handler& dh) -> std::vector<table_slice> {
  TENZIR_ASSERT(matched_input.rows() > 0);
  TENZIR_ASSERT(matched_input.rows() == matched_evaluation.rows());
  // Evaluate identifier and item expressions per slice, lazily for items.
  auto identifier_values
    = detail::flat_map<std::string_view, std::vector<bool>>{};
  for (auto const& identifier : identifiers) {
    identifier_values.emplace(identifier.name,
                              eval_boolean(identifier.expression,
                                           matched_evaluation, dh));
  }
  auto value_of_at = [&](std::string_view name, size_t row) {
    auto const entry = identifier_values.find(name);
    TENZIR_ASSERT(entry != identifier_values.end());
    return entry->second[row];
  };
  auto item_values
    = std::unordered_map<ast::expression const*, std::vector<bool>>{};
  auto item_value_at = [&](ItemArtifact const& item, size_t row) {
    auto entry = item_values.find(&item.expression);
    if (entry == item_values.end()) {
      entry = item_values
                .emplace(&item.expression,
                         eval_boolean(item.expression, matched_evaluation, dh))
                .first;
    }
    return entry->second[row];
  };
  struct FieldValues {
    std::vector<data> values;
    std::vector<data> paths;
  };
  auto field_values = std::unordered_map<std::string, FieldValues>{};
  auto field_value_at = [&](std::string const& field,
                            size_t row) -> std::pair<data, std::string> {
    auto entry = field_values.find(field);
    if (entry == field_values.end()) {
      auto expression = make_field_expr(field, bindings);
      auto provider = session_provider::make(dh);
      std::ignore = resolve_entities(expression, provider.as_session());
      auto values = std::vector<data>{};
      values.reserve(matched_evaluation.rows());
      for (auto series : eval(expression, matched_evaluation, dh).parts()) {
        for (auto value : series.values()) {
          values.push_back(materialize(value));
        }
      }
      auto paths = std::vector<data>{};
      paths.reserve(matched_evaluation.rows());
      if (bindings) {
        auto const binding = bindings->find(field);
        TENZIR_ASSERT(binding != bindings->end());
        if (auto const& constant = binding->second.constant_evidence_path) {
          paths.resize(matched_evaluation.rows(), data{*constant});
        } else {
          auto path_expression
            = make_private_field_expr(binding->second.evidence_path_column);
          std::ignore
            = resolve_entities(path_expression, provider.as_session());
          for (auto series :
               eval(path_expression, matched_evaluation, dh).parts()) {
            for (auto value : series.values()) {
              paths.push_back(materialize(value));
            }
          }
        }
      } else {
        paths.resize(matched_evaluation.rows(), data{field});
      }
      entry
        = field_values
            .emplace(field, FieldValues{std::move(values), std::move(paths)})
            .first;
    }
    auto path = std::string{field};
    if (auto const* value = try_as<std::string>(&entry->second.paths[row])) {
      path = *value;
    }
    return {entry->second.values[row], std::move(path)};
  };
  // Assemble one finding per matched row.
  auto const now = time::clock::now();
  auto builder = series_builder{};
  auto row = size_t{0};
  for (auto event : matched_input.values()) {
    auto const event_data = data{materialize(event)};
    auto value_of = [&](std::string_view name) {
      return value_of_at(name, row);
    };
    // The causal trace over all OR-linked condition entries: the first
    // satisfied entry explains the match.
    auto trace = std::vector<TraceDecision>{};
    for (auto const& condition : entry.adjusted.conditions) {
      if (evaluate_condition(condition, entry.adjusted, value_of)) {
        causal_trace(condition, entry.adjusted, value_of, true, trace);
        break;
      }
    }
    // Field-level matches for positively contributing identifiers: the
    // first satisfied group of each matched identifier explains it.
    struct FieldMatch {
      ItemArtifact const* item;
      data value;
      std::string evidence_path;
    };
    auto field_matches = std::vector<FieldMatch>{};
    for (auto const& decision : trace) {
      if (not decision.matched) {
        continue;
      }
      auto const artifact
        = std::ranges::find_if(identifiers, [&](auto const& candidate) {
            return candidate.name == decision.identifier;
          });
      if (artifact == identifiers.end()) {
        continue;
      }
      for (auto const& group : artifact->groups) {
        auto const satisfied
          = std::ranges::all_of(group, [&](ItemArtifact const& item) {
              return item_value_at(item, row);
            });
        if (not satisfied) {
          continue;
        }
        for (auto const& item : group) {
          auto value = data{};
          auto evidence_path = std::string{};
          if (not item.keyword and not item.negated) {
            std::tie(value, evidence_path) = field_value_at(item.field, row);
          }
          field_matches.push_back(
            FieldMatch{&item, std::move(value), std::move(evidence_path)});
        }
        break;
      }
    }
    auto const finding_uid = fmt::format("{}", uuid::random());
    // Build the finding.
    auto finding = builder.record();
    finding.field("time").data(now);
    finding.field("class_uid").data(int64_t{2004});
    finding.field("category_uid").data(int64_t{2});
    finding.field("activity_id").data(int64_t{1});
    finding.field("type_uid").data(int64_t{200401});
    finding.field("status_id").data(int64_t{1});
    finding.field("severity_id").data(entry.finding.severity_id);
    auto metadata_field = finding.field("metadata").record();
    metadata_field.field("version").data("1.9.0");
    auto product = metadata_field.field("product").record();
    product.field("name").data("Tenzir");
    product.field("vendor_name").data("Tenzir");
    metadata_field.field("profiles").list().data("security_control");
    finding.field("action_id").data(int64_t{3});
    finding.field("disposition_id").data(int64_t{15});
    if (not as<list>(entry.finding.attacks).empty()) {
      finding.field("attacks").data(entry.finding.attacks);
    }
    finding.field("policy").data(entry.finding.policy);
    auto info = finding.field("finding_info").record();
    info.field("uid").data(finding_uid);
    if (entry.finding.title) {
      info.field("title").data(*entry.finding.title);
    }
    info.field("analytic").data(entry.finding.analytic);
    if (not as<list>(entry.finding.attacks).empty()) {
      info.field("attacks").data(entry.finding.attacks);
    }
    auto traits = info.field("traits").list();
    for (auto const& decision : trace) {
      if (not decision.matched) {
        continue;
      }
      auto trait = traits.record();
      trait.field("name").data(decision.identifier);
      trait.field("type").data("sigma:search-identifier");
    }
    if (not as<list>(entry.finding.data_sources).empty()) {
      info.field("data_sources").data(entry.finding.data_sources);
    }
    auto observables = finding.field("observables").list();
    for (auto const& field_match : field_matches) {
      // Observables come only from positive field matches with a concrete
      // value; the input is not OCSF, so no type is invented.
      if (field_match.item->keyword or field_match.item->negated
          or is<caf::none_t>(field_match.value)) {
        continue;
      }
      auto observable = observables.record();
      observable.field("name").data(
        fmt::format("evidences[0].data.{}", field_match.evidence_path));
      observable.field("type_id").data(int64_t{0});
      if (auto const* str = try_as<std::string>(&field_match.value)) {
        observable.field("value").data(*str);
      } else {
        observable.field("value").data(fmt::format("{}", field_match.value));
      }
    }
    auto evidences = finding.field("evidences").list();
    evidences.record().field("data").data(event_data);
    auto provenance = evidences.record();
    provenance.field("name").data("SigmaMatch");
    provenance.field("data").data(record{});
    auto sigma_info = provenance.field("sigma").record();
    auto trace_list = sigma_info.field("trace").list();
    for (auto const& decision : trace) {
      auto decision_record = trace_list.record();
      decision_record.field("identifier").data(decision.identifier);
      decision_record.field("matched").data(decision.matched);
    }
    auto fields_list = sigma_info.field("fields").list();
    for (auto const& field_match : field_matches) {
      auto match_record = fields_list.record();
      if (not field_match.item->keyword) {
        match_record.field("field").data(field_match.item->field);
        // Dotted names resolve with exact-key precedence; record the
        // resolved interpretation when it is ambiguous.
        if (field_match.item->field.contains('.')) {
          auto const* event_record = try_as<record>(&event_data);
          auto const exact = event_record
                             and event_record->find(field_match.item->field)
                                   != event_record->end();
          match_record.field("path").data(exact ? "exact-key" : "nested");
        }
      }
      match_record.field("matcher").data(field_match.item->matcher);
      match_record.field("case").data(
        field_match.item->case_insensitive ? "insensitive" : "sensitive");
      match_record.field("polarity")
        .data(field_match.item->negated ? "negative" : "positive");
      if (not field_match.item->keyword and not field_match.item->negated) {
        match_record.field("value").data(field_match.value);
      }
    }
    ++row;
  }
  return builder.finish_as_table_slice("ocsf.detection_finding");
}

/// Matches one rule and builds the configured output representation.
auto build_output(table_slice const& input, table_slice const& evaluation,
                  RuleEntry const& entry, ast::expression const& expression,
                  std::vector<IdentifierArtifact> const& identifiers,
                  Option<FieldBindings const&> bindings, sigma_format format,
                  diagnostic_handler& dh) -> std::vector<table_slice> {
  auto const mask = eval_mask(expression, evaluation, dh);
  if (mask->true_count() == 0) {
    return {};
  }
  auto matched_input = filter(input, *mask);
  switch (format) {
    case sigma_format::ocsf:
      return build_findings(matched_input, filter(evaluation, *mask), entry,
                            identifiers, bindings, dh);
    case sigma_format::plain:
      return build_plain_matches(std::move(matched_input), entry.yaml);
  }
  TENZIR_UNREACHABLE();
}

// -- internal runtime functions ------------------------------------------

// Sigma regular expressions match anywhere in the value: the `re` modifier
// carries no implicit anchors, and the patterns lowered from plain strings
// spell their anchors and wildcards out. Every match site therefore uses
// partial matching.

/// Recursively matches a regular expression against string-valued leaves.
/// The optional field count limits only the outer record, keeping appended
/// private columns out while preserving recursive scans of original fields.
/// Sets `matches[i]` when any selected leaf of row `i` matches.
auto keyword_match(series const& input, re2::RE2 const& regex,
                   std::vector<bool>& matches,
                   Option<size_t> top_level_field_count = None{}) -> void {
  TENZIR_ASSERT(std::cmp_equal(input.length(), matches.size()));
  if (auto const strings = input.as<string_type>()) {
    auto const& array = *strings->array;
    for (auto i = int64_t{0}; i < array.length(); ++i) {
      if (matches[i] or array.IsNull(i)) {
        continue;
      }
      auto const value = array.GetView(i);
      matches[i] = re2::RE2::PartialMatch({value.data(), value.size()}, regex);
    }
    return;
  }
  if (auto const records = input.as<record_type>()) {
    // Flattening propagates parent-level nulls into each child so that
    // values inside null records can never match.
    auto const field_count
      = top_level_field_count
          ? std::min(*top_level_field_count, records->type.num_fields())
          : records->type.num_fields();
    auto index = size_t{0};
    for (auto const& field : records->type.fields()) {
      if (index == field_count) {
        break;
      }
      auto child = check(records->array->GetFlattenedField(
        detail::narrow<int>(index), tenzir::arrow_memory_pool()));
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

/// Matches one Sigma field as a string or as a nested list of strings. Sigma
/// applies field modifiers to every list element and joins the results with
/// `or` for one rule value.
auto field_regex_match(series const& input, re2::RE2 const& regex,
                       std::vector<bool>& matches) -> void {
  TENZIR_ASSERT(std::cmp_equal(input.length(), matches.size()));
  if (auto const strings = input.as<string_type>()) {
    auto const& array = *strings->array;
    for (auto i = int64_t{0}; i < array.length(); ++i) {
      if (matches[i] or array.IsNull(i)) {
        continue;
      }
      auto const value = array.GetView(i);
      matches[i] = re2::RE2::PartialMatch({value.data(), value.size()}, regex);
    }
    return;
  }
  auto const lists = input.as<list_type>();
  if (not lists) {
    if (input.as<record_type>() or input.as<map_type>()) {
      return;
    }
    auto row = size_t{0};
    for (auto value : input.values()) {
      if (not matches[row]) {
        auto const materialized = materialize(value);
        if (not is<caf::none_t>(materialized)) {
          auto const text = to_string(materialized);
          matches[row] = re2::RE2::PartialMatch(text, regex);
        }
      }
      ++row;
    }
    return;
  }
  auto const base = lists->array->value_offset(0);
  auto values = lists->list_values();
  auto nested = std::vector<bool>(values.length(), false);
  field_regex_match(values, regex, nested);
  for (auto i = int64_t{0}; i < lists->length(); ++i) {
    if (matches[i] or lists->array->IsNull(i)) {
      continue;
    }
    auto const begin = nested.begin() + lists->array->value_offset(i) - base;
    auto const end = nested.begin() + lists->array->value_offset(i + 1) - base;
    matches[i] = std::ranges::any_of(begin, end, std::identity{});
  }
}

using ProjectionError = ocsf::ProjectionError;

struct EvaluationField {
  std::string sigma_field;
  FieldBinding binding;
  /// Guards of the planned variants that reference this field. The field
  /// only materializes for a slice when one of these guards passes for at
  /// least one row, or when an ungated rule uses it.
  std::vector<size_t> guard_users;
  bool ungated_user = false;
};

struct LogsourceGuard {
  std::string mapping_id;
  ocsf::EvaluationGuard guard;
  std::string column;
};

/// One schema-specific compilation of a rule against one mapping
/// alternative. Alternatives of one rule target distinct OCSF classes, so at
/// most one variant matches a given event.
struct PlannedVariant {
  ast::expression expression;
  std::vector<IdentifierArtifact> identifiers;
  FieldBindings bindings;
  Option<size_t> guard;
};

struct PlannedRule {
  bool supported = false;
  std::vector<PlannedVariant> variants;
  std::vector<std::string> unsupported_fields;
  std::string reason;
};

struct SchemaEvaluationPlan {
  bool ocsf = false;
  std::vector<EvaluationField> fields;
  std::vector<LogsourceGuard> guards;
  std::vector<PlannedRule> rules;
};

/// The namespace of the private columns the evaluation slice appends.
constexpr auto private_column_prefix = std::string_view{"__sigma_ocsf_"};

auto private_column_name(type const& schema, std::string_view role,
                         size_t index) -> std::string {
  auto result = fmt::format("{}{}_{}", private_column_prefix, index, role);
  auto const& root = as<record_type>(schema);
  while (root.has_field(result)) {
    result += '_';
  }
  return result;
}

auto find_or_add_field(SchemaEvaluationPlan& plan, type const& schema,
                       std::string_view sigma_field, FieldSource source)
  -> Result<size_t, ProjectionError> {
  if (auto error = ocsf::validate(schema, sigma_field, source)) {
    return Err{std::move(*error)};
  }
  auto const existing
    = std::ranges::find_if(plan.fields, [&](EvaluationField const& field) {
        return field.sigma_field == sigma_field
               and field.binding.source == source;
      });
  if (existing != plan.fields.end()) {
    return detail::narrow<size_t>(existing - plan.fields.begin());
  }
  auto const index = plan.fields.size();
  auto constant_evidence
    = ocsf::constant_evidence_path(source.value, sigma_field);
  auto binding = FieldBinding{
    std::move(source),
    private_column_name(schema, "value", index),
    private_column_name(schema, "present", index),
    constant_evidence,
    constant_evidence ? std::string{}
                      : private_column_name(schema, "path", index),
  };
  plan.fields.push_back(
    EvaluationField{std::string{sigma_field}, std::move(binding)});
  return index;
}

auto find_or_add_guard(SchemaEvaluationPlan& plan, type const& schema,
                       ocsf::Mapping const& mapping, bool provenance)
  -> size_t {
  auto guard = LogsourceGuard{
    .mapping_id = std::string{mapping.id},
    .guard = ocsf::make_guard(mapping, provenance),
  };
  auto const existing
    = std::ranges::find_if(plan.guards, [&](LogsourceGuard const& candidate) {
        return candidate.mapping_id == guard.mapping_id
               and candidate.guard.event == guard.guard.event;
      });
  if (existing != plan.guards.end()) {
    return detail::narrow<size_t>(existing - plan.guards.begin());
  }
  guard.column = private_column_name(schema, "guard",
                                     plan.fields.size() + plan.guards.size());
  plan.guards.push_back(std::move(guard));
  return plan.guards.size() - 1;
}

/// Collects the field names a rule references, in reference order.
auto rule_fields(ir::DetectionRule const& rule) -> std::vector<std::string> {
  auto result = std::vector<std::string>{};
  auto add = [&](std::string_view field) {
    if (std::ranges::find(result, field) == result.end()) {
      result.emplace_back(field);
    }
  };
  for (auto const& [name, detection] : rule.detections) {
    TENZIR_UNUSED(name);
    for (auto const& group : detection.groups) {
      for (auto const& item : group) {
        if (item.kind != ir::DetectionItem::ItemKind::keyword) {
          add(item.field.raw);
        }
        if (std::ranges::find(item.modifiers, "fieldref")
            == item.modifiers.end()) {
          continue;
        }
        for (auto const& value : item.values) {
          if (auto const* field = try_as<std::string>(&value)) {
            add(*field);
          }
        }
      }
    }
  }
  return result;
}

/// Returns whether a rule scans keywords, whose lowering embeds the root
/// field count of the schema.
auto uses_keywords(ir::DetectionRule const& rule) -> bool {
  for (auto const& [name, detection] : rule.detections) {
    TENZIR_UNUSED(name);
    for (auto const& group : detection.groups) {
      for (auto const& item : group) {
        if (item.kind == ir::DetectionItem::ItemKind::keyword) {
          return true;
        }
      }
    }
  }
  return false;
}

/// The mapping alternatives a rule plans against: one per catalog mapping
/// that claims its logsource, or a single alternative without a mapping that
/// resolves every field literally and carries no guard.
auto mapping_alternatives(ir::DetectionRule const& rule)
  -> std::vector<ocsf::Mapping const*> {
  auto result = std::vector<ocsf::Mapping const*>{};
  for (auto const& mapping : ocsf::find_mappings(rule.log_source)) {
    result.push_back(&*mapping);
  }
  if (result.empty()) {
    result.push_back(nullptr);
  }
  return result;
}

/// Resolves how a rule field reads the event under one mapping alternative:
/// through the catalog projection, literally without one, or not at all for
/// a field the catalog declares unmapped, which the source-to-OCSF conversion
/// cannot preserve and which therefore never falls back to literal
/// resolution.
auto resolve_field_source(ocsf::Mapping const* mapping, std::string_view field)
  -> Option<FieldSource> {
  if (not mapping) {
    return FieldSource{ocsf::LiteralField{}};
  }
  auto const unmapped
    = std::ranges::find(mapping->unmapped, field, &ocsf::UnmappedField::field);
  if (unmapped != mapping->unmapped.end()) {
    return None{};
  }
  if (auto projection = ocsf::find_field(*mapping, field)) {
    return *projection;
  }
  return FieldSource{ocsf::LiteralField{}};
}

auto plan_rule(SchemaEvaluationPlan& plan, type const& schema,
               RuleEntry const& entry, diagnostic_handler& dh) -> PlannedRule {
  auto result = PlannedRule{};
  auto const fields = rule_fields(entry.adjusted);
  auto const keyword_field_count = as<record_type>(schema).num_fields();
  for (auto const* mapping : mapping_alternatives(entry.adjusted)) {
    auto const fields_before = plan.fields.size();
    auto const guards_before = plan.guards.size();
    auto rollback = [&] {
      plan.fields.resize(fields_before);
      plan.guards.resize(guards_before);
    };
    auto variant = PlannedVariant{};
    auto field_indices = std::vector<size_t>{};
    auto unsupported_fields = std::vector<std::string>{};
    auto reason = std::string{};
    auto provenance = false;
    for (auto const& field : fields) {
      auto source = resolve_field_source(mapping, field);
      if (not source) {
        TENZIR_ASSERT(mapping);
        auto const unmapped = std::ranges::find(mapping->unmapped, field,
                                                &ocsf::UnmappedField::field);
        TENZIR_ASSERT(unmapped != mapping->unmapped.end());
        unsupported_fields.emplace_back(field);
        if (reason.empty()) {
          reason = fmt::format("field `{}` is unmapped: {}", field,
                               unmapped->reason);
        }
        continue;
      }
      provenance = provenance or source->provenance_scoped;
      auto index = find_or_add_field(plan, schema, field, std::move(*source));
      if (index.is_err()) {
        unsupported_fields.emplace_back(field);
        if (reason.empty()) {
          reason = std::move(index).unwrap_err().message;
        }
        continue;
      }
      field_indices.push_back(index.unwrap());
      variant.bindings[field] = plan.fields[index.unwrap()].binding;
    }
    if (not unsupported_fields.empty()) {
      rollback();
      if (result.unsupported_fields.empty()) {
        result.unsupported_fields = std::move(unsupported_fields);
        if (result.reason.empty()) {
          result.reason = std::move(reason);
        }
      }
      continue;
    }
    if (mapping) {
      variant.guard = find_or_add_guard(plan, schema, *mapping, provenance);
    }
    auto const guard_column
      = variant.guard
          ? Option<std::string_view>{plan.guards[*variant.guard].column}
          : None{};
    auto lowered = lower_rule_with_artifacts(entry.adjusted, variant.bindings,
                                             guard_column, keyword_field_count);
    if (lowered.is_err()) {
      if (result.reason.empty()) {
        result.reason = std::move(lowered).unwrap_err().message;
      }
      rollback();
      continue;
    }
    auto [expression, identifiers] = std::move(lowered).unwrap();
    auto provider = session_provider::make(dh);
    auto resolved = bool{resolve_entities(expression, provider.as_session())};
    for (auto& identifier : identifiers) {
      resolved = resolved
                 and bool{resolve_entities(identifier.expression,
                                           provider.as_session())};
      for (auto& group : identifier.groups) {
        for (auto& item : group) {
          resolved = resolved
                     and bool{resolve_entities(item.expression,
                                               provider.as_session())};
        }
      }
    }
    if (not resolved) {
      if (result.reason.empty()) {
        result.reason = "failed to resolve the schema-specific Sigma rule";
      }
      rollback();
      continue;
    }
    // Record which guard gates each referenced field so that projections
    // only materialize for slices where the guard can pass.
    for (auto const index : field_indices) {
      auto& field = plan.fields[index];
      if (not variant.guard) {
        field.ungated_user = true;
      } else if (std::ranges::find(field.guard_users, *variant.guard)
                 == field.guard_users.end()) {
        field.guard_users.push_back(*variant.guard);
      }
    }
    variant.expression = std::move(expression);
    variant.identifiers = std::move(identifiers);
    result.variants.push_back(std::move(variant));
  }
  result.supported = not result.variants.empty();
  if (result.supported) {
    result.unsupported_fields.clear();
    result.reason.clear();
  }
  return result;
}

auto make_schema_plan(type const& schema, RuleMap const& rules,
                      diagnostic_handler& dh) -> SchemaEvaluationPlan {
  auto result = SchemaEvaluationPlan{};
  result.ocsf = ocsf::is_schema(schema);
  if (not result.ocsf) {
    return result;
  }
  result.rules.reserve(rules.entries.size());
  for (auto const& [key, entry] : rules.entries) {
    TENZIR_UNUSED(key);
    result.rules.push_back(plan_rule(result, schema, entry, dh));
  }
  return result;
}

/// Per-slice guard evaluation results with family activity flags. A family
/// whose guard rejects every row of a slice contributes no projections and
/// its rule variants skip evaluation entirely, so the per-slice cost scales
/// with the number of *eligible* mapping families instead of catalog size.
struct GuardEvaluation {
  series root;
  std::vector<series> values;
  std::vector<bool> active;
  std::vector<bool> field_active;
};

auto evaluate_guards(table_slice const& input, SchemaEvaluationPlan const& plan)
  -> GuardEvaluation {
  auto [root_type, root_array] = offset{}.get(input);
  auto result = GuardEvaluation{
    .root = series{std::move(root_type), std::move(root_array)},
    .values = {},
    .active = {},
    .field_active = {},
  };
  result.values.reserve(plan.guards.size());
  result.active.reserve(plan.guards.size());
  for (auto const& guard : plan.guards) {
    auto values = ocsf::evaluate_guard(result.root, guard.guard);
    auto const booleans = values.as<bool_type>();
    TENZIR_ASSERT(booleans);
    result.active.push_back(booleans->array->true_count() > 0);
    result.values.push_back(std::move(values));
  }
  result.field_active.reserve(plan.fields.size());
  for (auto const& field : plan.fields) {
    auto const active
      = field.ungated_user
        or std::ranges::any_of(field.guard_users, [&](size_t guard) {
             return result.active[guard];
           });
    result.field_active.push_back(active);
  }
  return result;
}

auto make_evaluation_slice(table_slice const& input,
                           SchemaEvaluationPlan const& plan,
                           GuardEvaluation const& guards) -> table_slice {
  auto const input_batch = to_record_batch(input);
  auto arrays = input_batch->columns();
  auto fields = std::vector<struct record_type::field>{};
  fields.reserve(arrays.size() + plan.fields.size() * 3 + plan.guards.size());
  for (auto const& field : as<record_type>(input.schema()).fields()) {
    fields.emplace_back(std::string{field.name}, field.type);
  }
  for (auto index = size_t{0}; index < plan.fields.size(); ++index) {
    if (not guards.field_active[index]) {
      continue;
    }
    auto const& field = plan.fields[index];
    auto projected
      = ocsf::project(guards.root, field.sigma_field, field.binding.source);
    TENZIR_ASSERT(projected.is_ok());
    auto values = std::move(projected).unwrap();
    fields.emplace_back(field.binding.value_column, values.value.type);
    arrays.push_back(std::move(values.value.array));
    fields.emplace_back(field.binding.presence_column, values.presence.type);
    arrays.push_back(std::move(values.presence.array));
    if (not field.binding.constant_evidence_path) {
      TENZIR_ASSERT(values.evidence_path);
      fields.emplace_back(field.binding.evidence_path_column,
                          values.evidence_path->type);
      arrays.push_back(std::move(values.evidence_path->array));
    }
  }
  for (auto index = size_t{0}; index < plan.guards.size(); ++index) {
    if (not guards.active[index]) {
      continue;
    }
    fields.emplace_back(plan.guards[index].column, guards.values[index].type);
    arrays.push_back(guards.values[index].array);
  }
  auto schema = type{"tenzir.sigma.evaluation", record_type{fields}};
  auto batch = arrow::RecordBatch::Make(schema.to_arrow_schema(),
                                        detail::narrow<int64_t>(input.rows()),
                                        std::move(arrays));
  return table_slice{std::move(batch), std::move(schema)};
}

/// Everything about a schema that planning observes, gathered once per rule
/// revision. Planning reads a schema only through `ocsf::validate` on the
/// resolved field sources, through the root field count that keyword scans
/// embed, and through root fields that collide with the private column
/// namespace. Two schemas that agree on these compile to identical plans.
struct PlanInputs {
  /// Every path whose type decides a projection's validation outcome, sorted
  /// and unique.
  std::vector<std::string> paths;
  bool keywords = false;
};

auto collect_plan_inputs(RuleMap const& rules) -> PlanInputs {
  auto result = PlanInputs{};
  for (auto const& [key, entry] : rules.entries) {
    TENZIR_UNUSED(key);
    auto const fields = rule_fields(entry.adjusted);
    for (auto const* mapping : mapping_alternatives(entry.adjusted)) {
      for (auto const& field : fields) {
        auto const source = resolve_field_source(mapping, field);
        if (not source) {
          continue;
        }
        for (auto& path : ocsf::validated_paths(field, *source)) {
          result.paths.push_back(std::move(path));
        }
      }
    }
    result.keywords = result.keywords or uses_keywords(entry.adjusted);
  }
  std::ranges::sort(result.paths);
  auto const duplicates = std::ranges::unique(result.paths);
  result.paths.erase(duplicates.begin(), duplicates.end());
  return result;
}

/// Keys the plan cache by what planning observes instead of by the schema
/// fingerprint, so that schema-rich input whose records differ only in
/// fields no rule reads shares one compiled plan. Every non-OCSF schema
/// shares the empty plan.
auto schema_plan_key(type const& schema, PlanInputs const& inputs)
  -> std::string {
  if (not ocsf::is_schema(schema)) {
    return {};
  }
  auto result = std::string{"ocsf;"};
  auto const& root = as<record_type>(schema);
  if (inputs.keywords) {
    fmt::format_to(std::back_inserter(result), "fields={};", root.num_fields());
  }
  for (auto const& field : root.fields()) {
    if (field.name.starts_with(private_column_prefix)) {
      fmt::format_to(std::back_inserter(result), "collision={};", field.name);
    }
  }
  result += ocsf::schema_shape(schema, inputs.paths);
  return result;
}

/// The plan cache retains at most this many cost units per loaded rule. A
/// plan costs one unit per rule plus one per compiled variant, so with the
/// usual one or two variants per rule the cache holds between five and eight
/// complete compilations of the corpus. Peak memory thus stays proportional
/// to the corpus the user loaded, however many schema shapes the input has;
/// a count bound would instead multiply the corpus by the schema variety.
constexpr auto plan_cache_units_per_rule = uint64_t{16};

/// The smallest plan cache budget, so that a handful of rules over
/// schema-rich input keeps every shape's plan resident; such plans are tiny.
constexpr auto plan_cache_min_budget = uint64_t{4096};

auto plan_cache_budget(RuleMap const& rules) -> uint64_t {
  return std::max(plan_cache_min_budget,
                  plan_cache_units_per_rule * rules.entries.size());
}

auto plan_cost(SchemaEvaluationPlan const& plan) -> uint64_t {
  auto result = uint64_t{plan.rules.size()};
  for (auto const& rule : plan.rules) {
    result += rule.variants.size();
  }
  return result;
}

struct MappingState {
  auto reset(uint64_t revision, RuleMap const& rules) -> void {
    if (rules_revision == revision) {
      return;
    }
    rules_revision = revision;
    plans.clear();
    plans.budget(plan_cache_budget(rules));
    inputs = collect_plan_inputs(rules);
    std::erase_if(warned_rules, [&](auto const& warning) {
      auto const entry = rules.index.find(warning.first);
      return entry == rules.index.end()
             or rules.entries[entry->second].second.revision != warning.second;
    });
  }

  auto should_warn(RuleKey const& key, uint64_t revision) -> bool {
    if (auto const entry = warned_rules.find(key);
        entry != warned_rules.end() and entry->second == revision) {
      return false;
    }
    warned_rules.insert_or_assign(key, revision);
    return true;
  }

  uint64_t rules_revision = 0;
  PlanInputs inputs;
  BudgetedLruCache<std::string, SchemaEvaluationPlan> plans{
    plan_cache_min_budget};
  std::unordered_map<RuleKey, uint64_t, RuleKeyHash> warned_rules;
};

struct SliceMatchStats {
  uint64_t evaluated_rules = 0;
  uint64_t matches = 0;
};

auto emit_unsupported_rule(RuleKey const& key, RuleEntry const& entry,
                           PlannedRule const& plan, MappingState& state,
                           diagnostic_handler& dh) -> void {
  if (not state.should_warn(key, entry.revision)) {
    return;
  }
  auto builder = diagnostic::warning(
    "sigma operator skips rule '{}' for OCSF input", entry.label);
  if (entry.source) {
    builder = std::move(builder).primary(entry.source);
  }
  if (not plan.unsupported_fields.empty()) {
    auto fields = std::string{};
    for (auto const& field : plan.unsupported_fields) {
      if (not fields.empty()) {
        fields += ", ";
      }
      fields += fmt::format("`{}`", field);
    }
    builder = std::move(builder).note("unresolved Sigma fields: {}", fields);
  }
  if (not plan.reason.empty()) {
    builder = std::move(builder).note("{}", plan.reason);
  }
  std::move(builder)
    .note("use `mapping=\"direct\"` only when the rule fields exist literally")
    .emit(dh);
}

/// Matches every rule against one slice, yielding each rule's output as soon
/// as it exists. Streaming per rule bounds peak memory: a broad corpus over
/// a large slice never retains the complete cross-rule result set. The stats
/// are complete once the generator is exhausted.
auto match_slice(table_slice const& input, RuleMap const& rules,
                 sigma_mapping mapping, sigma_format format,
                 MappingState& state, SliceMatchStats& stats,
                 diagnostic_handler& dh) -> generator<table_slice> {
  state.reset(rules.revision, rules);
  auto direct = mapping == sigma_mapping::direct;
  SchemaEvaluationPlan const* plan = nullptr;
  if (not direct) {
    auto const plan_key = schema_plan_key(input.schema(), state.inputs);
    plan = state.plans.get(plan_key);
    if (not plan) {
      auto compiled = make_schema_plan(input.schema(), rules, dh);
      auto const cost = plan_cost(compiled);
      plan = &state.plans.put(plan_key, std::move(compiled), cost);
    }
    direct = not plan->ocsf;
  }
  if (direct) {
    stats.evaluated_rules = rules.entries.size();
    for (auto const& [key, entry] : rules.entries) {
      TENZIR_UNUSED(key);
      auto output = build_output(input, input, entry, entry.rule,
                                 entry.identifiers, None{}, format, dh);
      for (auto& slice : output) {
        stats.matches += slice.rows();
        co_yield std::move(slice);
      }
    }
    co_return;
  }
  TENZIR_ASSERT(plan->rules.size() == rules.entries.size());
  auto const guards = evaluate_guards(input, *plan);
  auto const evaluation = make_evaluation_slice(input, *plan, guards);
  for (auto index = size_t{0}; index < rules.entries.size(); ++index) {
    auto const& [key, entry] = rules.entries[index];
    auto const& planned = plan->rules[index];
    if (not planned.supported) {
      emit_unsupported_rule(key, entry, planned, state, dh);
      continue;
    }
    ++stats.evaluated_rules;
    for (auto const& variant : planned.variants) {
      // A variant whose guard rejects every row cannot match; the guard is
      // a conjunct of the variant expression.
      if (variant.guard and not guards.active[*variant.guard]) {
        continue;
      }
      auto output
        = build_output(input, evaluation, entry, variant.expression,
                       variant.identifiers, variant.bindings, format, dh);
      for (auto& slice : output) {
        stats.matches += slice.rows();
        co_yield std::move(slice);
      }
    }
  }
}

/// Internal function implementing Sigma keyword selections. An optional field
/// count limits the outer record to its original columns during semantic
/// evaluation.
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
    auto field_count = located<int64_t>{};
    TRY(argument_parser2::function(name())
          .positional("input", expr, "any")
          .positional("regex", pattern)
          .positional("field_count", field_count)
          .parse(inv, ctx));
    auto regex = std::make_shared<re2::RE2>(pattern.inner,
                                            re2::RE2::CannedOptions::Quiet);
    if (not regex->ok()) {
      diagnostic::error("failed to parse regex: {}", regex->error())
        .primary(pattern)
        .emit(ctx);
      return failure::promise();
    }
    if (field_count.inner < -1) {
      diagnostic::error("field count must be `-1` or non-negative")
        .primary(field_count)
        .emit(ctx);
      return failure::promise();
    }
    return function_use::make([expr = std::move(expr), regex = std::move(regex),
                               field_count = field_count.inner](
                                evaluator eval, session ctx) -> multi_series {
      TENZIR_UNUSED(ctx);
      return map_series(eval(expr), [&](series input) -> multi_series {
        auto matches = std::vector<bool>(input.length(), false);
        auto const limit
          = field_count < 0
              ? Option<size_t>{None{}}
              : Option<size_t>{detail::narrow<size_t>(field_count)};
        keyword_match(input, *regex, matches, limit);
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

/// Internal function implementing Sigma string matching for scalar and list
/// fields.
class SigmaRegexFunction final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "_sigma_regex";
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
          // The outer validity bitmap answers row-level null checks without
          // materializing nested values.
          auto present = std::vector<bool>{};
          present.reserve(detail::narrow<size_t>(input.length()));
          auto const all_null = is<null_type>(input.type);
          for (auto row = int64_t{0}; row < input.length(); ++row) {
            present.push_back(not all_null and not input.array->IsNull(row));
          }
          field_regex_match(input, *regex, matches);
          auto builder = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
          check(builder.Reserve(input.length()));
          for (auto row = size_t{0}; row < matches.size(); ++row) {
            if (present[row]) {
              check(builder.Append(matches[row]));
            } else {
              check(builder.AppendNull());
            }
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
          return ocsf::resolve_field(input, field.inner);
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
          return ocsf::resolve_presence(input, field.inner);
        });
      });
  }
};

// -- argument handling -----------------------------------------------------

constexpr auto default_refresh_interval = std::chrono::seconds{5};

/// Validates the requested Sigma output format.
auto normalize_format(Option<located<std::string>> const& format)
  -> Result<sigma_format, diagnostic> {
  if (not format) {
    return sigma_format::ocsf;
  }
  auto result = from_string<sigma_format>(format->inner);
  if (not result) {
    return Err{diagnostic::error("unsupported format")
                 .primary(format->source)
                 .note("available formats: `ocsf`, `plain`")
                 .done()};
  }
  return *result;
}

/// Validates how Sigma fields are matched against input events.
auto normalize_mapping(Option<located<std::string>> const& mapping)
  -> Result<sigma_mapping, diagnostic> {
  if (not mapping or mapping->inner == "auto") {
    return sigma_mapping::automatic;
  }
  if (mapping->inner == "direct") {
    return sigma_mapping::direct;
  }
  return Err{diagnostic::error("unsupported mapping")
               .primary(mapping->source)
               .note("available mappings: `auto`, `direct`")
               .done()};
}

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
    // The evaluated argument only carries values and the complete span; the
    // AST supplies each entry's location and literal syntax afterwards.
    result.rule_sources.assign(result.rules.size(), InlineRuleOrigin{});
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

/// Recovers the location and literal syntax of every inline rule from the
/// `rules` argument's AST: one string literal, or a list whose elements are
/// string literals. The evaluated argument only carries the values and the
/// complete span, which cannot tell a raw string from a plain one with
/// escapes. An entry produced by any other expression keeps its location
/// but no syntax, so diagnostics point at the complete expression.
auto locate_inline_rules(ast::expression const& rules, SigmaSources& sources)
  -> void {
  auto origin_of = [](ast::expression const& expression) {
    auto result = InlineRuleOrigin{.source = expression.get_location()};
    auto const* constant = try_as<ast::constant>(expression);
    if (constant and is<std::string>(constant->value)) {
      result.literal
        = constant->raw ? InlineLiteral::raw : InlineLiteral::plain;
    }
    return result;
  };
  rules.match(
    [&](ast::constant const&) {
      if (sources.rule_sources.size() == 1) {
        sources.rule_sources[0] = origin_of(rules);
      }
    },
    [&](ast::list const& list) {
      auto origins = std::vector<InlineRuleOrigin>{};
      for (auto const& item : list.items) {
        auto const* expression = try_as<ast::expression>(&item);
        if (not expression) {
          return;
        }
        origins.push_back(origin_of(*expression));
      }
      if (origins.size() == sources.rules.size()) {
        sources.rule_sources = std::move(origins);
      }
    },
    [](auto const&) {});
}

/// Finds the `rules=` argument expression of an invocation by its location.
auto find_rules_expression(std::vector<ast::expression> const& args,
                           location rules_source)
  -> Option<ast::expression const&> {
  for (auto const& arg : args) {
    auto found = arg.match(
      [&](ast::assignment const& assignment) -> Option<ast::expression const&> {
        if (assignment.right.get_location() == rules_source) {
          return assignment.right;
        }
        return None{};
      },
      [](auto const&) -> Option<ast::expression const&> {
        return None{};
      });
    if (found) {
      return found;
    }
  }
  return None{};
}

/// Compiles inline rule content at pipeline-construction time so that
/// diagnostics anchor at the TQL argument and carry the list element and
/// YAML document indices.
auto validate_inline_rules(SigmaSources const& sources, location source)
  -> Option<diagnostic> {
  auto const& rules = sources.rules;
  for (auto index = size_t{0}; index < rules.size(); ++index) {
    // An entry without its own location falls back to the complete argument
    // and, lacking a literal to point into, keeps every document there.
    auto const rule_source = index < sources.rule_sources.size()
                                 and sources.rule_sources[index].source
                               ? sources.rule_sources[index]
                               : InlineRuleOrigin{.source = source};
    auto const document_locations
      = inline_document_locations(rules[index], rule_source);
    auto documents = from_yaml_documents(rules[index]);
    if (not documents) {
      return diagnostic::error("invalid YAML in `rules`")
        .primary(source)
        .note("list element {}: {}", index, documents.error())
        .done();
    }
    if (documents->empty()) {
      return diagnostic::error("`rules` must not be empty")
        .primary(source)
        .note("list element {} contains no YAML documents", index)
        .done();
    }
    for (auto doc = size_t{0}; doc < documents->size(); ++doc) {
      auto const& document = (*documents)[doc];
      auto const doc_source
        = doc < document_locations.size() ? document_locations[doc] : source;
      auto invalid = [&](std::string_view reason) {
        return diagnostic::error("invalid Sigma rule in `rules`")
          .primary(doc_source)
          .note("list element {}, document {}: {}", index, doc, reason)
          .done();
      };
      if (not is<record>(document)) {
        return invalid("rule is not a YAML dictionary");
      }
      auto parsed = ir::parse_document(document);
      if (parsed.is_err()) {
        return invalid(std::move(parsed).unwrap_err().message);
      }
      // All immutable inline documents must lower successfully. Filter targets
      // resolve later against the complete rule set.
      auto const& parsed_document = parsed.unwrap();
      auto lowered = [&]() -> ParseResult<ast::expression> {
        if (auto const* rule
            = try_as<ir::DetectionRule>(parsed_document.content)) {
          return lower_rule(*rule);
        }
        if (auto const* filter
            = try_as<ir::FilterRule>(parsed_document.content)) {
          return lower_filter(*filter);
        }
        TENZIR_UNREACHABLE();
      }();
      if (lowered.is_err()) {
        return invalid(std::move(lowered).unwrap_err().message);
      }
    }
  }
  return None{};
}

class sigma_operator final : public crtp_operator<sigma_operator> {
public:
  sigma_operator() = default;

  sigma_operator(duration refresh_interval, SigmaSources sources,
                 sigma_format format, sigma_mapping mapping)
    : refresh_interval_{refresh_interval},
      sources_{std::move(sources)},
      format_{format},
      mapping_{mapping} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto rules = RuleMap{};
    auto reload_state = ReloadState{};
    auto filter_bank = FilterBank{};
    auto mapping_state = MappingState{};
    auto diagnostics
      = make_source_diagnostic_handler(ctrl.diagnostics(), sources_.source);
    std::ignore
      = update_rules(sources_, rules, filter_bank, reload_state, diagnostics);
    mapping_state.reset(rules.revision, rules);
    auto metrics = ctrl.metrics(sigma_metrics_type);
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
        if (update_rules(sources_, rules, filter_bank, reload_state,
                         diagnostics)) {
          mapping_state.reset(rules.revision, rules);
        }
        last_update = now;
      }
      auto const events = static_cast<uint64_t>(slice.rows());
      auto stats = SliceMatchStats{};
      for (auto&& result : match_slice(slice, rules, mapping_, format_,
                                       mapping_state, stats, diagnostics)) {
        co_yield std::move(result);
      }
      emit_processing_metrics(metrics, events, stats.evaluated_rules,
                              stats.matches);
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

  auto optimize(expression const& filter, EventOrder order) const
    -> OptimizeResult override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, sigma_operator& x) -> bool {
    return f.object(x)
      .pretty_name("sigma_operator")
      .fields(f.field("refresh_interval", x.refresh_interval_),
              f.field("sources", x.sources_), f.field("format", x.format_),
              f.field("mapping", x.mapping_));
  }

private:
  duration refresh_interval_ = {};
  SigmaSources sources_;
  sigma_format format_ = sigma_format::ocsf;
  sigma_mapping mapping_ = sigma_mapping::automatic;
};

struct SigmaArgs {
  Option<located<std::string>> legacy_path;
  Option<located<data>> path;
  /// Kept as an expression so that list elements keep their locations; the
  /// value is constant and evaluated when the operator resolves its sources.
  Option<ast::expression> rules;
  Option<located<duration>> refresh_interval;
  Option<located<std::string>> format;
  Option<located<std::string>> mapping;
  location operator_location = location::unknown;
};

class Sigma final : public Operator<table_slice, table_slice> {
public:
  explicit Sigma(SigmaArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto rules = Option<located<data>>{};
    if (args_.rules) {
      auto evaluated = const_eval(*args_.rules, ctx.dh());
      // Argument validation already ran at pipeline-construction time.
      TENZIR_ASSERT(evaluated);
      rules = std::move(*evaluated);
    }
    auto sources
      = normalize_sources(args_.legacy_path, args_.path, rules,
                          args_.refresh_interval, args_.operator_location);
    TENZIR_ASSERT(sources.is_ok());
    sources_ = std::move(sources).unwrap();
    if (args_.rules) {
      locate_inline_rules(*args_.rules, sources_);
    }
    auto format = normalize_format(args_.format);
    // Argument validation already ran at pipeline-construction time.
    TENZIR_ASSERT(format.is_ok());
    format_ = std::move(format).unwrap();
    auto mapping = normalize_mapping(args_.mapping);
    TENZIR_ASSERT(mapping.is_ok());
    mapping_ = std::move(mapping).unwrap();
    refresh_interval_ = args_.refresh_interval ? args_.refresh_interval->inner
                                               : default_refresh_interval;
    auto diagnostics
      = make_source_diagnostic_handler(ctx.dh(), sources_.source);
    std::ignore = update_rules(sources_, rules_, filter_bank_, reload_state_,
                               diagnostics);
    mapping_state_.reset(rules_.revision, rules_);
    metrics_ = make_metric_handler(ctx, sigma_metrics_type);
    last_update_ = std::chrono::steady_clock::now();
    co_return;
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    if (input.rows() == 0) {
      co_return;
    }
    // Inline rules are part of the operator plan and never change.
    auto diagnostics
      = make_source_diagnostic_handler(ctx.dh(), sources_.source);
    auto const now = std::chrono::steady_clock::now();
    if (not sources_.paths.empty() and now - last_update_ > refresh_interval_) {
      if (update_rules(sources_, rules_, filter_bank_, reload_state_,
                       diagnostics)) {
        mapping_state_.reset(rules_.revision, rules_);
      }
      last_update_ = now;
    }
    auto const events = static_cast<uint64_t>(input.rows());
    auto stats = SliceMatchStats{};
    for (auto&& result : match_slice(input, rules_, mapping_, format_,
                                     mapping_state_, stats, diagnostics)) {
      co_await push(std::move(result));
    }
    emit_processing_metrics(metrics_, events, stats.evaluated_rules,
                            stats.matches);
  }

private:
  SigmaArgs args_;
  SigmaSources sources_;
  duration refresh_interval_ = default_refresh_interval;
  RuleMap rules_;
  FilterBank filter_bank_;
  ReloadState reload_state_;
  MappingState mapping_state_;
  metric_handler metrics_ = {};
  sigma_format format_ = sigma_format::ocsf;
  sigma_mapping mapping_ = sigma_mapping::automatic;
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
    auto format = Option<located<std::string>>{};
    auto mapping = Option<located<std::string>>{};
    TRY(argument_parser2::operator_("sigma")
          .positional("legacy_path", legacy_path)
          .named("path", path)
          .named("rules", rules)
          .named("refresh_interval", refresh_interval)
          .named("format", format)
          .named("mapping", mapping)
          .parse(inv, ctx));
    auto sources = normalize_sources(legacy_path, path, rules, refresh_interval,
                                     inv.self.get_location());
    if (sources.is_err()) {
      std::move(sources).unwrap_err().modify().emit(ctx);
      return failure::promise();
    }
    if (rules) {
      if (auto expression = find_rules_expression(inv.args, rules->source)) {
        locate_inline_rules(*expression, sources.unwrap());
      }
    }
    if (legacy_path) {
      diagnostic::warning("passing the path positionally is deprecated")
        .primary(legacy_path->source)
        .hint("use `path={:?}` instead", legacy_path->inner)
        .emit(ctx);
    }
    auto normalized_mapping = normalize_mapping(mapping);
    if (normalized_mapping.is_err()) {
      std::move(normalized_mapping).unwrap_err().modify().emit(ctx);
      return failure::promise();
    }
    if (rules) {
      if (auto error = validate_inline_rules(sources.unwrap(), rules->source)) {
        std::move(*error).modify().emit(ctx);
        return failure::promise();
      }
    }
    auto normalized_format = normalize_format(format);
    if (normalized_format.is_err()) {
      std::move(normalized_format).unwrap_err().modify().emit(ctx);
      return failure::promise();
    }
    auto const interval
      = refresh_interval ? refresh_interval->inner : default_refresh_interval;
    return std::make_unique<sigma_operator>(
      interval, std::move(sources).unwrap(),
      std::move(normalized_format).unwrap(),
      std::move(normalized_mapping).unwrap());
  }

  auto describe() const -> Description override {
    auto d = Describer<SigmaArgs, Sigma>{};
    d.parallelizable();
    auto legacy_path = d.positional("legacy_path", &SigmaArgs::legacy_path);
    auto path = d.named("path", &SigmaArgs::path);
    auto rules = d.named("rules", &SigmaArgs::rules, "string|list");
    auto refresh_interval
      = d.named("refresh_interval", &SigmaArgs::refresh_interval);
    auto format = d.named("format", &SigmaArgs::format, "ocsf|plain");
    auto mapping = d.named("mapping", &SigmaArgs::mapping, "auto|direct");
    d.operator_location(&SigmaArgs::operator_location);
    d.validate([legacy_path, path, rules, refresh_interval, format,
                mapping](DescribeCtx& ctx) -> Empty {
      auto const legacy_value = ctx.get(legacy_path);
      auto const path_value = ctx.get(path);
      auto const rules_expression = ctx.get(rules);
      auto const refresh_value = ctx.get(refresh_interval);
      auto to_option = [](auto const& value) {
        using Value = std::remove_cvref_t<decltype(*value)>;
        return value ? Option<Value>{*value} : Option<Value>{};
      };
      auto rules_value = Option<located<data>>{};
      if (rules_expression) {
        auto evaluated = const_eval(*rules_expression,
                                    static_cast<diagnostic_handler&>(ctx));
        if (not evaluated) {
          return {};
        }
        rules_value = std::move(*evaluated);
      }
      auto sources
        = normalize_sources(to_option(legacy_value), to_option(path_value),
                            rules_value, to_option(refresh_value),
                            ctx.operator_location());
      if (sources.is_err()) {
        static_cast<diagnostic_handler&>(ctx).emit(
          std::move(sources).unwrap_err());
        return {};
      }
      if (rules_expression) {
        locate_inline_rules(*rules_expression, sources.unwrap());
      }
      if (legacy_value) {
        diagnostic::warning("passing the path positionally is deprecated")
          .primary(legacy_value->source)
          .hint("use `path={:?}` instead", legacy_value->inner)
          .emit(ctx);
      }
      if (rules_value) {
        auto const mapping_value = ctx.get(mapping);
        auto normalized_mapping = normalize_mapping(to_option(mapping_value));
        if (normalized_mapping.is_ok()) {
          if (auto error
              = validate_inline_rules(sources.unwrap(), rules_value->source)) {
            static_cast<diagnostic_handler&>(ctx).emit(std::move(*error));
          }
        }
      }
      auto const format_value = ctx.get(format);
      if (auto normalized = normalize_format(to_option(format_value));
          normalized.is_err()) {
        static_cast<diagnostic_handler&>(ctx).emit(
          std::move(normalized).unwrap_err());
      }
      auto const mapping_value = ctx.get(mapping);
      if (auto normalized = normalize_mapping(to_option(mapping_value));
          normalized.is_err()) {
        static_cast<diagnostic_handler&>(ctx).emit(
          std::move(normalized).unwrap_err());
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
TENZIR_REGISTER_PLUGIN(tenzir::plugins::sigma::SigmaRegexFunction)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::sigma::SigmaFieldFunction)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::sigma::SigmaHasFunction)
