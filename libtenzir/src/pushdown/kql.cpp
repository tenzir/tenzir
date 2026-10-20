//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pushdown/kql.hpp"

#include "tenzir/pushdown/translate.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/try.hpp"
#include "tenzir/unicode.hpp"
#include "tenzir/variant_traits.hpp"

#include <fmt/chrono.h>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <limits>
#include <span>

namespace tenzir::pushdown {

namespace {

/// The most text that the planner appends to a query. Predicates that do not
/// fit stay local, which only costs reduction, never correctness.
constexpr auto max_appendix = size_t{16'384};

/// The largest row count that the planner spells in a `take` stage. KQL
/// integer literals are `long`, and both Defender and Log Analytics accept
/// `take 9223372036854775807`.
constexpr auto max_take = uint64_t{std::numeric_limits<int64_t>::max()};

/// The bare words that make a query unfit for trailing stages: `render` must
/// end a query, `fork` and `facet` return several tables, and a `set`
/// statement may change how the service limits a result, for example with
/// `set notruncation` or with `set query_take_max_records`, which may truncate
/// it silently.
constexpr auto opaque_words = std::array{
  std::string_view{"render"},
  std::string_view{"fork"},
  std::string_view{"facet"},
  std::string_view{"set"},
};

/// The `gettype()` results of the values that decode to TQL numbers. Typed
/// `int`, `long`, and `real` columns and `dynamic` numbers decode to numbers
/// of the same value; a `dynamic` real reports `double`. `decimal` decodes to
/// a string, and is excluded.
constexpr auto number_types
  = std::string_view{R"("int", "long", "real", "double")"};

/// The `gettype()` results of typed columns whose `null` the sources decode to
/// `null` only: their decoders reject every other spelling of a missing value,
/// such as an empty string, and fail the whole response instead.
constexpr auto strictly_null_types = std::string_view{
  R"("int", "long", "real", "bool", "datetime", "timespan", "decimal")"};

/// Quotes `text` as a KQL string literal delimited by `quote`, or returns
/// `None` for invalid UTF-8, which the JSON request body cannot carry. KQL
/// strings support the backslash escapes `\\`, `\"`, `\'`, `\t`, `\n`, `\r`,
/// and `\uXXXX`, which spell the delimiter, the backslash, and control
/// characters. U+0085, U+2028, and U+2029 are escaped, too: Kusto's lexer, like
/// `kql_accepts_trailing_stages`, may read the latter two as line breaks, which
/// a single-line literal must not contain.
auto quote_string(std::string_view text, char quote) -> Option<std::string> {
  if (not unicode::is_valid_utf8(text)) {
    return None{};
  }
  constexpr auto separators = std::array{
    std::pair{std::string_view{"\xC2\x85"}, std::string_view{"\\u0085"}},
    std::pair{std::string_view{"\xE2\x80\xA8"}, std::string_view{"\\u2028"}},
    std::pair{std::string_view{"\xE2\x80\xA9"}, std::string_view{"\\u2029"}},
  };
  auto result = std::string{quote};
  for (auto i = size_t{0}; i < text.size(); ++i) {
    auto rest = text.substr(i);
    auto separator = std::ranges::find_if(separators, [&](auto const& x) {
      return rest.starts_with(x.first);
    });
    if (separator != separators.end()) {
      result += separator->second;
      i += separator->first.size() - 1;
      continue;
    }
    auto c = text[i];
    switch (c) {
      case '\t':
        result += "\\t";
        continue;
      case '\n':
        result += "\\n";
        continue;
      case '\r':
        result += "\\r";
        continue;
      default:
        break;
    }
    auto byte = static_cast<unsigned char>(c);
    if (byte < 0x20 or byte == 0x7f) {
      result += fmt::format("\\u{:04x}", byte);
      continue;
    }
    if (c == '\\' or c == quote) {
      result += '\\';
    }
    result += c;
  }
  result += quote;
  return result;
}

/// Spells a top-level column so that it reads `null` where the query does not
/// return it. A column that the query returns keeps its own type: KQL resolves
/// `column_ifexists` while binding the query, not per row. Nested paths are
/// vetoed, since accessing a field of a non-`dynamic` column fails the query.
auto render_column(Column const& column) -> Option<std::string> {
  if (column.path.size() != 1) {
    return None{};
  }
  TRY(auto name, quote_string(column.path.front(), '\''));
  return fmt::format("column_ifexists({}, dynamic(null))", name);
}

/// Spells a number as a KQL `real` literal. Translation bounds the literals
/// that it compares with dynamic columns to 2^53, so that integers convert to
/// `double` exactly. A `double` keeps the shortest spelling that reads back as
/// the same value, with an exponent such as `1.5e-7` where that is shorter.
auto render_number(Literal const& literal) -> Option<std::string> {
  constexpr auto bound = int64_t{1} << 53;
  return match(
    literal.value,
    [&](int64_t x) -> Option<std::string> {
      if (x < -bound or x > bound) {
        return None{};
      }
      return fmt::format("{}.0", x);
    },
    [&](uint64_t x) -> Option<std::string> {
      if (x > static_cast<uint64_t>(bound)) {
        return None{};
      }
      return fmt::format("{}.0", x);
    },
    [](double x) -> Option<std::string> {
      if (not std::isfinite(x)) {
        return None{};
      }
      auto text = fmt::to_string(x);
      auto exponent = text.find('e');
      if (exponent == std::string::npos) {
        if (not text.contains('.')) {
          text += ".0";
        }
        return text;
      }
      // Spell `1e+20` as `1e20` and `1e-07` as `1e-7`, the forms that KQL
      // documents and that we validated.
      auto mantissa = text.substr(0, exponent);
      auto rest = std::string_view{text}.substr(exponent + 1);
      auto negative = rest.starts_with('-');
      if (negative or rest.starts_with('+')) {
        rest.remove_prefix(1);
      }
      while (rest.size() > 1 and rest.front() == '0') {
        rest.remove_prefix(1);
      }
      return fmt::format("{}e{}{}", mantissa, negative ? "-" : "", rest);
    },
    [](auto const&) -> Option<std::string> {
      return None{};
    });
}

/// The KQL types that a literal compares with.
enum class Kind {
  string,
  number,
  boolean,
};

auto kind_of(Literal const& literal) -> Option<Kind> {
  return match(
    literal.value,
    [](std::string const&) -> Option<Kind> {
      return Kind::string;
    },
    [](int64_t) -> Option<Kind> {
      return Kind::number;
    },
    [](uint64_t) -> Option<Kind> {
      return Kind::number;
    },
    [](double) -> Option<Kind> {
      return Kind::number;
    },
    [](bool) -> Option<Kind> {
      return Kind::boolean;
    },
    [](auto const&) -> Option<Kind> {
      return None{};
    });
}

auto type_names(Kind kind) -> std::string_view {
  switch (kind) {
    case Kind::string:
      return R"("string")";
    case Kind::number:
      return number_types;
    case Kind::boolean:
      return R"("bool")";
  }
  TENZIR_UNREACHABLE();
}

/// The KQL value of `column` converted for comparison with literals of `kind`.
/// Each conversion accepts every type, so the query compiles whatever type the
/// column has.
auto converted(std::string_view column, Kind kind) -> std::string {
  switch (kind) {
    case Kind::string:
      return fmt::format("tostring({})", column);
    case Kind::number:
      return fmt::format("todouble({})", column);
    case Kind::boolean:
      return fmt::format("tobool({})", column);
  }
  TENZIR_UNREACHABLE();
}

/// A condition on a column that applies where KQL reports a type of `kind`.
struct Match {
  Kind kind;
  std::string condition;
};

/// Applies `match` to the rows whose KQL type is of its kind, and keeps every
/// other row.
///
/// Rows of other types pass because a KQL value may decode to a TQL value of
/// another kind: `guid` and `decimal` decode to strings, and a `dynamic`
/// string that holds JSON decodes to the JSON value, such as the number `443`
/// or the boolean `true`. For strings, the predicate also keeps every string
/// that JSON parsing may have turned into another string: one that starts with
/// a quote after JSON whitespace.
auto typed(std::string_view column, Match match) -> std::string {
  if (match.kind == Kind::string) {
    match.condition
      = fmt::format("{} or tostring({}) matches regex @'^[ \\t\\r\\n]*\"'",
                    match.condition, column);
  }
  return fmt::format("(gettype({}) !in ({}) or {})", column,
                     type_names(match.kind), match.condition);
}

auto comparison_operator(BinaryOp op) -> Option<std::string_view> {
  switch (op) {
    case BinaryOp::eq:
      return "==";
    case BinaryOp::lt:
      return "<";
    case BinaryOp::leq:
      return "<=";
    case BinaryOp::gt:
      return ">";
    case BinaryOp::geq:
      return ">=";
    default:
      return None{};
  }
}

/// Resolves the column inside a parsed-IP prefilter.
auto parsed_ip_column(Expr const& expr) -> Option<std::string> {
  auto const* call = try_as<Call>(expr);
  if (not call or call->op != Operation::parse_ip or call->args.size() != 1) {
    return None{};
  }
  auto const* column = try_as<Column>(call->args.front());
  if (not column) {
    return None{};
  }
  return render_column(*column);
}

/// Compares addresses rather than text or `parse_ipv4`, which rejects mapped
/// spellings. KQL also equates IPv4-compatible and mapped addresses, which
/// only over-matches equality; subnet bounds need the checks below.
/// Translation separately keeps the spellings that KQL cannot parse.
auto render_ip_comparison(Expr const& expr, Literal const& literal,
                          std::string_view op) -> Option<std::string> {
  auto const* value = try_as<IpValue>(literal.value);
  if (not value) {
    return None{};
  }
  TRY(auto subject, parsed_ip_column(expr));
  return fmt::format("ipv6_compare(tostring({}), \"{}\") {} 0", subject,
                     value->address, op);
}

/// Compares a decoded timestamp without assuming nanosecond-exact decoding.
/// Both readers parse UTC clock fields through double seconds since midnight.
/// Within a day the floating-point error is below a nanosecond; truncation to
/// integer nanoseconds can lose another nanosecond. Widening each bound by a
/// microsecond, then rounding outwards to microseconds, covers both effects
/// and spells values exactly on KQL's 100-nanosecond grid. The exact predicate
/// stays local, including its strictness and null semantics.
///
/// Only datetime values are restricted. Dynamic datetimes can decode to
/// strings; keeping them unnecessarily is safe. Years before 1900 pass
/// untouched to preserve decoder errors. Values from 2262 onward also pass,
/// since calendar conversion can overflow the nanosecond range. The
/// deliberately whole-year upper guard covers the partial boundary year.
auto render_time_comparison(std::string_view column, BinaryOp op, time value)
  -> Option<std::string> {
  using namespace std::chrono;
  auto render = [](auto instant) {
    return fmt::format("datetime({:%Y-%m-%dT%H:%M:%S}Z)", instant);
  };
  auto lower
    = render(std::chrono::floor<microseconds>(value) - microseconds{1});
  auto upper = render(std::chrono::ceil<microseconds>(value) + microseconds{1});
  auto subject = fmt::format("todatetime({})", column);
  auto condition = std::string{};
  switch (op) {
    case BinaryOp::eq:
      condition
        = fmt::format("({0} >= {1} and {0} <= {2})", subject, lower, upper);
      break;
    case BinaryOp::gt:
    case BinaryOp::geq:
      condition = fmt::format("{} >= {}", subject, lower);
      break;
    case BinaryOp::lt:
    case BinaryOp::leq:
      condition = fmt::format("{} <= {}", subject, upper);
      break;
    default:
      return None{};
  }
  return fmt::format("(gettype({0}) != \"datetime\" or {1} < "
                     "datetime(1900-01-01) or {1} >= datetime(2262-01-01) "
                     "or {2})",
                     column, subject, condition);
}

/// Spells `column op literal`.
///
/// - A number compares in `real`, where the column's value converts exactly
///   below 2^53 and monotonically beyond, so that rounding only over-matches.
///   Translation relaxes strict comparisons at 2^53 for the same reason.
/// - A string compares for equality only. Equal strings are equal under every
///   collation, so KQL's case-sensitive `==` keeps every row that TQL keeps.
///   KQL has no ordering operators on strings, so ordering is vetoed.
/// - A boolean compares for equality only.
/// - Parsed IPs compare in the mapped address space.
auto render_comparison(Binary const& x) -> Option<std::string> {
  if (auto const* literal = try_as<Literal>(*x.right);
      literal and is<IpValue>(literal->value)) {
    // Ordering remains local. Only equality and the bounds of a subnet use
    // KQL's address comparison.
    if (x.op != BinaryOp::eq) {
      return None{};
    }
    return render_ip_comparison(*x.left, *literal, "==");
  }
  auto const* column = try_as<Column>(*x.left);
  auto const* literal = try_as<Literal>(*x.right);
  if (not column or not literal) {
    return None{};
  }
  TRY(auto subject, render_column(*column));
  if (auto const* instant = try_as<time>(literal->value)) {
    return render_time_comparison(subject, x.op, *instant);
  }
  TRY(auto op, comparison_operator(x.op));
  TRY(auto kind, kind_of(*literal));
  auto value = std::string{};
  switch (kind) {
    case Kind::string: {
      if (x.op != BinaryOp::eq) {
        return None{};
      }
      TRY(value, quote_string(as<std::string>(literal->value), '"'));
      break;
    }
    case Kind::number: {
      TRY(value, render_number(*literal));
      break;
    }
    case Kind::boolean: {
      if (x.op != BinaryOp::eq) {
        return None{};
      }
      value = as<bool>(literal->value) ? "true" : "false";
      break;
    }
  }
  return typed(subject, {kind, fmt::format("{} {} {}", converted(subject, kind),
                                           op, value)});
}

/// Spells `column in [literals]` as the union of the equalities with its
/// elements. Strings use KQL's case-sensitive `in`, which holds where `==`
/// holds for some element.
///
/// A list that mixes kinds is vetoed. The type guard of each kind keeps the
/// rows of every other KQL type, so the union of their sound prefilters keeps
/// every row. Applying each kind's condition to the rows of its KQL type only,
/// in contrast, would drop a `dynamic` string `443` that decodes to the number
/// in `x in ["a", 443]`.
auto render_in(In const& x) -> Option<std::string> {
  auto const* column = try_as<Column>(*x.expr);
  if (not column or x.list.empty()) {
    return None{};
  }
  TRY(auto subject, render_column(*column));
  auto strings = std::vector<std::string>{};
  auto numbers = std::vector<std::string>{};
  auto booleans = std::vector<std::string>{};
  for (auto const& element : x.list) {
    auto const* literal = try_as<Literal>(element);
    if (not literal) {
      return None{};
    }
    TRY(auto kind, kind_of(*literal));
    switch (kind) {
      case Kind::string: {
        TRY(auto value, quote_string(as<std::string>(literal->value), '"'));
        strings.push_back(std::move(value));
        break;
      }
      case Kind::number: {
        TRY(auto value, render_number(*literal));
        numbers.push_back(
          fmt::format("{} == {}", converted(subject, kind), value));
        break;
      }
      case Kind::boolean: {
        booleans.push_back(fmt::format("{} == {}", converted(subject, kind),
                                       as<bool>(literal->value)));
        break;
      }
    }
  }
  auto kinds = int{not strings.empty()} + int{not numbers.empty()}
               + int{not booleans.empty()};
  if (kinds != 1) {
    return None{};
  }
  if (not strings.empty()) {
    return typed(subject,
                 {Kind::string,
                  fmt::format("{} in ({})", converted(subject, Kind::string),
                              fmt::join(strings, ", "))});
  }
  if (not numbers.empty()) {
    return typed(subject,
                 {Kind::number, fmt::format("{}", fmt::join(numbers, " or "))});
  }
  return typed(subject,
               {Kind::boolean, fmt::format("{}", fmt::join(booleans, " or "))});
}

/// Spells `starts_with`, `ends_with`, and `contains` with KQL's case-sensitive
/// `startswith_cs`, `endswith_cs`, and `contains_cs`. Their matching is
/// ordinal: they neither normalize Unicode nor ignore characters, so a
/// byte-wise match of valid UTF-8 in TQL is a match in KQL. Both find the
/// empty needle in every string. Calls on calls, such as `fold_case`, are
/// vetoed.
auto render_call(Call const& x) -> Option<std::string> {
  auto op = std::string_view{};
  switch (x.op) {
    case Operation::starts_with:
      op = "startswith_cs";
      break;
    case Operation::ends_with:
      op = "endswith_cs";
      break;
    case Operation::contains:
      op = "contains_cs";
      break;
    default:
      return None{};
  }
  if (x.args.size() != 2) {
    return None{};
  }
  auto const* column = try_as<Column>(x.args[0]);
  auto const* literal = try_as<Literal>(x.args[1]);
  auto const* needle = literal ? try_as<std::string>(literal->value) : nullptr;
  if (not column or not needle) {
    return None{};
  }
  TRY(auto subject, render_column(*column));
  TRY(auto value, quote_string(*needle, '"'));
  return typed(
    subject,
    {Kind::string,
     fmt::format("{} {} {}", converted(subject, Kind::string), op, value)});
}

/// Matches ASCII needles without relying on KQL's Unicode case mapping.
/// Non-ASCII subjects pass even when KQL would not find the needle. Recursive
/// search also keeps JSON-looking arrays/objects: decoding can expose escaped
/// strings that are absent from their raw spelling. `typed` already keeps
/// non-string values and JSON-quoted strings.
auto render_string_match(StringMatch const& x) -> Option<std::string> {
  if (x.ignore_case and not std::ranges::all_of(x.needle, [](unsigned char c) {
        return c < 0x80;
      })) {
    return None{};
  }
  TRY(auto subject, render_column(x.column));
  TRY(auto needle, quote_string(x.needle, '"'));
  auto op = std::string_view{};
  switch (x.kind) {
    case StringMatchKind::equal:
      op = x.ignore_case ? "=~" : "==";
      break;
    case StringMatchKind::prefix:
      op = x.ignore_case ? "startswith" : "startswith_cs";
      break;
    case StringMatchKind::suffix:
      op = x.ignore_case ? "endswith" : "endswith_cs";
      break;
    case StringMatchKind::substring:
      op = x.ignore_case ? "contains" : "contains_cs";
      break;
  }
  auto condition = fmt::format("tostring({}) {} {}", subject, op, needle);
  if (x.ignore_case) {
    condition += fmt::format(" or tostring({}) matches regex @'[^\\x00-\\x7F]'",
                             subject);
  }
  if (x.recursive) {
    condition += fmt::format(
      " or tostring({}) matches regex @'^[ \\t\\r\\n]*[\\[{{]'", subject);
  }
  return typed(subject, {Kind::string, std::move(condition)});
}

/// Subnet bounds are already masked by translation, in the 128-bit address
/// space. This also handles IPv4 /0 as the mapped /96, not IPv6 /0.
///
/// KQL equates IPv4-compatible addresses in ::/96 with mapped addresses in
/// ::ffff:0:0/96. That normalization is not monotone across those regions:
/// moving a bound can exclude even a non-aliased address inside the interval.
/// Comparisons are monotone within mapped IPv4, and unchanged for native IPv6
/// intervals entirely above both alias regions. Conservatively veto every
/// other interval intersecting ::/80, rather than guarding only its subjects.
auto render_ip_between(Between const& x) -> Option<std::string> {
  auto const* lo = try_as<Literal>(*x.lo);
  auto const* hi = try_as<Literal>(*x.hi);
  auto const* lo_ip = lo ? try_as<IpValue>(lo->value) : nullptr;
  auto const* hi_ip = hi ? try_as<IpValue>(hi->value) : nullptr;
  if (not lo_ip or not hi_ip) {
    return None{};
  }
  auto both_v4 = lo_ip->address.is_v4() and hi_ip->address.is_v4();
  if (not both_v4
      and lo_ip->address <= ip::v4(std::numeric_limits<uint32_t>::max())) {
    return None{};
  }
  TRY(auto lower, render_ip_comparison(*x.expr, *lo, ">="));
  TRY(auto upper, render_ip_comparison(*x.expr, *hi, "<="));
  return fmt::format("({} and {})", lower, upper);
}

/// Spells `column != null`. KQL's `isnotnull` is `false` for a `null` of any
/// type, but strings are never `null` in KQL, and the sources may receive an
/// empty `guid` or `dynamic` value where KQL sees `null`. The predicate thus
/// drops a `null` only for the types whose `null` decodes to `null` alone.
auto render_not_null(IsNull const& x) -> Option<std::string> {
  if (not x.negated) {
    if (auto subject = parsed_ip_column(*x.expr)) {
      return fmt::format("isempty(parse_ipv6(tostring({})))", *subject);
    }
  }
  auto const* column = try_as<Column>(*x.expr);
  if (not x.negated or not column) {
    return None{};
  }
  TRY(auto subject, render_column(*column));
  return fmt::format("(isnotnull({}) or gettype({}) !in ({}))", subject,
                     subject, strictly_null_types);
}

/// Collects the referenced fields as dynamic columns, since the output of a
/// KQL query has no static schema.
class ColumnCollector final : public ast::visitor<ColumnCollector> {
public:
  template <class T>
  auto visit(T& x) -> void {
    enter(x);
  }

  auto visit(ast::expression& expr) -> void {
    if (auto path = ast::field_path::try_from(expr);
        path and not path->path().empty()) {
      auto segments = std::vector<std::string>{};
      for (auto const& segment : path->path()) {
        segments.push_back(segment.id.name);
      }
      columns.add(std::move(segments), {DynamicType{}, true});
      return;
    }
    enter(expr);
  }

  ColumnModel columns;
};

/// Returns the length of the line break at `i` in `text`, or zero if there is
/// none. Like Kusto's own lexer (`TextFacts.IsLineBreakStart`), KQL ends a line
/// at CR, LF, U+2028 LINE SEPARATOR, and U+2029 PARAGRAPH SEPARATOR, so each of
/// them ends a comment and is invalid in a single-line string literal.
auto line_break_length(std::string_view text, size_t i) -> size_t {
  if (text[i] == '\r' or text[i] == '\n') {
    return 1;
  }
  constexpr auto line_separator = std::string_view{"\xE2\x80\xA8"};
  constexpr auto paragraph_separator = std::string_view{"\xE2\x80\xA9"};
  auto rest = text.substr(i);
  if (rest.starts_with(line_separator)
      or rest.starts_with(paragraph_separator)) {
    return 3;
  }
  return 0;
}

/// Spells the `project` stage that keeps the top-level fields of `paths`, or
/// returns `None` if one of them has no spelling. A field that the query does
/// not return becomes `null`, where `project` would fail the query.
auto project_stage(ir::OptimizeProjection const& paths) -> Option<std::string> {
  auto names = std::vector<std::string>{};
  for (auto const& path : paths) {
    if (path.path().empty()) {
      return None{};
    }
    auto name = path.path().front().id.name;
    if (std::ranges::contains(names, name)) {
      continue;
    }
    names.push_back(std::move(name));
  }
  if (names.empty()) {
    return None{};
  }
  auto columns = std::vector<std::string>{};
  for (auto const& name : names) {
    TRY(auto quoted, quote_string(name, '\''));
    columns.push_back(
      fmt::format("[{}] = column_ifexists({}, dynamic(null))", quoted, quoted));
  }
  return fmt::format("\n| project {}", fmt::join(columns, ", "));
}

} // namespace

auto KqlRenderer::render(Expr const& expr) const -> Option<std::string> {
  return match(
    expr,
    [](Literal const& x) -> Option<std::string> {
      if (auto const* value = try_as<bool>(x.value)) {
        return *value ? "true" : "false";
      }
      return None{};
    },
    [](Binary const& x) -> Option<std::string> {
      return render_comparison(x);
    },
    [](In const& x) -> Option<std::string> {
      return render_in(x);
    },
    [](Call const& x) -> Option<std::string> {
      return render_call(x);
    },
    [](IsNull const& x) -> Option<std::string> {
      return render_not_null(x);
    },
    [](StringMatch const& x) -> Option<std::string> {
      return render_string_match(x);
    },
    [](Between const& x) -> Option<std::string> {
      return render_ip_between(x);
    },
    [&](And const& x) -> Option<std::string> {
      // Dropping a conjunct only widens a superset.
      auto parts = std::vector<std::string>{};
      for (auto const& operand : x.operands) {
        if (auto part = render(operand)) {
          parts.push_back(std::move(*part));
        }
      }
      if (parts.empty()) {
        return None{};
      }
      if (parts.size() == 1) {
        return std::move(parts.front());
      }
      return fmt::format("({})", fmt::join(parts, " and "));
    },
    [&](Or const& x) -> Option<std::string> {
      auto parts = std::vector<std::string>{};
      for (auto const& operand : x.operands) {
        TRY(auto part, render(operand));
        parts.push_back(std::move(part));
      }
      return fmt::format("({})", fmt::join(parts, " or "));
    },
    [](auto const&) -> Option<std::string> {
      // `Not` would turn a superset into a subset. Columns, arithmetic,
      // and `Conditional` never come from a dynamic prefilter.
      return None{};
    });
}

auto kql_accepts_trailing_stages(std::string_view query) -> bool {
  auto is_word_start = [](char c) {
    return (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z') or c == '_'
           or c == '$';
  };
  auto is_word = [&](char c) {
    return is_word_start(c) or (c >= '0' and c <= '9');
  };
  auto first = Option<char>{};
  auto last = Option<char>{};
  auto token = [&](char begin, char end) {
    if (not first) {
      first = begin;
    }
    last = end;
  };
  auto i = size_t{0};
  auto const n = query.size();
  while (i < n) {
    auto c = query[i];
    if (auto length = line_break_length(query, i); length > 0) {
      i += length;
      continue;
    }
    if (c == ' ' or c == '\t') {
      ++i;
      continue;
    }
    // Comments run to the end of the line.
    if (query.substr(i).starts_with("//")) {
      i += 2;
      while (i < n and line_break_length(query, i) == 0) {
        ++i;
      }
      continue;
    }
    // Multi-line string literals.
    if (query.substr(i).starts_with("```")
        or query.substr(i).starts_with("~~~")) {
      auto end = query.find(query.substr(i, 3), i + 3);
      if (end == std::string_view::npos) {
        return false;
      }
      token(c, c);
      i = end + 3;
      continue;
    }
    // String literals, optionally obfuscated (`h`) and verbatim (`@`).
    auto j = i;
    if ((c == 'h' or c == 'H') and j + 1 < n
        and (query[j + 1] == '\'' or query[j + 1] == '"'
             or query[j + 1] == '@')) {
      ++j;
    }
    auto verbatim = j < n and query[j] == '@';
    if (verbatim) {
      ++j;
    }
    if (j < n and (query[j] == '\'' or query[j] == '"')) {
      auto quote = query[j];
      auto k = j + 1;
      auto closed = false;
      while (k < n) {
        auto d = query[k];
        if (line_break_length(query, k) > 0) {
          return false;
        }
        if (not verbatim and d == '\\') {
          k += 2;
          continue;
        }
        if (d == quote) {
          if (verbatim and k + 1 < n and query[k + 1] == quote) {
            k += 2;
            continue;
          }
          closed = true;
          break;
        }
        ++k;
      }
      if (not closed) {
        return false;
      }
      token(c, quote);
      i = k + 1;
      continue;
    }
    if (verbatim) {
      // A lone `@` is not KQL that we know.
      return false;
    }
    if (is_word_start(c)) {
      auto k = i;
      while (k < n and is_word(query[k])) {
        ++k;
      }
      if (std::ranges::contains(opaque_words, query.substr(i, k - i))) {
        return false;
      }
      token(c, query[k - 1]);
      i = k;
      continue;
    }
    token(c, c);
    ++i;
  }
  if (not first or *first == '.') {
    return false;
  }
  return *last != ';' and *last != '|';
}

auto plan_kql_query(std::string_view query, ir::OptimizeFilter const& filter,
                    Option<ir::OptimizeProjection> const& projection,
                    Option<uint64_t> limit, Option<uint64_t> result_cap,
                    registry const& reg) -> KqlPlan {
  TENZIR_ASSERT(not result_cap
                or (*result_cap > 0 and *result_cap <= max_take));
  auto result = KqlPlan{.query = std::string{query}};
  if (not kql_accepts_trailing_stages(query)) {
    return result;
  }
  auto appendix = std::string{};
  // Filter first, so that the prefilters see every column of the query.
  auto collector = ColumnCollector{};
  for (auto expr : filter) {
    collector.visit(expr);
  }
  auto split = split_filter(filter, collector.columns, KqlRenderer{}, reg);
  auto where = std::string{};
  for (auto const& predicate : split.pushed) {
    auto addition = where.empty() ? fmt::format("\n| where {}", predicate)
                                  : fmt::format(" and {}", predicate);
    if (where.size() + addition.size() > max_appendix) {
      // Keep this prefilter local only and consider smaller later ones.
      continue;
    }
    where += addition;
  }
  if (not where.empty()) {
    // A service with a silent cap returns only the first rows of a large
    // result. A filter before that cap would reveal later rows, so the query
    // must reach the filter with the cap applied. `take` keeps the first rows
    // of a sorted result, as the cap does, and arbitrary rows of an unsorted
    // one, which the cap may return as well.
    if (result_cap) {
      appendix += fmt::format("\n| take {}", *result_cap);
    }
    appendix += where;
    result.filtered = true;
  }
  // Project after filtering, retaining every field that the local filter
  // reads, since all of it runs on the projected rows.
  auto retained = projection;
  for (auto const& expr : filter) {
    ir::add_refs_to_projection(retained, expr);
  }
  if (retained) {
    if (auto stage = project_stage(*retained);
        stage and appendix.size() + stage->size() <= max_appendix) {
      appendix += *stage;
      result.projected = true;
    }
  }
  // Every predicate stays local, so a limit is only safe without one. Larger
  // limits than a `long` can spell are enforced locally only.
  if (filter.empty() and limit and *limit > 0 and *limit <= max_take) {
    appendix += fmt::format("\n| take {}", *limit);
    result.limited = true;
  }
  result.query += appendix;
  return result;
}

} // namespace tenzir::pushdown
