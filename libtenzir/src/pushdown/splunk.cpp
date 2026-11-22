//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pushdown/splunk.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/ip.hpp"
#include "tenzir/subnet.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/try.hpp"
#include "tenzir/variant_traits.hpp"

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cmath>
#include <limits>
#include <string>
#include <utility>
#include <vector>

namespace tenzir::pushdown {

namespace {

using namespace std::literals;

/// The largest count that `head` and `sort` accept.
constexpr auto max_count
  = static_cast<uint64_t>(std::numeric_limits<int64_t>::max());

/// Whether `text` holds a control character, which a quoted SPL string or
/// field name should not contain.
auto has_control(std::string_view text) -> bool {
  return std::ranges::any_of(text, [](char c) {
    auto byte = static_cast<unsigned char>(c);
    return byte < 0x20 or byte == 0x7f;
  });
}

/// Quotes a string for `search`, `eval`, and `where`, escaping `"` and `\`
/// with a backslash. A `*` in a term of `search` is a wildcard.
auto quote_value(std::string_view text) -> Option<std::string> {
  if (has_control(text)) {
    return None{};
  }
  auto result = std::string{"\""};
  for (auto c : text) {
    if (c == '"' or c == '\\') {
      result += '\\';
    }
    result += c;
  }
  result += '"';
  return result;
}

/// Words that `search` reads as operators rather than as field names.
auto is_keyword(std::string_view name) -> bool {
  constexpr auto keywords
    = std::array<std::string_view, 6>{"and", "or", "not", "in", "term", "case"};
  return std::ranges::any_of(keywords, [&](std::string_view keyword) {
    return detail::ascii_icase_equal(name, keyword);
  });
}

/// Whether `search` reads `name=value` as a search modifier rather than as a
/// comparison of a field, such as `earliest=-1h`.
auto is_search_modifier(std::string_view name) -> bool {
  constexpr auto modifiers = std::array<std::string_view, 23>{
    "_index_earliest",
    "_index_latest",
    "daysago",
    "earliest",
    "endtime",
    "endtimeu",
    "eventtypetag",
    "hosttag",
    "hoursago",
    "latest",
    "minutesago",
    "monthsago",
    "savedsearch",
    "savedsplunk",
    "searchtimespandays",
    "searchtimespanhours",
    "searchtimespanminutes",
    "searchtimespanmonths",
    "sourcetypetag",
    "splunk_server_group",
    "starttime",
    "starttimeu",
    "timeformat",
  };
  return std::ranges::any_of(modifiers, [&](std::string_view modifier) {
    return detail::ascii_icase_equal(name, modifier);
  });
}

/// Whether `name` needs no quotes as a field name.
auto is_plain(std::string_view name) -> bool {
  return not name.empty()
         and (std::isalpha(static_cast<unsigned char>(name.front()))
              or name.front() == '_')
         and std::ranges::all_of(
           name,
           [](char c) {
             return std::isalnum(static_cast<unsigned char>(c)) or c == '_';
           })
         and not is_keyword(name);
}

/// Spells a field name for `search` and as an argument of a command, such as
/// `fields`, `stats`, or `sort`, quoting it in double quotes unless it is a
/// plain identifier. Names that a quoted field name cannot hold, and names
/// with wildcards, have no spelling.
auto quote_field(std::string_view name) -> Option<std::string> {
  if (name.empty() or has_control(name)
      or name.find_first_of("\"\\*") != std::string_view::npos) {
    return None{};
  }
  // Splunk treats fields whose names start with `__` as internal: live
  // probes found that `fields - __x` silently discards every result.
  if (name.starts_with("__")) {
    return None{};
  }
  if (is_plain(name)) {
    return std::string{name};
  }
  return fmt::format("\"{}\"", name);
}

/// Whether `eval` reads the field `name` when it is quoted in single quotes.
/// Live probes found that other characters fail the search, such as `|` and
/// `[`, or silently read another field, such as `\\` and `::`.
auto is_eval_name(std::string_view name) -> bool {
  if (name.empty() or name.front() == ' ' or name.back() == ' '
      or name.starts_with("__")) {
    return false;
  }
  return std::ranges::all_of(name, [](char c) {
    auto byte = static_cast<unsigned char>(c);
    return byte >= 0x80 or (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z')
           or (c >= '0' and c <= '9')
           or std::string_view{"_-.{}@/ "}.contains(c);
  });
}

/// Spells a number for `search`, `eval`, and `where`.
auto render_number(ast::constant::kind const& value) -> Option<std::string> {
  return match(
    value,
    [](int64_t x) -> Option<std::string> {
      return fmt::to_string(x);
    },
    [](uint64_t x) -> Option<std::string> {
      return fmt::to_string(x);
    },
    [](double x) -> Option<std::string> {
      if (not std::isfinite(x)) {
        return None{};
      }
      // `search` might read an exponent as text.
      auto text = fmt::to_string(x);
      if (text.find_first_of("eE") != std::string::npos) {
        return None{};
      }
      return text;
    },
    [](auto const&) -> Option<std::string> {
      return None{};
    });
}

/// Spells a time as epoch seconds, as Splunk holds `_time`, with as many
/// fractional digits as it needs.
auto epoch_seconds(time value) -> std::string {
  auto const ns = value.time_since_epoch().count();
  // The magnitude, without overflow for the smallest `int64_t`.
  auto const magnitude = ns < 0 ? uint64_t{0} - static_cast<uint64_t>(ns)
                                : static_cast<uint64_t>(ns);
  auto const sign = ns < 0 ? "-" : "";
  auto const seconds = magnitude / 1'000'000'000;
  auto const fraction = magnitude % 1'000'000'000;
  if (fraction == 0) {
    return fmt::format("{}{}", sign, seconds);
  }
  auto digits = fmt::format("{:09}", fraction);
  while (digits.back() == '0') {
    digits.pop_back();
  }
  return fmt::format("{}{}.{}", sign, seconds, digits);
}

/// Spells a top-level field for a term of `search`.
auto term_field(ast::expression const& expr) -> Option<std::string> {
  TRY(auto path, ir::field_segments(expr));
  if (path.size() != 1 or is_search_modifier(path.front())) {
    return None{};
  }
  return quote_field(path.front());
}

/// Returns the value of a constant, also of a negated number, which the
/// parser keeps as a negation.
auto constant_value(ast::expression const& expr)
  -> Option<ast::constant::kind> {
  if (auto const* constant = try_as<ast::constant>(expr)) {
    return constant->value;
  }
  auto const* x = try_as<ast::unary_expr>(expr);
  if (not x or x->op != ast::unary_op::neg) {
    return None{};
  }
  auto const* constant = try_as<ast::constant>(x->expr);
  if (not constant) {
    return None{};
  }
  return match(
    constant->value,
    [](int64_t value) -> Option<ast::constant::kind> {
      if (value == std::numeric_limits<int64_t>::min()) {
        return None{};
      }
      return ast::constant::kind{-value};
    },
    [](double value) -> Option<ast::constant::kind> {
      return ast::constant::kind{-value};
    },
    [](auto const&) -> Option<ast::constant::kind> {
      return None{};
    });
}

/// Spells a literal that `field=value` compares with: a number compares
/// numerically, and a string, a boolean, or an IP address as text, without
/// regard to case. A string with a `*` has no spelling, since `search` would
/// read it as a wildcard.
auto term_value(ast::constant::kind const& value) -> Option<std::string> {
  return match(
    value,
    [](std::string const& x) -> Option<std::string> {
      if (x.contains('*')) {
        return None{};
      }
      return quote_value(x);
    },
    [](bool x) -> Option<std::string> {
      return x ? "true" : "false";
    },
    [](ip const& x) -> Option<std::string> {
      return quote_value(fmt::to_string(x));
    },
    [&](auto const&) -> Option<std::string> {
      return render_number(value);
    });
}

/// The constants of the list `expr`, which the optimizer may fold into one
/// constant.
auto list_constants(ast::expression const& expr)
  -> Option<std::vector<ast::constant::kind>> {
  auto result = std::vector<ast::constant::kind>{};
  if (auto const* xs = try_as<ast::list>(expr)) {
    for (auto const& item : xs->items) {
      auto const* element = try_as<ast::expression>(item);
      auto const* constant
        = element ? try_as<ast::constant>(*element) : nullptr;
      if (not constant) {
        return None{};
      }
      result.push_back(constant->value);
    }
    return result;
  }
  auto const* constant = try_as<ast::constant>(expr);
  auto const* xs = constant ? try_as<tenzir::list>(constant->value) : nullptr;
  if (not xs) {
    return None{};
  }
  for (auto const& element : *xs) {
    result.push_back(
      ast::constant::make(located{element, location::unknown}).value);
  }
  return result;
}

/// Spells a wildcard pattern for a string relation, or returns `None` if the
/// needle has a `*`, which `search` would read as a wildcard.
auto term_pattern(std::string_view field, ast::expression const& needle,
                  std::string_view prefix, std::string_view suffix)
  -> Option<std::string> {
  auto const* constant = try_as<ast::constant>(needle);
  auto const* text = constant ? try_as<std::string>(constant->value) : nullptr;
  if (not text or text->contains('*')) {
    return None{};
  }
  TRY(auto value, quote_value(fmt::format("{}{}{}", prefix, *text, suffix)));
  return fmt::format("{}={}", field, value);
}

/// Spells a predicate as a term of the `search` command, or returns `None`.
///
/// - `==`, `<`, `<=`, `>`, and `>=` of a field with a number compare
///   numerically, and `==` with a string, a boolean, or an IP address
///   compares text without regard to case.
/// - Comparisons of `_time` with a time compare epoch seconds.
/// - `in` with a list becomes `IN`, and `in` with a subnet a CIDR match.
/// - `!= null` becomes `field=*`.
/// - `starts_with`, `ends_with`, and `"x" in field` become wildcards.
/// - `and` and `or` of terms combine them.
auto search_term(ast::expression const& expr) -> Option<std::string> {
  if (auto const* call = try_as<ast::function_call>(expr)) {
    auto const& ref = call->fn.ref;
    if (not ref.resolved() or ref.pkg() != entity_pkg_std
        or ref.segments().size() != 1 or call->args.size() != 2) {
      return None{};
    }
    auto const& name = ref.segments().front();
    TRY(auto field, term_field(call->args[0]));
    if (name == "starts_with") {
      return term_pattern(field, call->args[1], "", "*");
    }
    if (name == "ends_with") {
      return term_pattern(field, call->args[1], "*", "");
    }
    return None{};
  }
  auto const* x = try_as<ast::binary_expr>(expr);
  if (not x) {
    return None{};
  }
  using enum ast::binary_op;
  switch (x->op) {
    case and_:
    case or_: {
      TRY(auto left, search_term(x->left));
      TRY(auto right, search_term(x->right));
      return fmt::format("({} {} {})", left, x->op == and_ ? "AND" : "OR",
                         right);
    }
    case in: {
      if (auto const* constant = try_as<ast::constant>(x->left);
          constant and is<std::string>(constant->value)) {
        TRY(auto field, term_field(x->right));
        return term_pattern(field, x->left, "*", "*");
      }
      TRY(auto field, term_field(x->left));
      if (auto const* constant = try_as<ast::constant>(x->right)) {
        if (auto const* network = try_as<subnet>(constant->value)) {
          TRY(auto value, quote_value(fmt::to_string(*network)));
          return fmt::format("{}={}", field, value);
        }
      }
      TRY(auto constants, list_constants(x->right));
      if (constants.empty()) {
        return None{};
      }
      auto values = std::vector<std::string>{};
      for (auto const& constant : constants) {
        TRY(auto value, term_value(constant));
        values.push_back(std::move(value));
      }
      return fmt::format("{} IN ({})", field, fmt::join(values, ", "));
    }
    case eq:
    case neq:
    case lt:
    case leq:
    case gt:
    case geq: {
      auto op = x->op;
      auto const* field_expr = &x->left;
      auto constant = constant_value(x->right);
      if (not constant) {
        constant = constant_value(x->left);
        field_expr = &x->right;
        op = op == lt    ? gt
             : op == leq ? geq
             : op == gt  ? lt
             : op == geq ? leq
                         : op;
      }
      if (not constant) {
        return None{};
      }
      TRY(auto field, term_field(*field_expr));
      if (is<caf::none_t>(*constant)) {
        if (op != neq) {
          return None{};
        }
        return fmt::format("{}=*", field);
      }
      if (op == neq) {
        return None{};
      }
      auto symbol = op == eq    ? "="
                    : op == lt  ? "<"
                    : op == leq ? "<="
                    : op == gt  ? ">"
                                : ">=";
      if (auto const* instant = try_as<time>(*constant)) {
        if (field != "_time") {
          return None{};
        }
        return fmt::format("_time{}{}", symbol, epoch_seconds(*instant));
      }
      if (op == eq) {
        TRY(auto value, term_value(*constant));
        return fmt::format("{}={}", field, value);
      }
      TRY(auto value, render_number(*constant));
      return fmt::format("{}{}{}", field, symbol, value);
    }
    default:
      return None{};
  }
}

/// Commands whose effects a `head` stage could cut short.
auto has_side_effects(std::string_view command) -> bool {
  constexpr auto commands = std::array<std::string_view, 14>{
    "collect",       "delete",    "dbxoutput",    "map",        "mcollect",
    "meventcollect", "outputcsv", "outputlookup", "outputtext", "run",
    "script",        "sendalert", "sendemail",    "tscollect",
  };
  return std::ranges::any_of(commands, [&](std::string_view x) {
    return detail::ascii_icase_equal(command, x);
  });
}

/// The stages of SPL, or with `spl2`, of SPL2, which compiles them to the same
/// SPL commands. Expressions spell for `where`, whose comparisons are exact and
/// case-sensitive, and whose fields are single-quoted.
class SplQueryDialect final : public PipeQueryDialect {
public:
  explicit SplQueryDialect(bool spl2) : spl2_{spl2} {
  }

  auto render_null() const -> Option<std::string> override {
    return spl2_ ? "null" : "null()";
  }

  /// Splunk returns every value as a string, so a boolean is the text that
  /// `from_splunk` decodes as one.
  auto render_bool(bool value) const -> std::string override {
    return value ? R"("true")" : R"("false")";
  }

  auto render_int(int64_t value) const -> Option<std::string> override {
    return fmt::to_string(value);
  }

  auto render_uint(uint64_t value) const -> Option<std::string> override {
    return fmt::to_string(value);
  }

  auto render_double(double value) const -> Option<std::string> override {
    return fmt::to_string(value);
  }

  auto quote_string(std::string_view text) const
    -> Option<std::string> override {
    return quote_value(text);
  }

  /// `eval` holds `_time` as epoch seconds.
  auto render_time(time value) const -> Option<std::string> override {
    return epoch_seconds(value);
  }

  auto render_ip(ip value) const -> Option<std::string> override {
    return quote_value(fmt::to_string(value));
  }

  auto binary_operator(ast::binary_op op) const
    -> Option<std::string_view> override {
    using enum ast::binary_op;
    switch (op) {
      case eq:
        return "==";
      case neq:
        return "!=";
      case lt:
        return "<";
      case leq:
        return "<=";
      case gt:
        return ">";
      case geq:
        return ">=";
      case add:
        return "+";
      case sub:
        return "-";
      case mul:
        return "*";
      case div:
        return "/";
      case and_:
        return "AND";
      case or_:
        return "OR";
      case in:
      case if_:
      case else_:
        return None{};
    }
    TENZIR_UNREACHABLE();
  }

  auto render_not(std::string_view operand) const
    -> Option<std::string> override {
    return fmt::format("(NOT {})", operand);
  }

  auto render_negate(std::string_view operand) const
    -> Option<std::string> override {
    return fmt::format("(-{})", operand);
  }

  auto render_null_check(std::string_view operand, bool negated) const
    -> Option<std::string> override {
    return fmt::format("{}({})", negated ? "isnotnull" : "isnull", operand);
  }

  auto render_in(std::string_view operand,
                 std::span<std::string const> elements) const
    -> Option<std::string> override {
    return fmt::format("in({}, {})", operand, fmt::join(elements, ", "));
  }

  /// Unlike `substr(x, -len(y))`, the spelling of `ends_with` also holds for
  /// an empty `y`. Time parts and buckets use Splunk's `strftime` in its own
  /// time zone.
  auto functions() const -> std::span<Function const> override {
    static constexpr auto rows = std::array{
      Function{"starts_with", 2, "(substr({0}, 1, len({1}))=={1})",
               Decode::native, true},
      Function{"ends_with", 2, "(substr({0}, len({0})-len({1})+1)=={1})",
               Decode::native, true},
      Function{"match_regex", 2, "match({0}, {1})", Decode::native, true},
      Function{"to_lower", 1, "lower({0})"},
      Function{"to_upper", 1, "upper({0})"},
      Function{"capitalize", 1,
               "(upper(substr({0}, 1, 1)) + lower(substr({0}, 2)))"},
      Function{"trim", 1, "trim({0})"},
      Function{"trim_start", 1, "ltrim({0})"},
      Function{"trim_end", 1, "rtrim({0})"},
      Function{"replace", 3, "replace({0}, {1}, {2})"},
      Function{"is_empty", 1, "(len({0})==0)", Decode::native, true},
      // Negative bounds of `slice` stay in the pipeline.
      Function{"slice", 1, "substr({0}, {begin:nat} + 1)"},
      Function{"slice", 1, "substr({0}, 1, {end:nat})"},
      Function{"slice", 1,
               "if({end:nat} > {begin:nat}, substr({0}, {begin:nat} + 1, "
               "{end:nat} - {begin:nat}), \"\")"},
      Function{"length_chars", 1, "len({0})"},
      Function{"hash_md5", 1, "md5({0})"},
      Function{"hash_sha1", 1, "sha1({0})"},
      Function{"hash_sha256", 1, "sha256({0})"},
      Function{"abs", 1, "abs({0})"},
      Function{"ceil", 1, "ceiling({0})"},
      Function{"floor", 1, "floor({0})"},
      Function{"round", 1, "round({0})"},
      Function{"sqrt", 1, "sqrt({0})"},
      Function{"exp", 1, "exp({0})"},
      Function{"log", 1, "ln({0})"},
      Function{"log", 2, "log({0}, {1})"},
      Function{"year", 1, "tonumber(strftime({0}, \"%Y\"))", Decode::number},
      Function{"month", 1, "tonumber(strftime({0}, \"%m\"))", Decode::number},
      Function{"day", 1, "tonumber(strftime({0}, \"%d\"))", Decode::number},
      Function{"hour", 1, "tonumber(strftime({0}, \"%H\"))", Decode::number},
      Function{"minute", 1, "tonumber(strftime({0}, \"%M\"))", Decode::number},
      Function{"second", 1, "tonumber(strftime({0}, \"%S.%6N\"))",
               Decode::number},
      Function{"floor", 2,
               "strftime(floor({0} / {1:s}) * {1:s}, \"%Y-%m-%dT%H:%M:%S%z\")",
               Decode::time},
    };
    static_assert(valid_rows(rows));
    return rows;
  }

  /// `replace` takes a PCRE pattern, so a constant literal needle is
  /// regex-escaped. Non-literal needles stay in Tenzir.
  auto render_call(ast::function_call const& call,
                   ExpressionRenderer const& renderer) const
    -> Option<std::string> override {
    if (call.fn.ref.segments().front() == "replace" and call.args.size() == 3
        and not std::ranges::any_of(call.args, [](ast::expression const& arg) {
              return is<ast::assignment>(arg);
            })) {
      auto const* constant = try_as<ast::constant>(call.args[1]);
      auto const* needle
        = constant ? try_as<std::string>(constant->value) : nullptr;
      if (not needle) {
        return None{};
      }
      TRY(auto subject, renderer.value(call.args[0]));
      TRY(auto pattern, quote_string(escape_regex(*needle)));
      TRY(auto replacement, renderer.value(call.args[2]));
      return fmt::format("replace({}, {}, {})", subject, pattern, replacement);
    }
    return PipeQueryDialect::render_call(call, renderer);
  }

  /// Splunk returns every value as a string, so counts and sums decode as
  /// numbers. `count(eval(...))` counts the events for which the predicate
  /// holds, and `dc` counts exactly, unlike `estdc`. SPL2 calls `count` with
  /// parentheses and has no `eval` in `stats`.
  auto aggregations() const -> std::span<Aggregation const> override {
    static constexpr auto spl2_rows = std::array{
      Aggregation{"count", 0, "count()", Decode::integer},
      Aggregation{"count", 1, "count({0})", Decode::integer},
      Aggregation{"min", 1, "min({0})"},
      Aggregation{"max", 1, "max({0})"},
      Aggregation{"count_if", 2, "count(if({1:pred}, 1, null))",
                  Decode::integer},
      Aggregation{"count_distinct", 1, "dc({0})", Decode::integer},
      Aggregation{"sum", 1, "sum({0})", Decode::number},
      Aggregation{"mean", 1, "avg({0})", Decode::number},
      Aggregation{"stddev", 1, "stdevp({0})", Decode::number},
      Aggregation{"variance", 1, "varp({0})", Decode::number},
      Aggregation{"all", 1,
                  "min(case({0:expr}==\"true\", 1, {0:expr}==\"false\", 0))",
                  Decode::boolean},
      Aggregation{"any", 1,
                  "max(case({0:expr}==\"true\", 1, {0:expr}==\"false\", 0))",
                  Decode::boolean},
    };
    static_assert(valid_rows(spl2_rows));
    if (spl2_) {
      return spl2_rows;
    }
    static constexpr auto rows = std::array{
      Aggregation{"count", 0, "count", Decode::integer},
      Aggregation{"count", 1, "count({0})", Decode::integer},
      Aggregation{"min", 1, "min({0})"},
      Aggregation{"max", 1, "max({0})"},
      Aggregation{"count_if", 2, "count(eval({1:pred}))", Decode::integer},
      Aggregation{"count_distinct", 1, "dc({0})", Decode::integer},
      Aggregation{"sum", 1, "sum({0})", Decode::number},
      Aggregation{"mean", 1, "avg({0})", Decode::number},
      Aggregation{"stddev", 1, "stdevp({0})", Decode::number},
      Aggregation{"variance", 1, "varp({0})", Decode::number},
      Aggregation{"all", 1,
                  "min(eval(case({0:expr}==\"true\", 1, {0:expr}==\"false\", "
                  "0)))",
                  Decode::boolean},
      Aggregation{"any", 1,
                  "max(eval(case({0:expr}==\"true\", 1, {0:expr}==\"false\", "
                  "0)))",
                  Decode::boolean},
    };
    static_assert(valid_rows(rows));
    return rows;
  }

  /// Fields hold strings, never booleans, and `where` needs a predicate.
  auto boolean_values() const -> bool override {
    return false;
  }

  auto render_predicate_value(std::string_view predicate) const
    -> Option<std::string> override {
    return fmt::format(R"(if({}, "true", "false"))", predicate);
  }

  auto bind_column(std::string_view column) const
    -> Option<std::string> override {
    if (not is_eval_name(column)) {
      return None{};
    }
    return fmt::format("'{}'", column);
  }

  /// SPL2 names fields in commands as in expressions.
  auto name_column(std::string_view column) const
    -> Option<std::string> override {
    return spl2_ ? bind_column(column) : quote_field(column);
  }

  /// Splunk leaves fields whose names start with `__` out of its results.
  auto generated_name(std::string_view kind, size_t index) const
    -> std::string override {
    return fmt::format("tenzir_{}_{}", kind, index);
  }

  auto base_term(ast::expression const& conjunct) const
    -> Option<std::string> override {
    return search_term(conjunct);
  }

  auto filter_stage(std::string_view predicate) const
    -> Option<std::string> override {
    return fmt::format("\n| where {}", predicate);
  }

  auto select_stage(std::span<PipeAlias const> aliases,
                    std::span<std::string const> columns) const
    -> Option<std::string> override {
    TRY(auto result, assign(aliases));
    TRY(auto names, name_columns(columns));
    return fmt::format("{}\n| fields {}", result, fmt::join(names, ", "));
  }

  auto drop_stage(std::span<std::string const> columns) const
    -> Option<std::string> override {
    TRY(auto names, name_columns(columns));
    return fmt::format("\n| fields - {}", fmt::join(names, ", "));
  }

  /// `stats` names group keys after their fields, and renders a key that is
  /// `null` in a result by dropping the result.
  auto aggregate_stage(std::span<PipeAlias const> aliases,
                       std::span<PipeFunction const> functions,
                       std::span<std::string const> keys) const
    -> Option<std::string> override {
    TRY(auto result, assign(aliases));
    auto calls = std::vector<std::string>{};
    for (auto const& function : functions) {
      calls.push_back(fmt::format("{} as {}", function.call, function.name));
    }
    result += fmt::format("\n| stats {}", fmt::join(calls, ", "));
    if (not keys.empty()) {
      TRY(auto names, name_columns(keys));
      result += fmt::format(" by {}", fmt::join(names, ", "));
    }
    return result;
  }

  /// `sort` keeps 10,000 results unless it has a count, where `0` keeps all.
  auto
  sort_stage(std::span<PipeSortKey const> keys, Option<uint64_t> bound) const
    -> Option<std::string> override {
    if (bound and (*bound == 0 or *bound > max_count)) {
      return None{};
    }
    auto names = std::vector<std::string>{};
    for (auto const& key : keys) {
      TRY(auto name, name_column(key.column));
      names.push_back(fmt::format("{}{}", key.descending ? "-" : "", name));
    }
    return fmt::format("\n| sort {} {}", bound ? *bound : 0,
                       fmt::join(names, ", "));
  }

  auto limit_stage(uint64_t count) const -> Option<std::string> override {
    // `head 0` ends the search without running the stages after it, such as
    // the sentinel.
    if (count == 0 or count > max_count) {
      return None{};
    }
    return fmt::format("\n| head {}", count);
  }

private:
  /// Spells an `eval` stage that assigns `aliases`, if any.
  auto assign(std::span<PipeAlias const> aliases) const -> Option<std::string> {
    if (aliases.empty()) {
      return std::string{};
    }
    auto assignments = std::vector<std::string>{};
    for (auto const& alias : aliases) {
      TRY(auto value, alias_value(alias));
      assignments.push_back(fmt::format("{}={}", alias.name, value));
    }
    return fmt::format("\n| eval {}", fmt::join(assignments, ", "));
  }

  auto name_columns(std::span<std::string const> columns) const
    -> Option<std::vector<std::string>> {
    auto result = std::vector<std::string>{};
    for (auto const& column : columns) {
      TRY(auto name, name_column(column));
      result.push_back(std::move(name));
    }
    return result;
  }

  bool spl2_;
};

/// Spells the `fields` stage that keeps the top-level fields of `projection`,
/// or every field without a projection or a spelling of one of its fields.
/// SPL2 returns every field without one, and has no `fields *`.
auto fields_stage(Option<ir::OptimizeProjection> const& projection, bool spl2)
  -> std::string {
  auto const dialect = SplQueryDialect{spl2};
  auto const all = std::string{spl2 ? "" : "\n| fields *"};
  if (not projection) {
    return std::string{all};
  }
  auto names = std::vector<std::string>{};
  for (auto const& path : *projection) {
    if (path.path().empty()) {
      return std::string{all};
    }
    auto name = std::string{path.path().front().id.name};
    if (std::ranges::contains(names, name)) {
      continue;
    }
    names.push_back(std::move(name));
  }
  auto spelled = std::vector<std::string>{};
  for (auto const& name : names) {
    auto field = dialect.name_column(name);
    if (not field) {
      return std::string{all};
    }
    spelled.push_back(std::move(*field));
  }
  if (spelled.empty()) {
    return std::string{all};
  }
  return fmt::format("\n| fields {}", fmt::join(spelled, ", "));
}

/// The position after the whitespace and comments that `search` starts with.
auto skip_space_and_comments(std::string_view search) -> size_t {
  auto i = size_t{0};
  while (i < search.size()) {
    if (std::isspace(static_cast<unsigned char>(search[i]))) {
      ++i;
      continue;
    }
    if (search.substr(i).starts_with("```")) {
      auto end = search.find("```", i + 3);
      if (end == std::string_view::npos) {
        return search.size();
      }
      i = end + 3;
      continue;
    }
    break;
  }
  return i;
}

} // namespace

auto spl_search_command(std::string_view search) -> std::string {
  auto rest = search.substr(skip_space_and_comments(search));
  if (rest.empty() or rest.starts_with('|')) {
    return std::string{search};
  }
  constexpr auto command = std::string_view{"search"};
  if (rest.size() >= command.size()
      and detail::ascii_icase_equal(rest.substr(0, command.size()), command)
      and (rest.size() == command.size()
           or std::isspace(static_cast<unsigned char>(rest[command.size()])))) {
    return std::string{search};
  }
  return fmt::format("search {}", search);
}

auto spl_stages(std::string_view search, bool spl2) -> SplStages {
  auto result = SplStages{};
  auto side_effects = false;
  auto macro = false;
  auto formatted = false;
  // Whether a command follows the first one, outside of subsearches.
  auto piped = false;
  auto depth = size_t{0};
  // The last character outside of comments that is not whitespace.
  auto last = '\0';
  // Whether the next word names a command.
  auto at_command = false;
  auto i = size_t{0};
  auto is_space = [](char c) {
    return std::isspace(static_cast<unsigned char>(c)) != 0;
  };
  auto is_word = [](char c) {
    return std::isalnum(static_cast<unsigned char>(c)) != 0 or c == '_';
  };
  while (i < search.size()) {
    auto c = search[i];
    // SPL comments with three backticks, SPL2 with `/* */`, or with `//` up
    // to the end of the line.
    auto [open, close] = not spl2 ? std::pair{"```"sv, "```"sv}
                         : search.substr(i).starts_with("//")
                           ? std::pair{"//"sv, "\n"sv}
                           : std::pair{"/*"sv, "*/"sv};
    if (search.substr(i).starts_with(open)) {
      auto end = search.find(close, i + open.size());
      if (end == std::string_view::npos and close != "\n") {
        return result;
      }
      i = end == std::string_view::npos ? search.size() : end + close.size();
      continue;
    }
    if (is_space(c)) {
      ++i;
      continue;
    }
    if (at_command) {
      auto end = i;
      while (end < search.size() and is_word(search[end])) {
        ++end;
      }
      auto command = search.substr(i, end - i);
      side_effects = side_effects or has_side_effects(command);
      formatted
        = formatted or detail::ascii_icase_equal(command, "fieldformat");
      at_command = false;
    }
    last = c;
    // SPL2 has raw strings, which double a quote to hold it.
    auto raw = spl2 and search.substr(i).starts_with("@\"");
    switch (raw ? '"' : c) {
      case '"':
      case '\'':
      case '`': {
        // Strings, and in SPL2 also field names, escape with a backslash.
        // Other quotes, such as of SPL macros, end at the next equal one.
        auto quote = raw ? '"' : c;
        auto escapes = not raw and (c == '"' or (spl2 and c == '\''));
        i += raw ? 2 : 1;
        while (i < search.size()
               and (search[i] != quote
                    or (raw and search.substr(i).starts_with("\"\"")))) {
          i += (escapes and search[i] == '\\') or search[i] == quote ? 2 : 1;
        }
        if (i >= search.size()) {
          return result;
        }
        macro = macro or (c == '`' and not spl2);
        ++i;
        continue;
      }
      case ';':
        // SPL2 ends a statement with `;`, after which no stage may follow.
        if (spl2) {
          return result;
        }
        break;
      case '[':
        ++depth;
        break;
      case ']':
        if (depth == 0) {
          return result;
        }
        --depth;
        break;
      case '|':
        at_command = true;
        piped = piped or depth == 0;
        break;
      default:
        break;
    }
    ++i;
  }
  if (depth != 0 or last == '\0' or last == '|') {
    return result;
  }
  result.accepted = true;
  result.limitable = not side_effects and not macro;
  result.formatted = formatted or macro;
  // In SPL2, also the clauses of `from` may redefine a field, as
  // `from main select x as _time` does.
  result.plain = not spl2 and not piped and not macro;
  return result;
}

auto spl_index_search(std::span<std::string const> indexes)
  -> Option<std::string> {
  TENZIR_ASSERT(not indexes.empty());
  auto values = std::vector<std::string>{};
  for (auto const& index : indexes) {
    if (index.empty()) {
      return None{};
    }
    TRY(auto value, quote_value(index));
    values.push_back(std::move(value));
  }
  if (values.size() == 1) {
    return fmt::format("search index={}", values.front());
  }
  return fmt::format("search index IN ({})", fmt::join(values, ", "));
}

auto spl_planner(SplSource source, Option<std::string> gate,
                 Option<ir::OptimizeProjection> projection)
  -> Planner<SplQuery> {
  // A user's search must accept stages after it and must not format its
  // values, and a limit must not cut its side effects short. The gate of the
  // caller comes first. Searches for indexes extend their base search.
  auto base = std::string{};
  auto guard = SplStages{.accepted = true, .limitable = true, .plain = true};
  if (source.search) {
    base = source.spl2 ? *source.search : spl_search_command(*source.search);
    guard = spl_stages(base, source.spl2);
    if (not guard.accepted) {
      gate = gate.value_or("the search does not end where stages may follow");
    } else if (guard.formatted) {
      gate = gate.value_or("the search may format its values with "
                           "`fieldformat` or a macro");
    }
  } else if (auto search = spl_index_search(source.indexes);
             search and not source.indexes.empty()) {
    base = std::move(*search);
  } else {
    gate = gate.value_or("an index name has no spelling in SPL");
  }
  return {
    .target = "Splunk",
    .plan = [user = source.search.has_value(), spl2 = source.spl2,
             base = std::move(base), guard, gate = std::move(gate),
             projection = std::move(projection)](
              std::span<ir::QueryStage const> stages,
              std::monostate const*) -> Result<SplQuery, std::string> {
      if (gate and not stages.empty()) {
        return Err{*gate};
      }
      if (not guard.limitable
          and std::ranges::any_of(stages, [](auto const& x) {
                return is<ir::QueryStage::Limit>(x.kind);
              })) {
        return Err{std::string{"a `head` stage could cut the side effects of "
                               "the search short"}};
      }
      if (stages.empty() and not guard.accepted) {
        // Nothing may follow the search, which then goes out as it is.
        return SplQuery{
          .search = base,
          .generated = {},
          .layout = {},
          .sentinel = None{},
          .window = {},
        };
      }
      // Only Splunk's own `_time` holds epoch seconds, which a time literal
      // compares with. A stage may make the field `_time` hold another field,
      // as `select _time=event_time` does, after which a time literal has no
      // spelling. So may any command of a user's search after the first one,
      // as `| rename event* AS _*` does, so only a plain search keeps
      // Splunk's own.
      auto const dialect = SplQueryDialect{spl2};
      auto query = render_pipe(stages, dialect,
                               PipeOptions{
                                 .extend_base = not user,
                                 .time_column = std::string{"_time"},
                                 .native_time = guard.plain,
                                 .narrow = not user,
                               });
      if (not query) {
        return Err{std::string{"Splunk cannot spell the stage"}};
      }
      auto generated = std::string{};
      for (auto const& term : query->base_terms) {
        generated += ' ';
        generated += term;
      }
      generated += query->stages;
      if (not query->layout.fields) {
        generated += fields_stage(projection, spl2);
      }
      auto sentinel = Option<std::string>{};
      if (query->aggregates) {
        sentinel = fresh_name(*query, dialect, "groups");
        generated += fmt::format("\n| appendpipe [stats count{} as {}]",
                                 spl2 ? "()" : "", *sentinel);
      }
      auto search = base + generated;
      return SplQuery{
        .search = search,
        .generated = user ? std::move(generated) : std::move(search),
        .layout = std::move(query->layout),
        .sentinel = std::move(sentinel),
        .window = query->window,
      };
    },
  };
}

auto parse_spl_count(std::string_view text) -> Option<int64_t> {
  auto result = int64_t{};
  if (text.empty() or text.front() == '-') {
    return None{};
  }
  auto [end, error] = std::from_chars(text.begin(), text.end(), result);
  if (error != std::errc{} or end != text.end()) {
    return None{};
  }
  return result;
}

} // namespace tenzir::pushdown
