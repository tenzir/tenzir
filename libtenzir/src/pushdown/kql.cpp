//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pushdown/kql.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/ip.hpp"
#include "tenzir/pushdown/expr_render.hpp"
#include "tenzir/pushdown/pipe_plan.hpp"
#include "tenzir/subnet.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/try.hpp"
#include "tenzir/unicode.hpp"
#include "tenzir/variant_traits.hpp"

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <limits>
#include <utility>
#include <vector>

namespace tenzir::pushdown {

namespace {

/// The largest count that `take` accepts: KQL integer literals are `long`.
constexpr auto max_take
  = static_cast<uint64_t>(std::numeric_limits<int64_t>::max());

/// The duration of a KQL tick, the resolution of `datetime` and `timespan`.
constexpr auto tick = std::chrono::nanoseconds{100};

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

/// The words that start a statement that returns no table.
constexpr auto declaration_words = std::array{
  std::string_view{"let"},
  std::string_view{"declare"},
  std::string_view{"alias"},
};

/// Quotes `text` as a KQL string literal delimited by `quote`, or returns
/// `None` for invalid UTF-8, which the JSON request body cannot carry. KQL
/// strings support the backslash escapes `\\`, `\"`, `\'`, `\t`, `\n`, `\r`,
/// and `\uXXXX`, which spell the delimiter, the backslash, and control
/// characters. U+0085, U+2028, and U+2029 are escaped, too: Kusto's lexer, like
/// `kql_accepts_trailing_stages`, may read the latter two as line breaks, which
/// a single-line literal must not contain.
auto quote(std::string_view text, char delimiter) -> Option<std::string> {
  if (not unicode::is_valid_utf8(text)) {
    return None{};
  }
  constexpr auto separators = std::array{
    std::pair{std::string_view{"\xC2\x85"}, std::string_view{"\\u0085"}},
    std::pair{std::string_view{"\xE2\x80\xA8"}, std::string_view{"\\u2028"}},
    std::pair{std::string_view{"\xE2\x80\xA9"}, std::string_view{"\\u2029"}},
  };
  auto result = std::string{delimiter};
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
    if (c == '\\' or c == delimiter) {
      result += '\\';
    }
    result += c;
  }
  result += delimiter;
  return result;
}

/// Spells a column name as `['name']`, which KQL reads as a name in
/// expressions and as an argument of commands alike, or returns `None` for a
/// name that has none. `project-away` reads a `*` in a name as a wildcard, so
/// that no command names such a column safely.
auto quote_name(std::string_view name) -> Option<std::string> {
  if (name.empty() or name.contains('*')) {
    return None{};
  }
  TRY(auto quoted, quote(name, '\''));
  return fmt::format("[{}]", quoted);
}

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

/// Returns the constant that `expr` is, if any.
auto constant_of(ast::expression const& expr) -> ast::constant const* {
  return try_as<ast::constant>(expr);
}

template <class T>
auto holds(ast::expression const& expr) -> bool {
  auto const* constant = constant_of(expr);
  return constant and is<T>(constant->value);
}

/// Returns the constants of the list `expr`, which the optimizer may fold
/// into one constant, or `None` if `expr` is no list of constants.
auto list_constants(ast::expression const& expr)
  -> Option<std::vector<ast::constant::kind>> {
  auto result = std::vector<ast::constant::kind>{};
  if (auto const* xs = try_as<ast::list>(expr)) {
    for (auto const& item : xs->items) {
      auto const* element = try_as<ast::expression>(item);
      auto const* constant = element ? constant_of(*element) : nullptr;
      if (not constant) {
        return None{};
      }
      result.push_back(constant->value);
    }
    return result;
  }
  auto const* constant = constant_of(expr);
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

/// Whether `expr` is a list, of constants or not.
auto is_list(ast::expression const& expr) -> bool {
  return is<ast::list>(expr) or holds<tenzir::list>(expr);
}

/// Spells an IP address as the string that KQL holds it as.
auto ip_text(ip const& address) -> std::string {
  // The canonical spelling has no characters that need escaping.
  return fmt::format("\"{}\"", address);
}

/// The stages of KQL. Columns spell as `['name']`, both in expressions and as
/// arguments of commands.
class KqlDialect final : public PipeQueryDialect {
public:
  /// KQL has no `null` literal that compares with every type.
  auto render_null() const -> Option<std::string> override {
    return None{};
  }

  auto render_bool(bool value) const -> std::string override {
    return value ? "true" : "false";
  }

  /// The literal `-9223372036854775808` overflows, since KQL negates the
  /// positive literal.
  auto render_int(int64_t value) const -> Option<std::string> override {
    if (value == std::numeric_limits<int64_t>::min()) {
      return fmt::format("long({})", value);
    }
    return fmt::to_string(value);
  }

  /// KQL has no unsigned integers, and `long` literals end at 2^63 - 1.
  auto render_uint(uint64_t value) const -> Option<std::string> override {
    if (value > max_take) {
      return None{};
    }
    return fmt::to_string(value);
  }

  /// A `double` keeps a `real` spelling, such as `2.0`, so that it does not
  /// read as a `long`, which divides as an integer.
  auto render_double(double value) const -> std::string override {
    auto text = fmt::to_string(value);
    if (text.find_first_of(".e") == std::string::npos) {
      text += ".0";
    }
    return text;
  }

  auto quote_string(std::string_view text) const
    -> Option<std::string> override {
    return quote(text, '"');
  }

  /// KQL holds times in ticks of 100 nanoseconds, so a time in between has no
  /// spelling.
  auto render_time(time value) const -> Option<std::string> override {
    if (value.time_since_epoch() % tick != duration::zero()) {
      return None{};
    }
    // Split into days and the time of the day without converting the start
    // of the day to nanoseconds, which overflows for the earliest times.
    constexpr auto per_day
      = std::chrono::duration_cast<duration>(std::chrono::days{1}).count();
    auto const ticks = value.time_since_epoch().count();
    auto days = ticks / per_day;
    auto of_day = ticks % per_day;
    if (of_day < 0) {
      days -= 1;
      of_day += per_day;
    }
    auto const date = std::chrono::year_month_day{
      std::chrono::sys_days{std::chrono::days{days}}};
    auto const clock = std::chrono::hh_mm_ss{std::chrono::nanoseconds{of_day}};
    return fmt::format("datetime({:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:07}Z)",
                       static_cast<int>(date.year()),
                       static_cast<unsigned>(date.month()),
                       static_cast<unsigned>(date.day()), clock.hours().count(),
                       clock.minutes().count(), clock.seconds().count(),
                       clock.subseconds() / tick);
  }

  auto render_duration(duration value) const -> Option<std::string> override {
    if (value % tick != duration::zero()) {
      return None{};
    }
    return fmt::format("timespan({}tick)", value / tick);
  }

  /// KQL has no types for IP addresses and subnets, but holds them as
  /// strings. An address therefore compares as text, and only in `==`, `!=`,
  /// and `in`, while membership in a subnet uses `ipv4_is_in_range` or
  /// `ipv6_is_in_range`. `"x" in field` tests for a substring, and strings
  /// have no order.
  auto render_binary(ast::binary_expr const& x,
                     ExpressionRenderer const& renderer) const
    -> Option<Option<std::string>> override {
    using enum ast::binary_op;
    auto const unchanged = Option<std::string>{None{}};
    switch (x.op) {
      case lt:
      case leq:
      case gt:
      case geq:
        if (holds<std::string>(x.left) or holds<std::string>(x.right)) {
          return None{};
        }
        return unchanged;
      case eq:
      case neq: {
        if (not holds<ip>(x.left) and not holds<ip>(x.right)) {
          return unchanged;
        }
        auto const side = [&](ast::expression const& e) -> Option<std::string> {
          auto const* constant = constant_of(e);
          if (auto const* address
              = constant ? try_as<ip>(constant->value) : nullptr) {
            return ip_text(*address);
          }
          return renderer.value(e);
        };
        TRY(auto left, side(x.left));
        TRY(auto right, side(x.right));
        return Option<std::string>{
          fmt::format("({} {} {})", left, x.op == eq ? "==" : "!=", right)};
      }
      case in:
        return render_membership(x, renderer);
      default:
        return unchanged;
    }
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
        return "and";
      case or_:
        return "or";
      case in:
      case if_:
      case else_:
        return None{};
    }
    TENZIR_UNREACHABLE();
  }

  auto render_not(std::string_view operand) const
    -> Option<std::string> override {
    return fmt::format("not({})", operand);
  }

  auto render_negate(std::string_view operand) const
    -> Option<std::string> override {
    return fmt::format("(-{})", operand);
  }

  /// A string is never `null` in KQL, but may be empty.
  auto render_null_check(std::string_view operand, bool negated) const
    -> Option<std::string> override {
    return fmt::format("{}({})", negated ? "isnotnull" : "isnull", operand);
  }

  auto render_in(std::string_view operand,
                 std::span<std::string const> elements) const
    -> Option<std::string> override {
    return fmt::format("({} in ({}))", operand, fmt::join(elements, ", "));
  }

  /// String matching is case-sensitive, and `trim` trims the whitespace that
  /// `\s` matches.
  auto
  render_call(std::string_view name, std::span<std::string const> args) const
    -> Option<std::string> override {
    if (args.size() == 2) {
      auto const op = name == "starts_with" ? std::string_view{"startswith_cs"}
                      : name == "ends_with" ? std::string_view{"endswith_cs"}
                      : name == "match_regex"
                        ? std::string_view{"matches regex"}
                        : std::string_view{};
      if (op.empty()) {
        return None{};
      }
      return fmt::format("({} {} {})", args[0], op, args[1]);
    }
    if (args.size() != 1) {
      return None{};
    }
    if (name == "trim") {
      return fmt::format("trim(@\"\\s+\", {})", args[0]);
    }
    struct Function {
      std::string_view tql;
      std::string_view kql;
    };
    static constexpr auto functions = std::array{
      Function{"to_lower", "tolower"},
      Function{"to_upper", "toupper"},
      Function{"length_chars", "strlen"},
      Function{"abs", "abs"},
    };
    auto it = std::ranges::find(functions, name, &Function::tql);
    if (it == functions.end()) {
      return None{};
    }
    return fmt::format("{}({})", it->kql, args[0]);
  }

  auto call_is_predicate(std::string_view name) const -> bool override {
    return name == "starts_with" or name == "ends_with"
           or name == "match_regex";
  }

  /// `where flag` keeps the rows where a `bool` column holds.
  auto boolean_values() const -> bool override {
    return true;
  }

  auto bind_column(std::string_view column) const
    -> Option<std::string> override {
    return quote_name(column);
  }

  auto name_column(std::string_view column) const
    -> Option<std::string> override {
    return quote_name(column);
  }

  auto generated_name(std::string_view kind, size_t index) const
    -> std::string override {
    return fmt::format("__tenzir_{}_{}", kind, index);
  }

  auto filter_stage(std::string_view predicate) const
    -> Option<std::string> override {
    return fmt::format("\n| where {}", predicate);
  }

  auto select_stage(std::span<PipeAlias const> aliases,
                    std::span<std::string const> columns) const
    -> Option<std::string> override {
    auto assignments = std::vector<std::string>{};
    for (auto const& column : columns) {
      TRY(auto name, quote_name(column));
      auto alias = std::ranges::find(aliases, column, &PipeAlias::name);
      if (alias == aliases.end()) {
        assignments.push_back(std::move(name));
        continue;
      }
      TRY(auto input, bind_column(alias->input));
      assignments.push_back(fmt::format("{} = {}", name, input));
    }
    return fmt::format("\n| project {}", fmt::join(assignments, ", "));
  }

  auto drop_stage(std::span<std::string const> columns) const
    -> Option<std::string> override {
    TRY(auto names, quote_names(columns));
    return fmt::format("\n| project-away {}", fmt::join(names, ", "));
  }

  /// `count(x)` counts the values that are not empty: KQL has no `null`
  /// string, but an empty one. `mean` has no spelling: `avg` yields a `NaN`
  /// for no values.
  auto render_aggregate(std::string_view function,
                        Option<std::string_view> column) const
    -> Option<std::string> override {
    if (not column) {
      if (function == "count") {
        return std::string{"count()"};
      }
      return None{};
    }
    TRY(auto arg, bind_column(*column));
    if (function == "count") {
      return fmt::format("countif(isnotempty({}))", arg);
    }
    if (function == "min" or function == "max" or function == "sum") {
      return fmt::format("{}({})", function, arg);
    }
    return None{};
  }

  /// `summarize` names every result column, and puts the group keys first.
  auto aggregate_stage(std::span<PipeAlias const> aliases,
                       std::span<PipeFunction const> functions,
                       std::span<std::string const> keys) const
    -> Option<std::string> override {
    // Every column that a function reads has a name.
    if (not aliases.empty()) {
      return None{};
    }
    auto calls = std::vector<std::string>{};
    for (auto const& function : functions) {
      TRY(auto name, quote_name(function.name));
      calls.push_back(fmt::format("{} = {}", name, function.call));
    }
    auto result = fmt::format("\n| summarize {}", fmt::join(calls, ", "));
    if (not keys.empty()) {
      TRY(auto names, quote_names(keys));
      result += fmt::format(" by {}", fmt::join(names, ", "));
    }
    return result;
  }

  /// `sort` defaults to descending, so every key has an explicit direction.
  /// The limit after a sort with a bound enforces the bound.
  auto
  sort_stage(std::span<PipeSortKey const> keys, Option<uint64_t> bound) const
    -> Option<std::string> override {
    TENZIR_UNUSED(bound);
    auto names = std::vector<std::string>{};
    for (auto const& key : keys) {
      TRY(auto name, quote_name(key.column));
      names.push_back(
        fmt::format("{} {}", name, key.descending ? "desc" : "asc"));
    }
    return fmt::format("\n| sort by {}", fmt::join(names, ", "));
  }

  auto limit_stage(uint64_t count) const -> Option<std::string> override {
    if (count > max_take) {
      return None{};
    }
    return fmt::format("\n| take {}", count);
  }

private:
  static auto quote_names(std::span<std::string const> columns)
    -> Option<std::vector<std::string>> {
    auto result = std::vector<std::string>{};
    for (auto const& column : columns) {
      TRY(auto name, quote_name(column));
      result.push_back(std::move(name));
    }
    return result;
  }

  /// Spells `x in y`: a test for a substring of a string, for membership in
  /// a subnet, or for membership in a list with IP addresses, or the default
  /// spelling for other lists.
  auto render_membership(ast::binary_expr const& x,
                         ExpressionRenderer const& renderer) const
    -> Option<Option<std::string>> {
    if (auto const* constant = constant_of(x.right)) {
      if (auto const* network = try_as<subnet>(constant->value)) {
        TRY(auto subject, renderer.value(x.left));
        if (network->network().is_v4()) {
          return Option<std::string>{
            fmt::format("ipv4_is_in_range({}, \"{}\")", subject, *network)};
        }
        // KQL's IPv6 functions may read IPv4 addresses as mapped ones, so a
        // network that holds mapped addresses has no spelling.
        if (network->contains(subnet{ip::v4(uint32_t{0}), 96})) {
          return None{};
        }
        return Option<std::string>{
          fmt::format("ipv6_is_in_range({}, \"{}\")", subject, *network)};
      }
    }
    if (holds<std::string>(x.left) and not is_list(x.right)) {
      TRY(auto needle, renderer.value(x.left));
      TRY(auto haystack, renderer.value(x.right));
      return Option<std::string>{
        fmt::format("({} contains_cs {})", haystack, needle)};
    }
    auto constants = list_constants(x.right);
    if (not constants
        or not std::ranges::any_of(*constants, [](auto const& constant) {
             return is<ip>(constant);
           })) {
      return Option<std::string>{None{}};
    }
    auto elements = std::vector<std::string>{};
    for (auto const& constant : *constants) {
      if (auto const* address = try_as<ip>(constant)) {
        elements.push_back(ip_text(*address));
        continue;
      }
      TRY(auto element, renderer.literal(constant));
      elements.push_back(std::move(element));
    }
    TRY(auto subject, renderer.value(x.left));
    TRY(auto text, render_in(subject, elements));
    return Option<std::string>{std::move(text)};
  }
};

/// The top-level fields of `projection` that `stages` did not drop, or `None`
/// if the projection refers to a whole event.
auto projected_names(ir::OptimizeProjection const& projection,
                     std::span<ir::QueryStage const> stages)
  -> Option<std::vector<std::string>> {
  auto dropped = std::vector<std::string>{};
  for (auto const& stage : stages) {
    if (auto const* drop = try_as<ir::QueryStage::Drop>(stage.kind)) {
      for (auto const& path : drop->fields) {
        if (path.size() == 1) {
          dropped.push_back(path.front());
        }
      }
    }
  }
  auto names = std::vector<std::string>{};
  for (auto const& path : projection) {
    if (path.path().empty()) {
      return None{};
    }
    auto name = std::string{path.path().front().id.name};
    if (std::ranges::contains(names, name)
        or std::ranges::contains(dropped, name)) {
      continue;
    }
    names.push_back(std::move(name));
  }
  return names;
}

/// Spells the `project` stage that keeps the top-level fields of `projection`
/// that `stages` did not drop, or nothing if it keeps every field. A field
/// that the query does not return becomes `null`, where `project` would fail
/// the query. `column_ifexists` resolves while KQL binds the query, so that a
/// column that the query returns keeps its type.
auto projection_stage(Option<ir::OptimizeProjection> const& projection,
                      std::span<ir::QueryStage const> stages) -> std::string {
  if (not projection) {
    return {};
  }
  auto names = projected_names(*projection, stages);
  if (not names or names->empty()) {
    return {};
  }
  auto columns = std::vector<std::string>{};
  for (auto const& name : *names) {
    auto column = quote_name(name);
    auto literal = quote(name, '\'');
    if (not column or not literal) {
      return {};
    }
    columns.push_back(fmt::format("{} = column_ifexists({}, dynamic(null))",
                                  *column, *literal));
  }
  return fmt::format("\n| project {}", fmt::join(columns, ", "));
}

} // namespace

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
  // The nesting of brackets, outside of which `;` ends a statement.
  auto depth = size_t{0};
  // Whether no token of the current statement came yet, and whether the
  // statement declares something rather than returning a table. A table may
  // be named like a declaration keyword, as in `let alias = T; alias`, so a
  // statement declares only with the syntax of a declaration: `let <name> =`,
  // `declare query_parameters` or `declare pattern`, and `alias database`.
  auto statement_start = true;
  auto declares = false;
  auto tokens = size_t{0};
  auto keyword = std::string_view{};
  auto token = [&](char begin, char end, std::string_view word = {},
                   bool assignment = false) {
    if (not first) {
      first = begin;
    }
    if (statement_start) {
      statement_start = false;
      declares = false;
      tokens = 0;
      keyword = std::ranges::contains(declaration_words, word)
                  ? word
                  : std::string_view{};
    } else if (tokens == 1 and keyword == "declare") {
      declares = word == "query_parameters" or word == "pattern";
    } else if (tokens == 1 and keyword == "alias") {
      declares = word == "database";
    } else if (tokens == 1 and keyword == "let" and word.empty()) {
      keyword = {};
    } else if (tokens == 2 and keyword == "let") {
      declares = assignment;
    }
    ++tokens;
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
      auto delimiter = query[j];
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
        if (d == delimiter) {
          if (verbatim and k + 1 < n and query[k + 1] == delimiter) {
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
      token(c, delimiter);
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
      auto word = query.substr(i, k - i);
      if (std::ranges::contains(opaque_words, word)) {
        return false;
      }
      token(c, query[k - 1], word);
      i = k;
      continue;
    }
    switch (c) {
      case '(':
      case '[':
      case '{':
        ++depth;
        break;
      case ')':
      case ']':
      case '}':
        if (depth == 0) {
          return false;
        }
        --depth;
        break;
      case ';':
        // Only the last statement may return a table.
        if (depth == 0) {
          if (statement_start or not declares) {
            return false;
          }
          last = c;
          statement_start = true;
          ++i;
          continue;
        }
        break;
      default:
        break;
    }
    // A single `=` assigns, unlike `==` and `=~`.
    auto const assignment
      = c == '='
        and (i + 1 >= n or (query[i + 1] != '=' and query[i + 1] != '~'));
    token(c, c, {}, assignment);
    ++i;
  }
  if (not first or *first == '.' or depth != 0) {
    return false;
  }
  return *last != ';' and *last != '|';
}

auto plan_kql_query(std::string_view query,
                    std::span<ir::QueryStage const> stages,
                    Option<ir::OptimizeProjection> const& projection)
  -> Option<KqlQuery> {
  auto result = KqlQuery{
    .query = std::string{query},
    .generated = {},
    .layout = {},
  };
  if (not kql_accepts_trailing_stages(query)) {
    if (not stages.empty()) {
      return None{};
    }
    return result;
  }
  // KQL holds times in typed columns, so the plan needs no time column, and
  // the request window never depends on the stages.
  auto const options = PipeOptions{
    .extend_base = false,
    .time_column = None{},
    .native_time = true,
    .narrow = false,
  };
  TRY(auto pipe, render_pipe(stages, KqlDialect{}, options));
  // Both services limit the size of a query, so stages that would grow it
  // beyond the budget stay in the pipeline, and a projection that would only
  // stays out of the query.
  if (pipe.stages.size() > max_kql_appendix) {
    return None{};
  }
  result.generated = std::move(pipe.stages);
  if (not pipe.layout.fields) {
    auto project = projection_stage(projection, stages);
    if (result.generated.size() + project.size() <= max_kql_appendix) {
      result.generated += project;
    }
  }
  result.query += result.generated;
  result.layout = std::move(pipe.layout);
  return result;
}

} // namespace tenzir::pushdown
