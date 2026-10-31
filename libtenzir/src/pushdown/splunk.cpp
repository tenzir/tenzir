//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pushdown/splunk.hpp"

#include "tenzir/data_builder.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/hash/hash.hpp"
#include "tenzir/ip.hpp"
#include "tenzir/nova/event_builder.hpp"
#include "tenzir/nova/union_array.hpp"
#include "tenzir/pushdown/aggregation.hpp"
#include "tenzir/ref.hpp"
#include "tenzir/subnet.hpp"
#include "tenzir/try.hpp"

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cmath>
#include <limits>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace tenzir::pushdown {

namespace {

/// The size budget of the appended stages. Splunk accepts much longer
/// searches, but a long tail of prefilters rarely pays off.
constexpr auto appendix_budget = size_t{16384};

/// The largest count that `head` accepts.
constexpr auto max_head
  = static_cast<uint64_t>(std::numeric_limits<int64_t>::max());

/// Whether `text` holds a control character, which a quoted SPL string or
/// field name should not contain.
auto has_control(std::string_view text) -> bool {
  return std::ranges::any_of(text, [](char c) {
    auto byte = static_cast<unsigned char>(c);
    return byte < 0x20 or byte == 0x7f;
  });
}

/// Quotes a string value for `search`, escaping `"` and `\`. A `*` stays a
/// wildcard, which only widens the match.
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

/// Spells a field name, quoting it unless it is a plain identifier. Names that
/// a quoted field name cannot hold, and names with wildcards, are vetoed.
auto quote_field(std::string_view name) -> Option<std::string> {
  if (name.empty() or has_control(name)
      or name.find_first_of("\"\\*") != std::string_view::npos) {
    return None{};
  }
  auto plain = (std::isalpha(static_cast<unsigned char>(name.front()))
                or name.front() == '_')
               and std::ranges::all_of(name,
                                       [](char c) {
                                         return std::isalnum(
                                                  static_cast<unsigned char>(c))
                                                or c == '_';
                                       })
               and not is_keyword(name);
  if (plain) {
    return std::string{name};
  }
  return fmt::format("\"{}\"", name);
}

/// Spells a top-level column. `from_splunk` keeps Splunk's field names, such
/// as `obj.k` or `tags{}`, as top-level fields, so nested paths have no
/// counterpart in Splunk.
auto render_field(Expr const& expr) -> Option<std::string> {
  auto const* column = try_as<Column>(expr);
  if (not column or column->path.size() != 1
      or is_search_modifier(column->path.front())) {
    return None{};
  }
  return quote_field(column->path.front());
}

/// Spells a field for a comparison with a string, number, or boolean. Splunk
/// holds `_time` as epoch seconds, while `from_splunk` receives it as a
/// formatted timestamp, so such comparisons on `_time` stay local.
auto render_value_field(Expr const& expr) -> Option<std::string> {
  TRY(auto field, render_field(expr));
  if (field == "_time") {
    return None{};
  }
  return field;
}

/// Spells a number without an exponent, which `search` might read as text.
/// Translation bounds integers to ±2^53 for dynamic columns.
auto render_number(Literal const& literal) -> Option<std::string> {
  return match(
    literal.value,
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

/// Spells a literal as a value that `field=value` compares with: a number
/// compares numerically, a string or boolean case-insensitively.
auto render_value(Literal const& literal) -> Option<std::string> {
  return match(
    literal.value,
    [](std::string const& x) -> Option<std::string> {
      return quote_value(x);
    },
    [](bool x) -> Option<std::string> {
      return x ? "true" : "false";
    },
    [&](auto const&) -> Option<std::string> {
      return render_number(literal);
    });
}

auto comparison_operator(BinaryOp op) -> Option<std::string_view> {
  switch (op) {
    case BinaryOp::eq:
      return "=";
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

/// Compares `_time`, which Splunk holds as epoch seconds and `from_splunk`
/// decodes from the rendered timestamp. Bounds round outwards to whole
/// seconds, so that rendering and decoding precision only widen the match.
/// Other fields hold timestamps as text, which `search` cannot order.
auto render_time_comparison(std::string_view field, BinaryOp op, time value)
  -> Option<std::string> {
  if (field != "_time") {
    return None{};
  }
  using std::chrono::seconds;
  auto since_epoch = value.time_since_epoch();
  auto lower = std::chrono::floor<seconds>(since_epoch).count();
  auto upper = std::chrono::ceil<seconds>(since_epoch).count();
  switch (op) {
    case BinaryOp::eq:
      return fmt::format("(_time>={} _time<={})", lower, upper);
    case BinaryOp::gt:
    case BinaryOp::geq:
      return fmt::format("_time>={}", lower);
    case BinaryOp::lt:
    case BinaryOp::leq:
      return fmt::format("_time<={}", upper);
    default:
      return None{};
  }
}

/// Spells `field op literal`.
///
/// - A number compares numerically where the value parses as a number, and
///   as text otherwise, which TQL's comparison of a number with a string never
///   keeps anyway.
/// - A string or boolean compares for equality, case-insensitively. Ordering
///   on strings stays local: TQL orders bytes, `search` does not.
auto render_comparison(Binary const& x) -> Option<std::string> {
  auto const* literal = try_as<Literal>(*x.right);
  if (not literal) {
    return None{};
  }
  if (auto const* instant = try_as<time>(literal->value)) {
    TRY(auto field, render_field(*x.left));
    return render_time_comparison(field, x.op, *instant);
  }
  TRY(auto field, render_value_field(*x.left));
  TRY(auto op, comparison_operator(x.op));
  auto ordered = x.op != BinaryOp::eq;
  if (ordered) {
    TRY(auto value, render_number(*literal));
    return fmt::format("{}{}{}", field, op, value);
  }
  TRY(auto value, render_value(*literal));
  return fmt::format("{}={}", field, value);
}

/// Spells `field in [literals]` with `IN`, which holds where `field=value`
/// holds for some element.
auto render_in(In const& x) -> Option<std::string> {
  if (x.list.empty()) {
    return None{};
  }
  TRY(auto field, render_value_field(*x.expr));
  auto values = std::vector<std::string>{};
  for (auto const& element : x.list) {
    auto const* literal = try_as<Literal>(element);
    if (not literal) {
      return None{};
    }
    TRY(auto value, render_value(*literal));
    values.push_back(std::move(value));
  }
  return fmt::format("{} IN ({})", field, fmt::join(values, ", "));
}

/// Spells a string relation as a wildcard pattern. Splunk matches patterns
/// case-insensitively, which keeps every case-sensitive match, and reads a `*`
/// in the needle as a wildcard too, which only widens the pattern.
auto render_pattern(std::string_view field, std::string_view needle,
                    StringMatchKind kind) -> Option<std::string> {
  auto pattern = std::string{};
  switch (kind) {
    case StringMatchKind::equal:
      pattern = needle;
      break;
    case StringMatchKind::prefix:
      pattern = fmt::format("{}*", needle);
      break;
    case StringMatchKind::suffix:
      pattern = fmt::format("*{}", needle);
      break;
    case StringMatchKind::substring:
      pattern = fmt::format("*{}*", needle);
      break;
  }
  TRY(auto value, quote_value(pattern));
  return fmt::format("{}={}", field, value);
}

/// Spells `starts_with`, `ends_with`, and `contains` on a field.
auto render_call(Call const& x) -> Option<std::string> {
  auto kind = StringMatchKind{};
  switch (x.op) {
    case Operation::starts_with:
      kind = StringMatchKind::prefix;
      break;
    case Operation::ends_with:
      kind = StringMatchKind::suffix;
      break;
    case Operation::contains:
      kind = StringMatchKind::substring;
      break;
    default:
      return None{};
  }
  if (x.args.size() != 2) {
    return None{};
  }
  TRY(auto field, render_value_field(x.args[0]));
  auto const* literal = try_as<Literal>(x.args[1]);
  auto const* needle = literal ? try_as<std::string>(literal->value) : nullptr;
  if (not needle) {
    return None{};
  }
  return render_pattern(field, *needle, kind);
}

/// Spells a string match. A recursive match may find the needle in a nested
/// value, which a field of a Splunk result does not have, so it stays local.
auto render_string_match(StringMatch const& x) -> Option<std::string> {
  if (x.recursive) {
    return None{};
  }
  TRY(auto field, render_value_field(Expr{x.column}));
  return render_pattern(field, x.needle, x.kind);
}

/// Spells `field != null` as `field=*`, which holds for every value of a field
/// that a result has, including the empty string.
auto render_not_null(IsNull const& x) -> Option<std::string> {
  if (not x.negated) {
    return None{};
  }
  TRY(auto field, render_field(*x.expr));
  return fmt::format("{}=*", field);
}

/// The last address of `network`.
auto last_address(subnet const& network) -> ip {
  auto all_ones = std::array<uint8_t, 16>{};
  all_ones.fill(0xff);
  auto host_mask = ip{all_ones};
  auto net_mask = ip{all_ones};
  net_mask.mask(network.length());
  host_mask ^= net_mask;
  auto last = network.network();
  last |= host_mask;
  return last;
}

/// The CIDR terms that match an address. Splunk's CIDR matching parses the
/// values of a field, so a single-host network matches every spelling of an
/// address, such as `2001:0db8::0:1` for `2001:db8::1`, while a plain value
/// matches only the same text. An IPv4 address also needs its IPv4-mapped
/// IPv6 network, since Splunk matches IPv4 networks only against IPv4 values.
auto address_values(ip const& address) -> Option<std::vector<std::string>> {
  auto canonical = fmt::to_string(address);
  if (address.is_v4()) {
    TRY(auto value, quote_value(fmt::format("{}/32", canonical)));
    TRY(auto mapped, quote_value(fmt::format("::ffff:{}/128", canonical)));
    return std::vector{std::move(value), std::move(mapped)};
  }
  TRY(auto value, quote_value(fmt::format("{}/128", canonical)));
  return std::vector{std::move(value)};
}

/// The network whose addresses are exactly `[lo, hi]`, or `None`.
auto network_of(ip const& lo, ip const& hi) -> Option<subnet> {
  for (auto length = 0; length <= 128; ++length) {
    auto network = subnet{lo, static_cast<uint8_t>(length)};
    if (network.network() == lo and last_address(network) == hi) {
      return network;
    }
  }
  return None{};
}

/// The spellings of a network that Splunk matches with CIDR matching. A
/// network that covers IPv4-mapped addresses only in part, or IPv4 and IPv6
/// addresses alike, has none: Splunk matches an IPv4 network only against
/// IPv4 addresses, and an IPv6 network only against IPv6 addresses.
auto network_values(subnet const& network) -> Option<std::vector<std::string>> {
  auto first = network.network();
  if (first.is_v4()) {
    if (network.length() < 96) {
      return None{};
    }
    // `subnet` spells an IPv4-mapped network in IPv4 notation.
    auto v4 = fmt::to_string(network);
    TRY(auto value, quote_value(v4));
    TRY(auto mapped,
        quote_value(fmt::format("::ffff:{}/{}", first, network.length())));
    return std::vector{std::move(value), std::move(mapped)};
  }
  auto mapped = subnet{ip::v4(uint32_t{0}), 96};
  if (network.contains(mapped.network())) {
    return None{};
  }
  TRY(auto value, quote_value(fmt::to_string(network)));
  return std::vector{std::move(value)};
}

/// Spells the condition on `ip(field)` that a parsed-address prefilter
/// holds: equality with an address, membership in a list of addresses, or
/// membership in a network.
auto render_parsed_ip(Expr const& field_expr, Expr const& parsed,
                      Expr const& condition) -> Option<std::string> {
  TRY(auto field, render_value_field(field_expr));
  auto is_subject = [&](Expr const& x) {
    auto const* call = try_as<Call>(x);
    auto const* column = try_as<Column>(field_expr);
    if (not call or call->op != Operation::parse_ip or call->args.size() != 1
        or not column) {
      return false;
    }
    auto const* arg = try_as<Column>(call->args.front());
    return arg and arg->path == column->path;
  };
  TENZIR_ASSERT(is_subject(parsed));
  auto address = [](Expr const& x) -> Option<ip> {
    auto const* literal = try_as<Literal>(x);
    auto const* value = literal ? try_as<IpValue>(literal->value) : nullptr;
    if (not value) {
      return None{};
    }
    return value->address;
  };
  auto values = std::vector<std::string>{};
  if (auto const* x = try_as<Binary>(condition)) {
    if (x->op != BinaryOp::eq or not is_subject(*x->left)) {
      return None{};
    }
    TRY(auto value, address(*x->right));
    TRY(values, address_values(value));
  } else if (auto const* x = try_as<In>(condition)) {
    if (not is_subject(*x->expr) or x->list.empty()) {
      return None{};
    }
    for (auto const& element : x->list) {
      TRY(auto value, address(element));
      TRY(auto spellings, address_values(value));
      std::ranges::move(spellings, std::back_inserter(values));
    }
  } else if (auto const* x = try_as<Between>(condition)) {
    if (not is_subject(*x->expr)) {
      return None{};
    }
    TRY(auto lo, address(*x->lo));
    TRY(auto hi, address(*x->hi));
    TRY(auto network, network_of(lo, hi));
    TRY(values, network_values(network));
  } else {
    return None{};
  }
  if (values.size() == 1) {
    return fmt::format("{}={}", field, values.front());
  }
  auto terms = std::vector<std::string>{};
  for (auto& value : values) {
    terms.push_back(fmt::format("{}={}", field, value));
  }
  return fmt::format("({})", fmt::join(terms, " OR "));
}

/// Marks a rendered predicate as an expression of a `where` stage rather
/// than as a term of the `search` command. Rendered text never contains
/// control characters otherwise.
constexpr auto where_marker = std::string_view{"\x1f"};

/// Returns the expression of a predicate that `where_marker` marks.
auto where_expression(std::string_view predicate) -> Option<std::string_view> {
  if (not predicate.starts_with(where_marker)) {
    return None{};
  }
  return predicate.substr(where_marker.size());
}

/// Whether `eval` reads the field `name` when it is quoted in single quotes.
/// Live probes found that other characters fail the search, such as `|` and
/// `[`, or silently read another field, such as `\\` and `::`.
auto is_eval_name(std::string_view name) -> bool {
  if (name.empty() or name.front() == ' ' or name.back() == ' ') {
    return false;
  }
  return std::ranges::all_of(name, [](char c) {
    auto byte = static_cast<unsigned char>(c);
    return byte >= 0x80 or (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z')
           or (c >= '0' and c <= '9')
           or std::string_view{"_-.{}@/ "}.contains(c);
  });
}

/// Spells a field name for `eval`, quoting it in single quotes unless it is a
/// plain identifier, or returns `None` if `eval` cannot read it.
auto eval_field(std::string_view name) -> Option<std::string> {
  if (not is_eval_name(name)) {
    return None{};
  }
  auto plain = (std::isalpha(static_cast<unsigned char>(name.front()))
                or name.front() == '_')
               and std::ranges::all_of(name,
                                       [](char c) {
                                         return std::isalnum(
                                                  static_cast<unsigned char>(c))
                                                or c == '_';
                                       })
               and not is_keyword(name);
  if (plain) {
    return std::string{name};
  }
  return fmt::format("'{}'", name);
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

/// Options of `makeresults` that keep `_time` in its results. Others, such as
/// `format=csv` with `data`, create results with only the fields that `data`
/// lists, which `fields` may then remove.
auto keeps_time(std::string_view option) -> bool {
  constexpr auto options = std::array<std::string_view, 4>{
    "annotate",
    "count",
    "splunk_server",
    "splunk_server_group",
  };
  return std::ranges::any_of(options, [&](std::string_view x) {
    return detail::ascii_icase_equal(option, x);
  });
}

/// Commands that keep the fields of the results and add no result without
/// fields. `fields` keeps fields only without `-`, since it keeps internal
/// fields such as `_time` otherwise. A field stays in the results also if
/// `eval` assigns it `null()` in every result, which Splunk then returns as
/// empty objects.
auto keeps_fields(std::string_view command) -> bool {
  constexpr auto commands = std::array<std::string_view, 36>{
    "accum",       "addinfo", "autoregress", "bin",      "bucket",
    "convert",     "dedup",   "delta",       "eval",     "eventstats",
    "extract",     "fields",  "filldown",    "fillnull", "head",
    "iplocation",  "kv",      "lookup",      "makemv",   "mvcombine",
    "mvexpand",    "nomv",    "rangemap",    "regex",    "replace",
    "reverse",     "rex",     "search",      "sort",     "spath",
    "streamstats", "strcat",  "tail",        "uniq",     "where",
    "xmlkv",
  };
  return std::ranges::any_of(commands, [&](std::string_view x) {
    return detail::ascii_icase_equal(command, x);
  });
}

/// The trailing stages of SPL: `search`, `where`, `fields`, and `head`.
///
/// For a search that Tenzir generates, which ends in its base `search`
/// command, the prefilters extend that command instead.
class SplDialect final : public PipeDialect {
public:
  SplDialect(bool extend_base, SplRenderer::Settings settings)
    : renderer_{settings}, extend_base_{extend_base} {
  }

  auto accepts_trailing_stages(std::string_view query) const -> bool override {
    return spl_stages(query).accepted;
  }

  auto renderer() const -> Renderer const& override {
    return renderer_;
  }

  /// Terms of the `search` command extend the base search or follow in a
  /// `search` stage, and expressions follow in a `where` stage.
  auto filter_stage(std::span<std::string const> predicates) const
    -> std::string override {
    auto terms = std::vector<std::string_view>{};
    auto expressions = std::vector<std::string>{};
    for (auto const& predicate : predicates) {
      if (auto expression = where_expression(predicate)) {
        expressions.push_back(fmt::format("({})", *expression));
      } else {
        terms.push_back(predicate);
      }
    }
    auto result = std::string{};
    if (not terms.empty()) {
      result += extend_base_ ? " " : "\n| search ";
      result += fmt::to_string(fmt::join(terms, " "));
    }
    if (not expressions.empty()) {
      result += fmt::format("\n| where {}", fmt::join(expressions, " AND "));
    }
    return result;
  }

  /// `fields` keeps Splunk's internal fields, such as `_raw` and `_time`, and
  /// ignores names that the results do not have.
  auto project_stage(std::span<std::string const> names) const
    -> Option<std::string> override {
    auto fields = std::vector<std::string>{};
    for (auto const& name : names) {
      TRY(auto field, quote_field(name));
      fields.push_back(std::move(field));
    }
    return fmt::format("\n| fields {}", fmt::join(fields, ", "));
  }

  auto limit_stage(uint64_t n) const -> Option<std::string> override {
    if (n > max_head) {
      return None{};
    }
    return fmt::format("\n| head {}", n);
  }

  auto max_appendix() const -> size_t override {
    return appendix_budget;
  }

private:
  SplRenderer renderer_;
  bool extend_base_ = false;
};

/// Adds `fields *` to a plan without a projection stage, before any `head`
/// stage, so that the stages keep the order filter, projection, limit.
auto with_all_fields(PipePlan plan) -> PipePlan {
  if (plan.projected) {
    return plan;
  }
  constexpr auto all_fields = std::string_view{"\n| fields *"};
  if (plan.limited) {
    auto head = plan.query.rfind("\n| head ");
    TENZIR_ASSERT(head != std::string::npos);
    plan.query.insert(head, all_fields);
  } else {
    plan.query += all_fields;
  }
  return plan;
}

/// The column of `stats` that holds output column `index` of an aggregation.
/// Splunk leaves fields whose names start with `__` out of its results, so the
/// column cannot be `aggregation_column(index)`.
auto spl_aggregate_name(size_t index) -> std::string {
  return fmt::format("tenzir_aggregate_{}", index);
}

/// The column of `stats` that is `1` for a group in which the input of the
/// `min` or `max` in output column `index` has a value other than an integer
/// that the stages compare.
auto spl_check_name(size_t index) -> std::string {
  return fmt::format("tenzir_check_{}", index);
}

/// The column of `stats` that counts the results of a group. Every `count()`
/// reads it: `stats` computes a function only once per input, and keeps only
/// the last of the names that it gives a repeated function.
constexpr auto spl_count_name = std::string_view{"tenzir_count"};

/// The column of the last row, which counts the groups.
constexpr auto spl_groups_name = std::string_view{"tenzir_groups"};

/// The values that `min` and `max` compare, as an `eval` string: integers that
/// `from_splunk` infers as `int64`, also with leading zeros, and that Splunk
/// compares exactly, since their magnitude stays below 2^53.
constexpr auto spl_integer_pattern = std::string_view{R"("^-?[0-9]{1,15}$")"};

/// Spells the input field of an aggregation for `eval`, in single quotes, or
/// returns `None` if `eval` might read another field, as `is_eval_name`
/// describes. A name that starts with `_` is an internal field, such as
/// `_time`, whose value `eval` sees differently from the results, and the
/// stages write fields that start with `tenzir_`.
auto eval_input(std::vector<std::string> const& path) -> Option<std::string> {
  if (path.size() != 1) {
    return None{};
  }
  auto const& name = path.front();
  if (name.starts_with('_') or name.starts_with("tenzir_")
      or not is_eval_name(name)) {
    return None{};
  }
  return fmt::format("'{}'", name);
}

/// Parses a count that Splunk renders as decimal digits.
auto parse_count(std::string_view text) -> Option<int64_t> {
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

/// Parses the `min` or `max` of integers that match `spl_integer_pattern`,
/// which Splunk renders as such an integer again, such as `-0` or `7`.
auto parse_integer(std::string_view text) -> Option<int64_t> {
  auto result = int64_t{};
  auto digits = text.starts_with('-') ? text.substr(1) : text;
  if (digits.empty() or digits.size() > 15
      or not std::ranges::all_of(digits, [](char c) {
           return c >= '0' and c <= '9';
         })) {
    return None{};
  }
  auto [end, error] = std::from_chars(text.begin(), text.end(), result);
  if (error != std::errc{} or end != text.end()) {
    return None{};
  }
  return result;
}

/// Whether type inference turns `text` into a time relative to the current
/// time, such as `now` or `5 min ago`. Every result of a search infers such a
/// time anew, so `summarize` groups each result apart, where Splunk groups
/// the text once.
auto is_relative_time(std::string_view text) -> bool {
  if (not text.starts_with("now") and not text.starts_with("in")
      and not text.ends_with("ago")) {
    return false;
  }
  auto parsed = detail::data_builder::best_effort_parser(text);
  return parsed and is<time>(*parsed);
}

/// A group key of one output row, as views into the decoded events.
using KeyView = std::vector<nova::RowView<nova::Data>>;

/// Checks that no two decoded group keys group together in `summarize`.
/// Splunk groups the keys by their text, so all results of a group share a
/// key, but type inference can turn different text into equivalent values,
/// such as `5` and `5.0`, which `summarize` then groups together.
auto check_distinct_keys(std::span<nova::Events const> batches,
                         ir::OptimizeAggregation const& aggregation)
  -> Result<void, std::string> {
  auto keys = std::vector<size_t>{};
  for (auto i = size_t{0}; i < aggregation.columns.size(); ++i) {
    if (not aggregation.columns[i].function) {
      keys.push_back(i);
    }
  }
  if (keys.empty()) {
    return {};
  }
  // The views in `seen` borrow from these columns, which must outlive them.
  auto columns_per_batch = std::vector<std::vector<nova::Array<nova::Data>>>{};
  columns_per_batch.reserve(batches.size());
  auto seen = std::unordered_map<uint64_t, std::vector<KeyView>>{};
  for (auto const& batch : batches) {
    auto& columns = columns_per_batch.emplace_back();
    for (auto index : keys) {
      auto field = batch.data.field(aggregation_column(index));
      TENZIR_ASSERT(field);
      TENZIR_ASSERT(field->present.true_count() == batch.data.length());
      columns.push_back(std::move(field->data));
    }
    for (auto row = nova::storage::Index{0}; row < batch.length(); ++row) {
      auto key = KeyView{};
      auto hash = uint64_t{0};
      for (auto const& column : columns) {
        auto value = column.get(row);
        hash = tenzir::hash(hash, static_cast<uint64_t>(nova::hash(value)));
        key.push_back(value);
      }
      auto& candidates = seen[hash];
      for (auto const& other : candidates) {
        if (std::ranges::equal(key, other, [](auto const& x, auto const& y) {
              return nova::equivalent(x, y);
            })) {
          return Err{std::string{"type inference merges two groups"}};
        }
      }
      candidates.push_back(std::move(key));
    }
  }
  return {};
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

SplRenderer::SplRenderer(Settings settings) : settings_{settings} {
}

auto SplRenderer::prefilter_options() const -> PrefilterOptions {
  return {.negations = true};
}

auto SplRenderer::render_composite(Expr const& expr) const
  -> Option<std::string> {
  if (settings_.formatted) {
    return None{};
  }
  // Both negations and parsed addresses keep a null explicitly:
  // `Or{IsNull{subject}, condition}`.
  auto const* x = try_as<Or>(expr);
  if (not x or x->operands.size() != 2) {
    return None{};
  }
  auto const* null_check = try_as<IsNull>(x->operands[0]);
  if (not null_check or null_check->negated) {
    return None{};
  }
  auto const& subject = *null_check->expr;
  auto const& condition = x->operands[1];
  if (auto const* call = try_as<Call>(subject)) {
    if (call->op != Operation::parse_ip or call->args.size() != 1) {
      return None{};
    }
    // Splunk matches addresses and networks by their text, so the null of
    // a string that does not parse as an address needs no spelling: TQL
    // never matches such a string either.
    return render_parsed_ip(call->args.front(), subject, condition);
  }
  return render_negation(subject, condition);
}

auto SplRenderer::render_negated_value(Literal const& literal) const
  -> Option<std::string> {
  return match(
    literal.value,
    [&](std::string const& x) -> Option<std::string> {
      // With inference, a Splunk value spelled like the literal becomes
      // another type, which differs from the string in TQL, so negating the
      // match would drop it.
      if (settings_.inference) {
        if (auto inferred = detail::data_builder::best_effort_parser(x);
            inferred and not is<std::string>(*inferred)) {
          return None{};
        }
      }
      // Quotes and backslashes, whose escaping in `eval` we have not
      // verified, stay local.
      if (has_control(x) or x.find_first_of("\"\\") != std::string::npos) {
        return None{};
      }
      return fmt::format("\"{}\"", x);
    },
    [&](bool x) -> Option<std::string> {
      // Without inference, the string `"true"` differs from the boolean.
      if (not settings_.inference) {
        return None{};
      }
      return x ? "\"true\"" : "\"false\"";
    },
    [&](auto const&) -> Option<std::string> {
      // Without inference, the string `"5"` differs from the number.
      if (not settings_.inference) {
        return None{};
      }
      return render_number(literal);
    });
}

auto SplRenderer::render_negation(Expr const& subject,
                                  Expr const& negated) const
  -> Option<std::string> {
  // Validates the field like every other term, but spells it for `eval`.
  TRY(render_value_field(subject));
  auto const& column = as<Column>(subject);
  TRY(auto field, eval_field(column.path.front()));
  auto values = std::vector<std::string>{};
  if (auto const* x = try_as<Binary>(negated)) {
    auto const* left = try_as<Column>(*x->left);
    auto const* literal = try_as<Literal>(*x->right);
    if (x->op != BinaryOp::neq or not left or left->path != column.path
        or not literal) {
      return None{};
    }
    TRY(auto value, render_negated_value(*literal));
    values.push_back(std::move(value));
  } else {
    auto const* y = try_as<Not>(negated);
    auto const* set = y ? try_as<In>(*y->expr) : nullptr;
    auto const* left = set ? try_as<Column>(*set->expr) : nullptr;
    if (not left or left->path != column.path or set->list.empty()) {
      return None{};
    }
    for (auto const& element : set->list) {
      auto const* literal = try_as<Literal>(element);
      if (not literal) {
        return None{};
      }
      TRY(auto value, render_negated_value(*literal));
      values.push_back(std::move(value));
    }
  }
  // The `search` command drops an event if any value of a multivalue field
  // matches, while TQL keeps the list, which differs from every scalar. An
  // `eval` expression keeps fields with several values and tests the others
  // exactly. `if` keeps values that do not compare, such as `"abc"` against
  // `5`, as TQL does.
  auto equalities = std::vector<std::string>{};
  for (auto const& value : values) {
    equalities.push_back(fmt::format("{}=={}", field, value));
  }
  return fmt::format("{}isnull({}) OR mvcount({})>1 OR if({}, false(), true())",
                     where_marker, field, field, fmt::join(equalities, " OR "));
}

auto SplRenderer::render_atom(Expr const& expr) const -> Option<std::string> {
  if (settings_.formatted) {
    return None{};
  }
  return match(
    expr,
    [&](Binary const& x) -> Option<std::string> {
      auto const* literal = try_as<Literal>(*x.right);
      if (not settings_.push_time and literal and is<time>(literal->value)) {
        return None{};
      }
      return render_comparison(x);
    },
    [](In const& x) -> Option<std::string> {
      return render_in(x);
    },
    [](Call const& x) -> Option<std::string> {
      return render_call(x);
    },
    [](StringMatch const& x) -> Option<std::string> {
      return render_string_match(x);
    },
    [](IsNull const& x) -> Option<std::string> {
      return render_not_null(x);
    },
    [](auto const&) -> Option<std::string> {
      return None{};
    });
}

auto SplRenderer::render_junction(Junction junction,
                                  std::span<std::string const> operands) const
  -> Option<std::string> {
  // Operands are terms of the `search` command or, with `where_marker`,
  // expressions of a `where` stage, which do not mix.
  auto terms = std::vector<std::string>{};
  auto expressions = std::vector<std::string>{};
  for (auto const& operand : operands) {
    if (auto expression = where_expression(operand)) {
      expressions.push_back(fmt::format("({})", *expression));
    } else {
      terms.push_back(operand);
    }
  }
  if (junction == Junction::conjunction) {
    // Dropping the expressions of a conjunction only widens it.
    if (not terms.empty()) {
      if (terms.size() == 1) {
        return terms.front();
      }
      return fmt::format("({})", fmt::join(terms, " AND "));
    }
    return fmt::format("{}{}", where_marker, fmt::join(expressions, " AND "));
  }
  if (not terms.empty() and not expressions.empty()) {
    return None{};
  }
  if (not expressions.empty()) {
    return fmt::format("{}{}", where_marker, fmt::join(expressions, " OR "));
  }
  if (terms.size() == 1) {
    return terms.front();
  }
  return fmt::format("({})", fmt::join(terms, " OR "));
}

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

auto spl_stages(std::string_view search) -> SplStages {
  auto result = SplStages{};
  auto side_effects = false;
  auto macro = false;
  auto formatted = false;
  auto removes_fields = false;
  // Whether the search started, and whether its first command is known.
  auto started = false;
  auto first_seen = false;
  // Whether the lexer is in the arguments of a leading `makeresults`.
  auto in_makeresults = false;
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
  // Whether `=` follows position `end`, so that the token before it names an
  // option, such as `count` in `count=3`.
  auto names_option = [&](size_t end) {
    while (end < search.size() and is_space(search[end])) {
      ++end;
    }
    return end < search.size() and search[end] == '=';
  };
  while (i < search.size()) {
    auto c = search[i];
    if (search.substr(i).starts_with("```")) {
      auto end = search.find("```", i + 3);
      if (end == std::string_view::npos) {
        return result;
      }
      i = end + 3;
      continue;
    }
    if (is_space(c)) {
      ++i;
      continue;
    }
    if (not started) {
      started = true;
      // A search that does not start with `|` starts with `search`, which
      // returns events.
      first_seen = c != '|';
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
      if (depth == 0 and not first_seen) {
        // The events of `search` have internal fields, such as `_cd`, and the
        // results of `makeresults` have `_time`, which other generating
        // commands do not guarantee. `fields` without `-` keeps them.
        first_seen = true;
        in_makeresults = detail::ascii_icase_equal(command, "makeresults");
        removes_fields = removes_fields
                         or not(detail::ascii_icase_equal(command, "search")
                                or in_makeresults);
      } else {
        auto next = end;
        while (next < search.size() and is_space(search[next])) {
          ++next;
        }
        auto removes = detail::ascii_icase_equal(command, "fields")
                         ? next < search.size() and search[next] == '-'
                         : not keeps_fields(command);
        removes_fields = removes_fields or removes;
      }
      at_command = false;
    } else if (in_makeresults and is_word(c)
               and (i == 0 or not is_word(search[i - 1]))) {
      auto end = i;
      while (end < search.size() and is_word(search[end])) {
        ++end;
      }
      removes_fields
        = removes_fields
          or (names_option(end) and not keeps_time(search.substr(i, end - i)));
    }
    last = c;
    switch (c) {
      case '"': {
        // Strings escape with a backslash.
        ++i;
        while (i < search.size() and search[i] != '"') {
          i += search[i] == '\\' ? 2 : 1;
        }
        if (i >= search.size()) {
          return result;
        }
        ++i;
        // Splunk also reads quoted options of `makeresults`, such as
        // `"format"=csv`.
        removes_fields = removes_fields or (in_makeresults and names_option(i));
        continue;
      }
      case '\'':
      case '`': {
        // Quoted field names and macros end at the next equal quote.
        auto end = search.find(c, i + 1);
        if (end == std::string_view::npos) {
          return result;
        }
        macro = macro or c == '`';
        i = end + 1;
        removes_fields = removes_fields or (in_makeresults and names_option(i));
        continue;
      }
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
        in_makeresults = in_makeresults and depth != 0;
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
  result.keeps_fields = not removes_fields and not macro;
  return result;
}

auto plan_spl_search(std::string_view search, ir::OptimizeFilter const& filter,
                     Option<ir::OptimizeProjection> const& projection,
                     Option<uint64_t> limit, bool inference,
                     registry const& reg) -> PipePlan {
  auto stages = spl_stages(search);
  if (not stages.accepted) {
    return PipePlan{.query = std::string{search}};
  }
  // A search that mentions `_time` may assign it, for example with
  // `eval _time=strftime(_time, ...)`, after which a comparison with epoch
  // seconds no longer holds.
  auto settings = SplRenderer::Settings{
    .push_time = not search.contains("_time"),
    .inference = inference,
    .formatted = stages.formatted,
  };
  return with_all_fields(plan_trailing_stages(
    SplDialect{false, settings}, search, filter, projection,
    stages.limitable ? limit : None{}, None{}, reg));
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

auto plan_spl_indexes(std::span<std::string const> indexes,
                      ir::OptimizeFilter const& filter,
                      Option<ir::OptimizeProjection> const& projection,
                      Option<uint64_t> limit, bool inference,
                      registry const& reg) -> Option<PipePlan> {
  TRY(auto search, spl_index_search(indexes));
  auto settings
    = SplRenderer::Settings{.push_time = true, .inference = inference};
  return with_all_fields(plan_trailing_stages(SplDialect{true, settings},
                                              search, filter, projection, limit,
                                              None{}, reg));
}

auto plan_spl_aggregation(std::string_view search,
                          ir::OptimizeAggregation const& aggregation, bool raw)
  -> Option<std::string> {
  // The source sends the search again without the stages if their result
  // might differ from `summarize`, which would repeat a side effect.
  auto stages = spl_stages(search);
  if (not stages.accepted or not stages.limitable or stages.formatted
      or not stages.keeps_fields) {
    return None{};
  }
  TRY(auto plan, classify(aggregation));
  auto evals = std::vector<std::string>{};
  // The count of results gives `stats` a function also for a summary of
  // group keys only.
  auto functions
    = std::vector<std::string>{fmt::format("count as {}", spl_count_name)};
  auto keys = std::vector<std::string>{};
  for (auto i = size_t{0}; i < plan.columns.size(); ++i) {
    auto name = spl_aggregate_name(i);
    // Adds `min(input)` or `max(input)` over the integers of `input`.
    auto extremum
      = [&](std::string_view function, std::vector<std::string> const& input) {
          // With `raw`, `summarize` sees strings, which `min` and `max` reject.
          auto field = eval_input(input);
          if (raw or not field) {
            return false;
          }
          auto integer = fmt::format("mvcount({0})==1 AND match({0}, {1})",
                                     *field, spl_integer_pattern);
          auto check = spl_check_name(i);
          evals.push_back(fmt::format("{}=if({}, tonumber({}), null())", name,
                                      integer, *field));
          evals.push_back(fmt::format("{}=if(isnull({}) OR ({}), 0, 1)", check,
                                      *field, integer));
          functions.push_back(fmt::format("{1}({0}) as {0}", name, function));
          functions.push_back(fmt::format("max({0}) as {0}", check));
          return true;
        };
    auto added = match(
      plan.columns[i].kind,
      [&](AggregationPlan::Key const& key) {
        auto field = eval_input(key.input);
        if (not field) {
          return false;
        }
        evals.push_back(
          fmt::format(R"({0}=case(isnull({1}), "n", mvcount({1})!=1, "m", )"
                      R"(match({1}, "\x00"), "z", true(), "v" . {1}))",
                      name, *field));
        keys.push_back(std::move(name));
        return true;
      },
      [&](AggregationPlan::Count const& count) {
        if (not count.input) {
          return true;
        }
        auto field = eval_input(*count.input);
        if (not field) {
          return false;
        }
        evals.push_back(
          fmt::format("{}=if(isnull({}), null(), 1)", name, *field));
        functions.push_back(fmt::format("count({0}) as {0}", name));
        return true;
      },
      [&](AggregationPlan::Min const& min) {
        return extremum("min", min.input);
      },
      [&](AggregationPlan::Max const& max) {
        return extremum("max", max.input);
      });
    if (not added) {
      return None{};
    }
  }
  auto result = std::string{search};
  if (not evals.empty()) {
    result += fmt::format("\n| eval {}", fmt::join(evals, ", "));
  }
  result += fmt::format("\n| stats {}", fmt::join(functions, ", "));
  if (not keys.empty()) {
    result += fmt::format(" by {}", fmt::join(keys, ", "));
  }
  result += fmt::format("\n| appendpipe [stats count as {}]", spl_groups_name);
  return result;
}

auto decode_spl_aggregation(std::span<nova::Array<nova::Record> const> rows,
                            ir::OptimizeAggregation const& aggregation,
                            bool raw, diagnostic_handler& dh)
  -> Result<std::vector<nova::Events>, std::string> {
  auto plan = classify(aggregation);
  TENZIR_ASSERT(plan);
  auto const& columns = plan->columns;
  // The cells of a row: the value of each column, the check of each `min` and
  // `max`, and the count of results.
  auto values = std::vector<Option<std::string_view>>(columns.size());
  auto checks = std::vector<Option<std::string_view>>(columns.size());
  auto results = Option<std::string_view>{};
  auto cells = std::unordered_map<std::string, Ref<Option<std::string_view>>>{};
  cells.emplace(spl_count_name, results);
  for (auto i = size_t{0}; i < columns.size(); ++i) {
    match(
      columns[i].kind,
      [&](AggregationPlan::Key const&) {
        cells.emplace(spl_aggregate_name(i), values[i]);
      },
      [&](AggregationPlan::Count const& count) {
        if (count.input) {
          cells.emplace(spl_aggregate_name(i), values[i]);
        }
      },
      [&](auto const&) {
        cells.emplace(spl_aggregate_name(i), values[i]);
        cells.emplace(spl_check_name(i), checks[i]);
      });
  }
  auto const grouped = std::ranges::any_of(columns, [](auto const& column) {
    return is<AggregationPlan::Key>(column.kind);
  });
  auto settings = nova::EventBuilder::Settings{};
  // Infer the types of the keys as for the results of a search.
  settings.raw = raw;
  settings.infer_numbers = true;
  auto builder = nova::EventBuilder::make(std::move(settings), dh);
  if (not builder) {
    return Err{std::string{"failed to initialize the decoder"}};
  }
  // Writes the cells of a group to the next event.
  auto decode_group = [&]() -> Result<void, std::string> {
    auto count = results ? parse_count(*results) : None{};
    if (not count) {
      return Err{std::string{"a count is not a non-negative integer"}};
    }
    if (*count == 0 and grouped) {
      return Err{std::string{"a group contains no results"}};
    }
    auto event = builder->event();
    for (auto i = size_t{0}; i < columns.size(); ++i) {
      auto value = values[i];
      auto output = aggregation_column(i);
      auto decoded = match(
        columns[i].kind,
        [&](AggregationPlan::Key const&) -> Result<void, std::string> {
          if (value == "n") {
            event.field(output).null();
            return {};
          }
          if (value and value->starts_with('v')) {
            auto text = value->substr(1);
            if (not raw and is_relative_time(text)) {
              return Err{std::string{"a group key is a relative time"}};
            }
            event.field(output).data_unparsed(text);
            return {};
          }
          if (value == "m") {
            return Err{std::string{"a group key has several values"}};
          }
          if (value == "z") {
            return Err{std::string{"a group key contains a NUL character"}};
          }
          return Err{std::string{"a group key is invalid"}};
        },
        [&](AggregationPlan::Count const& x) -> Result<void, std::string> {
          if (not x.input) {
            event.field(output).data(*count);
            return {};
          }
          auto present = value ? parse_count(*value) : None{};
          if (not present) {
            return Err{std::string{"a count is not a non-negative integer"}};
          }
          event.field(output).data(*present);
          return {};
        },
        [&](auto const&) -> Result<void, std::string> {
          // `min` or `max`, which hold only if every value of the input is
          // an integer that the stages compare. Every result has a check, so
          // only a group without results has none, which `stats` returns
          // without group keys.
          if (checks[i] != "0" and not(not checks[i] and *count == 0)) {
            return Err{std::string{"a group has a value for `min` or `max` "
                                   "other than an integer"}};
          }
          if (not value) {
            event.field(output).null();
            return {};
          }
          auto integer = parse_integer(*value);
          if (not integer) {
            return Err{std::string{"a result of `min` or `max` is invalid"}};
          }
          event.field(output).data(*integer);
          return {};
        });
      TRY(decoded);
    }
    return {};
  };
  auto batches = std::vector<nova::Events>{};
  auto groups = int64_t{0};
  auto expected_groups = Option<int64_t>{};
  for (auto const& array : rows) {
    for (auto index = nova::storage::Index{0}; index < array.length();
         ++index) {
      if (expected_groups) {
        return Err{std::string{"a row follows the count of groups"}};
      }
      std::ranges::fill(values, None{});
      std::ranges::fill(checks, None{});
      results = None{};
      auto fields = size_t{0};
      for (auto const& [name, value] : array.get(index)) {
        ++fields;
        auto const* text = try_as<nova::RowView<nova::String>>(value);
        if (not text) {
          return Err{std::string{"a field is not a single string"}};
        }
        if (name == spl_groups_name) {
          expected_groups = parse_count(**text);
          if (not expected_groups) {
            return Err{std::string{"the count of groups is invalid"}};
          }
          continue;
        }
        auto cell = cells.find(std::string{name});
        if (cell == cells.end()) {
          return Err{fmt::format("unexpected field `{}`", name)};
        }
        *cell->second = **text;
      }
      if (expected_groups) {
        if (fields != 1) {
          return Err{std::string{"the count of groups has other fields"}};
        }
        continue;
      }
      ++groups;
      TRY(decode_group());
      if (std::cmp_greater_equal(builder->length(),
                                 defaults::import::table_slice_size)) {
        batches.push_back(builder->finish());
      }
    }
  }
  // Splunk answers a search that fails in `eval` with an empty response.
  if (not expected_groups) {
    return Err{std::string{"the result misses the count of groups"}};
  }
  if (groups != *expected_groups) {
    return Err{
      fmt::format("received {} of {} groups", groups, *expected_groups)};
  }
  // Without group keys, `stats` returns a single row, also for no results.
  if (not grouped and groups != 1) {
    return Err{std::string{"expected a single row"}};
  }
  if (builder->length() > 0) {
    batches.push_back(builder->finish());
  }
  TRY(check_distinct_keys(batches, aggregation));
  for (auto& batch : batches) {
    batch = nest_aggregation(std::move(batch), aggregation, dh);
  }
  return batches;
}

} // namespace tenzir::pushdown
