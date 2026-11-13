//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "cloudwatch/request_plan.hpp"

#include "cloudwatch/operators.hpp"

#include <tenzir/pushdown/sql_source.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/try.hpp>
#include <tenzir/variant.hpp>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace tenzir::plugins::cloudwatch {

auto epoch_ms(time value) -> int64_t {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
           value.time_since_epoch())
    .count();
}

namespace {

/// Live Tail and `FilterLogEvents` accept at most 100 log stream names.
constexpr auto max_stream_names = size_t{100};

/// `FilterLogEvents` accepts log stream names of at most 512 characters.
/// Counting bytes keeps within the limit also for names beyond ASCII.
constexpr auto max_stream_name_length = size_t{512};

/// `FilterLogEvents` and Live Tail accept filter patterns of at most 1,024
/// characters. Counting bytes keeps within the limit also for patterns
/// beyond ASCII.
constexpr auto max_pattern_length = size_t{1024};

constexpr auto ns_per_ms = int64_t{1'000'000};

auto floor_div(int64_t x, int64_t y) -> int64_t {
  auto q = x / y;
  return x % y < 0 ? q - 1 : q;
}

auto ceil_div(int64_t x, int64_t y) -> int64_t {
  auto q = x / y;
  return x % y > 0 ? q + 1 : q;
}

/// Returns the name of the top-level field that `expr` reads, if it is one.
auto top_level_field(ast::expression const& expr) -> Option<std::string> {
  TRY(auto path, ast::field_path::try_from(expr));
  if (path.path().size() != 1 or path.path().front().has_question_mark) {
    return None{};
  }
  return std::string{path.path().front().id.name};
}

auto is_field(ast::expression const& expr, std::string_view name) -> bool {
  auto field = top_level_field(expr);
  return field and *field == name;
}

auto string_constant(ast::expression const& expr) -> Option<std::string> {
  auto const* constant = try_as<ast::constant>(expr);
  if (not constant) {
    return None{};
  }
  auto const* text = try_as<std::string>(constant->value);
  if (not text) {
    return None{};
  }
  return *text;
}

/// Returns the strings of a list of string constants, as the optimizer may
/// fold it.
auto string_list(ast::expression const& expr)
  -> Option<std::vector<std::string>> {
  auto result = std::vector<std::string>{};
  if (auto const* xs = try_as<ast::list>(expr)) {
    for (auto const& item : xs->items) {
      auto const* element = try_as<ast::expression>(item);
      if (not element) {
        return None{};
      }
      TRY(auto text, string_constant(*element));
      result.push_back(std::move(text));
    }
    return result;
  }
  auto const* constant = try_as<ast::constant>(expr);
  auto const* xs = constant ? try_as<list>(constant->value) : nullptr;
  if (not xs) {
    return None{};
  }
  for (auto const& element : *xs) {
    auto const* text = try_as<std::string>(element);
    if (not text) {
      return None{};
    }
    result.push_back(*text);
  }
  return result;
}

/// Whether a filter pattern spells `phrase` exactly in double quotes. The
/// grammar is deliberately narrow: no quotes, backslashes, or control
/// characters, and some visible character.
auto is_pattern_phrase(std::string_view phrase) -> bool {
  auto visible = false;
  for (auto c : phrase) {
    auto const u = static_cast<unsigned char>(c);
    if (c == '"' or c == '\\' or u < 0x20 or u == 0x7f) {
      return false;
    }
    visible = visible or c != ' ';
  }
  return visible;
}

/// Returns the phrase of the term `"x" in message`, if `expr` is one.
auto message_term(ast::expression const& expr) -> Option<std::string> {
  auto const* binary = try_as<ast::binary_expr>(expr);
  if (not binary or binary->op != ast::binary_op::in
      or not is_field(binary->right, "message")) {
    return None{};
  }
  TRY(auto phrase, string_constant(binary->left));
  if (not is_pattern_phrase(phrase)) {
    return None{};
  }
  return phrase;
}

/// Collects the operands of a chain of `op`.
auto flatten(ast::expression const& expr, ast::binary_op op,
             std::vector<ast::expression const*>& result) -> void {
  if (auto const* binary = try_as<ast::binary_expr>(expr);
      binary and binary->op == op) {
    flatten(binary->left, op, result);
    flatten(binary->right, op, result);
    return;
  }
  result.push_back(&expr);
}

/// What the filters that a read took over add up to.
struct CloudWatchFilters {
  std::vector<std::string> required;
  std::vector<std::string> excluded;
  std::vector<std::vector<std::string>> optional;
  Option<int64_t> first_ms;
  Option<int64_t> last_ms;
  Option<std::vector<std::string>> streams;
  bool empty = false;

  /// Narrows the window to the events whose timestamp compares with `value`
  /// as `op` does, for events with timestamps in whole milliseconds.
  auto narrow(ast::binary_op op, time value) -> void {
    auto const ns = value.time_since_epoch().count();
    auto const lower = [&](int64_t ms) {
      first_ms = first_ms ? std::max(*first_ms, ms) : ms;
    };
    auto const upper = [&](int64_t ms) {
      last_ms = last_ms ? std::min(*last_ms, ms) : ms;
    };
    using enum ast::binary_op;
    switch (op) {
      case geq:
        lower(ceil_div(ns, ns_per_ms));
        return;
      case gt:
        lower(floor_div(ns, ns_per_ms) + 1);
        return;
      case leq:
        upper(floor_div(ns, ns_per_ms));
        return;
      case lt:
        upper(ceil_div(ns, ns_per_ms) - 1);
        return;
      case eq:
        if (ns % ns_per_ms != 0) {
          empty = true;
          return;
        }
        lower(ns / ns_per_ms);
        upper(ns / ns_per_ms);
        return;
      default:
        TENZIR_UNREACHABLE();
    }
  }

  auto restrict_streams(std::vector<std::string> names) -> void {
    if (not streams) {
      std::ranges::sort(names);
      auto duplicates = std::ranges::unique(names);
      names.erase(duplicates.begin(), duplicates.end());
      streams = std::move(names);
      return;
    }
    std::erase_if(*streams, [&](std::string const& name) {
      return not std::ranges::contains(names, name);
    });
  }

  /// The filter pattern, or `None` without terms. Fails for a combination
  /// that CloudWatch Logs does not match as a conjunction: it ignores
  /// optional terms next to others, and excluded terms need an included one.
  auto pattern() const -> Option<Option<std::string>> {
    if (optional.size() > 1) {
      return None{};
    }
    if (not optional.empty()
        and (not required.empty() or not excluded.empty())) {
      return None{};
    }
    if (not excluded.empty() and required.empty()) {
      return None{};
    }
    auto terms = std::vector<std::string>{};
    for (auto const& phrase : required) {
      terms.push_back(fmt::format("\"{}\"", phrase));
    }
    for (auto const& phrase : excluded) {
      terms.push_back(fmt::format("-\"{}\"", phrase));
    }
    if (not optional.empty()) {
      for (auto const& phrase : optional.front()) {
        terms.push_back(fmt::format("?\"{}\"", phrase));
      }
    }
    if (terms.empty()) {
      return Option<std::string>{None{}};
    }
    return Option<std::string>{fmt::to_string(fmt::join(terms, " "))};
  }
};

/// Adds the conjunct `expr` of a filter to `filters`, or returns `false` if
/// CloudWatch Logs cannot compute it in `mode`.
auto add_conjunct(FromCloudWatchArgs const& args, FromMode mode,
                  ast::expression const& expr, CloudWatchFilters& filters)
  -> bool {
  auto const patterns = mode != FromMode::replay and not args.filter;
  if (auto phrase = message_term(expr)) {
    if (not patterns) {
      return false;
    }
    filters.required.push_back(std::move(*phrase));
    return true;
  }
  if (auto const* unary = try_as<ast::unary_expr>(expr);
      unary and unary->op == ast::unary_op::not_) {
    auto phrase = message_term(unary->expr);
    if (not phrase or not patterns) {
      return false;
    }
    filters.excluded.push_back(std::move(*phrase));
    return true;
  }
  auto const* binary = try_as<ast::binary_expr>(expr);
  if (not binary) {
    return false;
  }
  using enum ast::binary_op;
  if (binary->op == or_) {
    auto operands = std::vector<ast::expression const*>{};
    flatten(expr, or_, operands);
    auto group = std::vector<std::string>{};
    for (auto const* operand : operands) {
      auto phrase = message_term(*operand);
      if (not phrase) {
        return false;
      }
      group.push_back(std::move(*phrase));
    }
    if (not patterns) {
      return false;
    }
    filters.optional.push_back(std::move(group));
    return true;
  }
  if (binary->op == in) {
    if (mode != FromMode::search or args.stream or args.stream_prefix
        or not is_field(binary->left, "log_stream")) {
      return false;
    }
    auto names = string_list(binary->right);
    if (not names or names->empty()) {
      return false;
    }
    filters.restrict_streams(std::move(*names));
    return true;
  }
  if (binary->op != eq and binary->op != lt and binary->op != leq
      and binary->op != gt and binary->op != geq) {
    return false;
  }
  // Put the field on the left.
  auto op = binary->op;
  auto const* field = &binary->left;
  auto const* value = &binary->right;
  if (not top_level_field(*field)) {
    std::swap(field, value);
    switch (op) {
      case lt:
        op = gt;
        break;
      case leq:
        op = geq;
        break;
      case gt:
        op = lt;
        break;
      case geq:
        op = leq;
        break;
      default:
        break;
    }
  }
  auto name = top_level_field(*field);
  if (not name) {
    return false;
  }
  if (*name == "timestamp") {
    auto const* constant = try_as<ast::constant>(*value);
    auto const* instant = constant ? try_as<time>(constant->value) : nullptr;
    if (not instant or mode == FromMode::live) {
      return false;
    }
    filters.narrow(op, *instant);
    return true;
  }
  if (*name == "log_stream" and op == eq) {
    if (mode != FromMode::search or args.stream or args.stream_prefix) {
      return false;
    }
    auto stream = string_constant(*value);
    if (not stream) {
      return false;
    }
    filters.restrict_streams({std::move(*stream)});
    return true;
  }
  return false;
}

auto valid_stream_name(std::string_view name) -> bool {
  return not name.empty() and name.size() <= max_stream_name_length
         and name.find_first_of(":*") == std::string::npos;
}

} // namespace

auto from_mode(FromCloudWatchArgs const& args) -> FromMode {
  if (args.mode.inner == "search") {
    return FromMode::search;
  }
  if (args.mode.inner == "replay") {
    return FromMode::replay;
  }
  return FromMode::live;
}

namespace {

/// Spells `stages` like `plan_cloudwatch_request`, but only checks the length
/// of the filter pattern if `limit_pattern` is set.
auto plan_request(FromCloudWatchArgs const& args,
                  std::span<ir::QueryStage const> stages, bool limit_pattern)
  -> Option<CloudWatchRequest> {
  auto const mode = from_mode(args);
  auto filters = CloudWatchFilters{};
  auto limit = Option<uint64_t>{};
  if (args.count) {
    limit = args.count->inner;
  }
  auto limited = false;
  for (auto const& stage : stages) {
    // The user's `count` limits the events before the stages after the
    // operator, and a limit those of the stages before it, so that only
    // further limits follow either.
    if ((args.count or limited) and not is<ir::QueryStage::Limit>(stage.kind)) {
      return None{};
    }
    auto ok = match(
      stage.kind,
      [&](ir::QueryStage::Filter const& x) {
        auto conjuncts = std::vector<ast::expression const*>{};
        flatten(x.predicate, ast::binary_op::and_, conjuncts);
        return std::ranges::all_of(conjuncts, [&](auto const* conjunct) {
          return add_conjunct(args, mode, *conjunct, filters);
        });
      },
      [&](ir::QueryStage::Limit const& x) {
        if (mode == FromMode::live) {
          return false;
        }
        limit = limit ? std::min(*limit, x.count) : x.count;
        limited = true;
        return true;
      },
      [](auto const&) {
        return false;
      });
    if (not ok) {
      return None{};
    }
  }
  TRY(auto pattern, filters.pattern());
  if (limit_pattern and pattern and pattern->size() > max_pattern_length) {
    return None{};
  }
  auto result = CloudWatchRequest{};
  result.pattern = std::move(pattern);
  result.limit = limit;
  result.empty = filters.empty;
  if (filters.streams) {
    if (filters.streams->size() > max_stream_names
        or not std::ranges::all_of(*filters.streams, valid_stream_name)) {
      return None{};
    }
    result.empty = result.empty or filters.streams->empty();
    result.streams = std::move(filters.streams);
  }
  // Timestamps are not negative, which the requests reject anyway.
  auto first_ms = Option<int64_t>{};
  if (filters.first_ms and *filters.first_ms > 0) {
    first_ms = filters.first_ms;
  }
  if (filters.last_ms and *filters.last_ms < 0) {
    result.empty = true;
  }
  // The window of the filters narrows the user's bounds, which are inclusive
  // except for the end of `GetLogEvents`.
  auto const exclusive_end = mode == FromMode::replay;
  if (args.start) {
    result.start_time = epoch_ms(args.start->inner);
  }
  if (first_ms) {
    result.start_time
      = result.start_time ? std::max(*result.start_time, *first_ms) : *first_ms;
  }
  if (args.end) {
    result.end_time = epoch_ms(args.end->inner);
  }
  if (filters.last_ms) {
    auto const end = exclusive_end ? *filters.last_ms + 1 : *filters.last_ms;
    result.end_time = result.end_time ? std::min(*result.end_time, end) : end;
  }
  if (first_ms or filters.last_ms) {
    auto first = first_ms.value_or(0);
    auto last = filters.last_ms.value_or(std::numeric_limits<int64_t>::max());
    if (args.start) {
      first = std::max(first, epoch_ms(args.start->inner));
    }
    if (args.end) {
      auto const end = epoch_ms(args.end->inner);
      last = std::min(last, exclusive_end ? end - 1 : end);
    }
    result.empty = result.empty or first > last;
  }
  return result;
}

} // namespace

auto plan_cloudwatch_request(FromCloudWatchArgs const& args,
                             std::span<ir::QueryStage const> stages)
  -> Option<CloudWatchRequest> {
  return plan_request(args, stages, true);
}

auto absorb_cloudwatch_stages(FromCloudWatchArgs const& args,
                              ir::QueryChain const& chain) -> ir::Absorption {
  auto const mode = from_mode(args);
  auto result = pushdown::absorb_prefix(
    chain,
    [&](std::span<ir::QueryStage const> stages) {
      return plan_cloudwatch_request(args, stages).is_some();
    },
    {
      .target = "CloudWatch Logs",
      .filters_only = false,
      .veto = [&](ir::QueryStage const& stage) -> Option<std::string> {
        auto const is_filter = is<ir::QueryStage::Filter>(stage.kind);
        auto const is_limit = is<ir::QueryStage::Limit>(stage.kind);
        if (mode == FromMode::live and not is_filter) {
          return std::string{"a live read takes over filters only"};
        }
        if (args.count and not is_limit) {
          return std::string{"`count` limits the events before the stage"};
        }
        if (is_filter or is_limit) {
          return None{};
        }
        if (is<ir::QueryStage::Sort>(stage.kind)) {
          return std::string{"CloudWatch Logs returns events in its own "
                             "order"};
        }
        if (is<ir::QueryStage::Aggregate>(stage.kind)) {
          return std::string{"CloudWatch Logs reads events without "
                             "aggregating them"};
        }
        return std::string{"CloudWatch Logs returns a fixed set of fields"};
      },
    });
  // Name the length of the filter pattern if it is what rules out the next
  // stage, which then stays in Tenzir together with the stages after it.
  if (result.prefix < chain.stages.size()) {
    auto const next = std::span{chain.stages}.first(result.prefix + 1);
    if (not plan_request(args, next, true)
        and plan_request(args, next, false)) {
      result.declined
        = fmt::format("the filter pattern would exceed the limit of {} "
                      "characters of CloudWatch Logs",
                      max_pattern_length);
    }
  }
  return result;
}

} // namespace tenzir::plugins::cloudwatch
