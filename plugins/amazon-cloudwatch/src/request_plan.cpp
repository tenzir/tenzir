//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "cloudwatch/request_plan.hpp"

#include "cloudwatch/operators.hpp"

#include <tenzir/pushdown/time_window.hpp>
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

/// Returns whether `expr` reads the top-level field `name`.
auto is_field(ast::expression const& expr, std::string_view name) -> bool {
  auto path = ir::field_segments(expr);
  return path and path->size() == 1 and path->front() == name;
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

/// What the filters that a read took over add up to.
struct CloudWatchFilters {
  std::vector<std::string> required;
  std::vector<std::string> excluded;
  std::vector<std::vector<std::string>> optional;
  Option<std::vector<std::string>> streams;
  bool empty = false;

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
    // Every operand of a chain of `or` is a term of the message.
    auto group = std::vector<std::string>{};
    auto const terms
      = [&](this auto const& self, ast::expression const& x) -> bool {
      auto const* inner = try_as<ast::binary_expr>(x);
      if (inner and inner->op == or_) {
        return self(inner->left) and self(inner->right);
      }
      auto phrase = message_term(x);
      if (not phrase) {
        return false;
      }
      group.push_back(std::move(*phrase));
      return true;
    };
    if (not terms(expr) or not patterns) {
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
  auto const op = binary->op;
  auto const* field = &binary->left;
  auto const* value = &binary->right;
  if (not ir::field_segments(*field)) {
    std::swap(field, value);
  }
  if (is_field(*field, "timestamp")) {
    auto const* constant = try_as<ast::constant>(*value);
    auto const* instant = constant ? try_as<time>(constant->value) : nullptr;
    if (not instant or mode == FromMode::live) {
      return false;
    }
    // The window of the filters spans the timestamps that they compare with.
    return true;
  }
  if (is_field(*field, "log_stream") and op == eq) {
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

auto cloudwatch_planner(FromCloudWatchArgs args)
  -> pushdown::Planner<CloudWatchRequest> {
  return {
    .target = "CloudWatch Logs",
    .plan = [args = std::move(args)](
              std::span<ir::QueryStage const> stages,
              std::monostate const*) -> Result<CloudWatchRequest, std::string> {
      auto const declined
        = std::string{"CloudWatch Logs cannot spell the stage"};
      auto const mode = from_mode(args);
      auto filters = CloudWatchFilters{};
      auto limit = args.count ? Option<uint64_t>{args.count->inner} : None{};
      auto limited = false;
      auto window = pushdown::TimeWindow{};
      for (auto const& stage : stages) {
        auto const is_filter = is<ir::QueryStage::Filter>(stage.kind);
        auto const is_limit = is<ir::QueryStage::Limit>(stage.kind);
        if (mode == FromMode::live and not is_filter) {
          return Err{std::string{"a live read takes over filters only"}};
        }
        // The user's `count` limits the events before the stages after the
        // operator, and a limit those of the stages before it, so that only
        // further limits follow either.
        if (args.count and not is_limit) {
          return Err{std::string{"`count` limits the events before the stage"}};
        }
        auto const after_limit = limited and not is_limit;
        auto reason = match(
          stage.kind,
          [&](ir::QueryStage::Filter const& x) -> Option<std::string> {
            window = pushdown::intersect(
              window, pushdown::time_window(x.predicate, "timestamp"));
            if (std::ranges::all_of(ir::split_conjunction(x.predicate),
                                    [&](ast::expression const& conjunct) {
                                      return add_conjunct(args, mode, conjunct,
                                                          filters);
                                    })) {
              return None{};
            }
            return declined;
          },
          [&](ir::QueryStage::Limit const& x) -> Option<std::string> {
            limit = limit ? std::min(*limit, x.count) : x.count;
            limited = true;
            return None{};
          },
          [](ir::QueryStage::Sort const&) -> Option<std::string> {
            return "CloudWatch Logs returns events in its own order";
          },
          [](ir::QueryStage::Aggregate const&) -> Option<std::string> {
            return "CloudWatch Logs reads events without aggregating them";
          },
          [](auto const&) -> Option<std::string> {
            return "CloudWatch Logs returns a fixed set of fields";
          });
        if (reason or after_limit) {
          return Err{reason.value_or(declined)};
        }
      }
      auto pattern = filters.pattern();
      if (not pattern) {
        return Err{declined};
      }
      if (*pattern and (*pattern)->size() > max_pattern_length) {
        return Err{fmt::format("the filter pattern would exceed the limit of "
                               "{} characters of CloudWatch Logs",
                               max_pattern_length)};
      }
      auto result = CloudWatchRequest{};
      result.pattern = std::move(*pattern);
      result.limit = limit;
      result.empty = filters.empty;
      if (filters.streams) {
        if (filters.streams->size() > max_stream_names
            or not std::ranges::all_of(*filters.streams, valid_stream_name)) {
          return Err{declined};
        }
        result.empty = result.empty or filters.streams->empty();
        result.streams = std::move(filters.streams);
      }
      // Events have timestamps in whole milliseconds, which are not negative,
      // as the requests reject. The window of the filters narrows the user's
      // bounds, which are inclusive except for the end of `GetLogEvents`.
      auto const exclusive_end = mode == FromMode::replay;
      auto [first, last] = pushdown::in_units(
        window, std::chrono::milliseconds{1}, pushdown::Round::inward);
      if (first and *first <= 0) {
        first = None{};
      }
      if (args.start) {
        first = std::max(first.value_or(std::numeric_limits<int64_t>::min()),
                         epoch_ms(args.start->inner));
      }
      if (args.end) {
        last = std::min(last.value_or(std::numeric_limits<int64_t>::max()),
                        epoch_ms(args.end->inner) - (exclusive_end ? 1 : 0));
      }
      result.start_time = first;
      result.end_time = last.transform([&](int64_t x) {
        return exclusive_end ? x + 1 : x;
      });
      if (window.begin or window.end) {
        result.empty
          = result.empty
            or std::max(first.value_or(0), int64_t{0})
                 > last.value_or(std::numeric_limits<int64_t>::max());
      }
      return result;
    },
  };
}

} // namespace tenzir::plugins::cloudwatch
