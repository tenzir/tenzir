//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pushdown/pipe.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/pushdown/translate.hpp"
#include "tenzir/tql2/ast.hpp"

#include <algorithm>

namespace tenzir::pushdown {

namespace {

/// The distinct top-level fields of `paths`, in order, or `None` if one of
/// them refers to `this`.
auto top_level_names(ir::OptimizeProjection const& paths)
  -> Option<std::vector<std::string>> {
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
  return names;
}

} // namespace

auto select_within_budget(
  std::span<std::string const> predicates, size_t budget,
  std::function<auto(std::span<std::string const>)->size_t> const& size)
  -> std::vector<std::string> {
  auto result = std::vector<std::string>{};
  for (auto const& predicate : predicates) {
    result.push_back(predicate);
    if (size(result) > budget) {
      // Keep this predicate local only and consider smaller later ones.
      result.pop_back();
    }
  }
  return result;
}

auto plan_trailing_stages(PipeDialect const& dialect, std::string_view query,
                          ir::OptimizeFilter const& filter,
                          Option<ir::OptimizeProjection> const& projection,
                          Option<uint64_t> limit, Option<uint64_t> result_cap,
                          registry const& reg) -> PipePlan {
  auto result = PipePlan{.query = std::string{query}};
  if (not dialect.accepts_trailing_stages(query)) {
    return result;
  }
  auto const budget = dialect.max_appendix();
  auto appendix = std::string{};
  // Filter first, so that the prefilters see every column of the query.
  auto split
    = split_filter(filter, dynamic_columns(filter), dialect.renderer(), reg);
  auto predicates = select_within_budget(
    split.pushed, budget, [&](std::span<std::string const> selection) {
      return dialect.filter_stage(selection).size();
    });
  // A service with a silent cap returns only the first rows of a large
  // result. A filter before that cap would reveal later rows, so the query
  // must reach the filter with the cap applied. A limit keeps the first rows
  // of a sorted result, as the cap does, and arbitrary rows of an unsorted
  // one, which the cap may return as well. Without a spelling of the cap, the
  // filter stays local entirely.
  auto cap = result_cap ? dialect.limit_stage(*result_cap)
                        : Option<std::string>{std::string{}};
  if (not predicates.empty() and cap) {
    appendix += *cap;
    appendix += dialect.filter_stage(predicates);
    result.filtered = true;
  }
  // Project after filtering, retaining every field that the local filter
  // reads, since all of it runs on the projected rows.
  auto retained = projection;
  for (auto const& expr : filter) {
    ir::add_refs_to_projection(retained, expr);
  }
  if (retained) {
    if (auto names = top_level_names(*retained); names and not names->empty()) {
      if (auto stage = dialect.project_stage(*names);
          stage and appendix.size() + stage->size() <= budget) {
        appendix += *stage;
        result.projected = true;
      }
    }
  }
  // Every predicate stays local, so a limit is only safe without one.
  if (auto n = pushable_limit(filter, limit)) {
    if (auto stage = dialect.limit_stage(*n)) {
      appendix += *stage;
      result.limited = true;
    }
  }
  result.query += appendix;
  return result;
}

} // namespace tenzir::pushdown
