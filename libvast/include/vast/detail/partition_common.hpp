//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

// The functions in this namespace take PartitionState as template argument
// because the impelementation is the same for passive and active partitions.

#include "vast/system/active_partition.hpp"
#include "vast/system/actors.hpp"
#include "vast/system/passive_partition.hpp"
#include "vast/time_synopsis.hpp"
#include "vast/type.hpp"

namespace vast::detail {

/// Gets the INDEXER at position in the layout.
/// @relates active_partition_state
/// @relates passive_partition_state
template <typename PartitionState>
system::indexer_actor
fetch_indexer(const PartitionState& state, const data_extractor& dx,
              relational_operator op, const data& x) {
  VAST_TRACE_SCOPE("{} {} {}", VAST_ARG(dx), VAST_ARG(op), VAST_ARG(x));
  return state.indexer_at(dx.column);
}

/// Retrieves an INDEXER for a predicate with a data extractor.
/// @param dx The extractor.
/// @param op The operator (only used to precompute ids for type queries.
/// @param x The literal side of the predicate.
/// @relates active_partition_state
/// @relates passive_partition_state
template <typename PartitionState>
system::indexer_actor
fetch_indexer(const PartitionState& state, const selector& ex,
              relational_operator op, const data& x) {
  VAST_TRACE_SCOPE("{} {} {}", VAST_ARG(ex), VAST_ARG(op), VAST_ARG(x));
  ids row_ids;
  if (ex.kind == selector::type) {
    // We know the answer immediately: all IDs that are part of the table.
    // However, we still have to "lift" this result into an actor for the
    // EVALUATOR.
    for (auto& [name, ids] : state.type_ids()) {
      if (evaluate(name, op, x))
        row_ids |= ids;
    }
  } else if (ex.kind == selector::import_time) {
    // For a passive partition, this already went through a time synopsis in
    // the catalog, but for the active partition we create an ad-hoc time
    // synopsis here to do the lookup.
    if constexpr (std::is_same_v<PartitionState,
                                 system::active_partition_state>) {
      if (const auto* t = caf::get_if<time>(&x)) {
        auto ts = time_synopsis{
          state.data.synopsis->min_import_time,
          state.data.synopsis->max_import_time,
        };
        auto add = ts.lookup(op, *t);
        if (!add || *add)
          for (const auto& [_, ids] : state.type_ids())
            row_ids |= ids;
      }
    } else {
      for (const auto& [_, ids] : state.type_ids())
        row_ids |= ids;
    }

  } else if (ex.kind == selector::field) {
    auto s = caf::get_if<std::string>(&x);
    if (!s) {
      VAST_WARN("{} #field meta queries only support string "
                "comparisons",
                *state.self);
      return {};
    }
    auto neg = is_negated(op);
    auto layout = *state.combined_layout();
    // data s -> string, rhs in #field query
    for (const auto& [field, offset] : layout.leaves()) {
      const auto key = layout.key(offset);
      const auto [s_mismatch, key_mismatch]
        = std::mismatch(s->rbegin(), s->rend(), key.rbegin(), key.rend());
      if (s_mismatch == s->rend()
          && (key_mismatch == key.rend() || *key_mismatch == '.')) {
        for (const auto& [layout_name, ids] : state.type_ids()) {
          const auto [layout_name_mismatch, key_mismatch] = std::mismatch(
            layout_name.begin(), layout_name.end(), key.begin(), key.end());
          if (layout_name_mismatch == layout_name.end()
              && (key_mismatch == key.end() || *key_mismatch == '.')) {
            row_ids |= ids;
          }
        }
      }
    }
    if (neg) {
      auto partition_ids
        = std::accumulate(state.type_ids().begin(), state.type_ids().end(),
                          ids{}, [](ids acc, const auto& x) {
                            return acc | x.second;
                          });
      row_ids = partition_ids ^ row_ids;
    }
  } else {
    VAST_WARN("{} got unsupported attribute: {}", *state.self, ex.kind);
    return {};
  }
  // TODO: Spawning a one-shot actor is quite expensive. Maybe the
  //       partition could instead maintain this actor lazily.
  return state.self->spawn([row_ids]() -> system::indexer_actor::behavior_type {
    return {
      [=](const curried_predicate&) {
        return row_ids;
      },
      [](atom::shutdown) {
        VAST_DEBUG("one-shot indexer received shutdown request");
      },
    };
  });
}

/// Returns all INDEXERs that are involved in evaluating the expression.
/// @relates active_partition_state
/// @relates passive_partition_state
template <typename PartitionState>
std::vector<system::evaluation_triple>
evaluate(const PartitionState& state, const expression& expr) {
  auto combined_layout = state.combined_layout();
  if (!combined_layout) {
    // The partition may not have a combined layout yet, simply because it does
    // not have any events yet. This is not an error, so we simply return an
    // empty set of evaluation triples here.
    VAST_DEBUG("{} cannot evaluate expression because it has no layout",
               *state.self);
    return {};
  }
  // Pretend the partition is a table, and return fitted predicates for the
  // partitions layout.
  // TODO: Should resolve take a record_type directly?
  std::vector<system::evaluation_triple> result;
  auto resolved = resolve(expr, type{*combined_layout});
  for (auto& kvp : resolved) {
    // For each fitted predicate, look up the corresponding INDEXER
    // according to the specified type of extractor.
    auto& pred = kvp.second;
    auto get_indexer_handle = [&](const auto& ext, const data& x) {
      return fetch_indexer(state, ext, pred.op, x);
    };
    auto v = detail::overload{
      [&](const selector& ex, const data& x) {
        return get_indexer_handle(ex, x);
      },
      [&](const data_extractor& dx, const data& x) {
        return get_indexer_handle(dx, x);
      },
      [](const auto&, const auto&) {
        return system::indexer_actor{}; // clang-format fix
      },
    };
    // Package the predicate, its position in the query and the required
    // INDEXER as a "job description".
    if (auto hdl = caf::visit(v, pred.lhs, pred.rhs))
      result.emplace_back(kvp.first, curried(pred), std::move(hdl));
  }
  // Return the list of jobs, to be used by the EVALUATOR.
  return result;
}

} // namespace vast::detail
