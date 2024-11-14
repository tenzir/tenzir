//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

// The functions in this namespace take PartitionState as template argument
// because the impelementation is the same for passive and active partitions.

#include "tenzir/active_partition.hpp"
#include "tenzir/actors.hpp"
#include "tenzir/expression.hpp"
#include "tenzir/passive_partition.hpp"
#include "tenzir/time_synopsis.hpp"
#include "tenzir/type.hpp"

namespace tenzir::detail {

/// Gets the INDEXER at position in the schema.
/// @relates active_partition_state
/// @relates passive_partition_state
template <typename PartitionState>
indexer_actor
fetch_indexer(const PartitionState& state, const data_extractor& dx,
              relational_operator op, const data& x) {
  TENZIR_TRACE_SCOPE("{} {} {}", TENZIR_ARG(dx), TENZIR_ARG(op), TENZIR_ARG(x));
  return state.indexer_at(dx.column);
}

/// Retrieves an INDEXER for a predicate with a data extractor.
/// @param dx The extractor.
/// @param op The operator (only used to precompute ids for type queries.
/// @param x The literal side of the predicate.
/// @relates active_partition_state
/// @relates passive_partition_state
template <typename PartitionState>
indexer_actor
fetch_indexer(const PartitionState& state, const meta_extractor& ex,
              relational_operator op, const data& x) {
  TENZIR_TRACE_SCOPE("{} {} {}", TENZIR_ARG(ex), TENZIR_ARG(op), TENZIR_ARG(x));
  ids row_ids;
  if (ex.kind == meta_extractor::schema) {
    // We know the answer immediately: all IDs that are part of the table.
    // However, we still have to "lift" this result into an actor for the
    // EVALUATOR.
    for (auto& [name, ids] : state.type_ids()) {
      if (evaluate(name, op, x))
        row_ids |= ids;
    }
  } else if (ex.kind == meta_extractor::schema_id) {
    // TODO: Actually take the schema fingerprint into account. For now, we just
    // return all stored ids.
    for (const auto& [_, ids] : state.type_ids()) {
      row_ids |= ids;
    }
  } else if (ex.kind == meta_extractor::import_time) {
    // For a passive partition, this already went through a time synopsis in
    // the catalog, but for the active partition we create an ad-hoc time
    // synopsis here to do the lookup.
    if constexpr (std::is_same_v<PartitionState, active_partition_state>) {
      if (const auto* t = try_as<time>(&x)) {
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
  } else if (ex.kind == meta_extractor::internal) {
    // TODO: Actually take the internal flag into account. For now, we just
    // return all stored ids.
    for (const auto& [_, ids] : state.type_ids()) {
      row_ids |= ids;
    }
  } else {
    TENZIR_WARN("{} got unsupported attribute: {}", *state.self, ex.kind);
    return {};
  }
  // TODO: Spawning a one-shot actor is quite expensive. Maybe the
  //       partition could instead maintain this actor lazily.
  return state.self->spawn([row_ids]() -> indexer_actor::behavior_type {
    return {
      [=](atom::evaluate, const curried_predicate&) {
        return row_ids;
      },
      [](atom::shutdown) {
        TENZIR_DEBUG("one-shot indexer received shutdown request");
      },
    };
  });
}

/// Returns all INDEXERs that are involved in evaluating the expression.
/// @relates active_partition_state
/// @relates passive_partition_state
template <typename PartitionState>
std::vector<evaluation_triple>
evaluate(const PartitionState& state, const expression& expr) {
  auto combined_schema = state.combined_schema();
  if (!combined_schema) {
    // The partition may not have a combined schema yet, simply because it does
    // not have any events yet. This is not an error, so we simply return an
    // empty set of evaluation triples here.
    TENZIR_DEBUG("{} cannot evaluate expression because it has no schema",
                 *state.self);
    return {};
  }
  // Pretend the partition is a table, and return fitted predicates for the
  // partitions schema.
  // TODO: Should resolve take a record_type directly?
  std::vector<evaluation_triple> result;
  auto resolved = resolve(expr, type{*combined_schema});
  for (auto& [offset, predicate] : resolved) {
    // For each fitted predicate, look up the corresponding INDEXER
    // according to the specified type of extractor.
    auto get_indexer_handle
      = [&, &pred = predicate](const auto& ext, const data& x) {
          return fetch_indexer(state, ext, pred.op, x);
        };
    auto v = detail::overload{
      [&](const meta_extractor& ex, const data& x) {
        return get_indexer_handle(ex, x);
      },
      [&](const data_extractor& dx, const data& x) {
        return get_indexer_handle(dx, x);
      },
      [](const auto&, const auto&) {
        return indexer_actor{}; // clang-format fix
      },
    };
    // Package the predicate, its position in the query and the required
    // INDEXER as a "job description". INDEXER can be nullptr
    auto hdl = match(std::tie(predicate.lhs, predicate.rhs), v);
    result.emplace_back(std::move(offset), curried(predicate), std::move(hdl));
  }
  // Return the list of jobs, to be used by the EVALUATOR.
  return result;
}

/// Evaluator requires all ids for a given partition when no indexers are used.
/// This is a helper function to extract them when needed.
/// @param type_ids mapping of partition schema name to it's ids.
/// @param evaluation_triples contains indexers used for evaluation.
/// @returns all ids from type_ids if any evaluation_triple doesn't have an
/// indexer. Returns default initialized ids{} otherwise.
ids get_ids_for_evaluation(
  const std::unordered_map<std::string, ids>& type_ids,
  const std::vector<evaluation_triple>& evaluation_triples);

} // namespace tenzir::detail
