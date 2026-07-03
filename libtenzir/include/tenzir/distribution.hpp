//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/tql2/ast.hpp"
#include "tenzir/variant.hpp"

#include <vector>

namespace tenzir {

/// The input distribution an operator requires from its upstream edge.
///
/// This is the edge-centric declaration behind implicit parallelization: an
/// operator states what distribution of the input stream it can correctly
/// consume, and the planner inserts exchanges wherever the current distribution
/// does not satisfy the next requirement.
///
/// The alternatives form a lattice ordered by restrictiveness:
///
///   AnyDistribution < HashDistribution{k} < SingleDistribution
///
/// where `AnyDistribution` is the least restrictive (any subset of events
/// works) and `SingleDistribution` is the most restrictive (one instance must
/// observe the entire stream). See `meet` for how requirements combine.

/// Stateless / row-local: any subset of events works (e.g. where). An operator
/// declaring this can run at any parallelism degree behind a round-robin
/// scatter.
struct AnyDistribution {
  friend auto operator==(const AnyDistribution&, const AnyDistribution&) -> bool
    = default;
};

/// The operator must observe the full stream (e.g. decuplicate, to_file).
/// This is the conservative default: nothing is parallelized until an
/// operator opts into a weaker requirement.
struct SingleDistribution {
  friend auto operator==(const SingleDistribution&, const SingleDistribution&)
    -> bool
    = default;
};

/// The operator must observe all events for a given key (e.g. `group`).
/// Carries the key expressions that define the partitioning.
struct HashDistribution {
  std::vector<ast::expression> keys;
};

/// @see AnyDistribution, SingleDistribution, HashDistribution
using Distribution
  = variant<AnyDistribution, SingleDistribution, HashDistribution>;

/// Returns whether two hash distributions partition on the same key spec.
///
/// This is a best-effort structural comparison. When equality cannot be
/// established, it conservatively returns `false` (treating the keys as
/// different), which the planner resolves by narrowing to
/// `SingleDistribution` — always correct, occasionally pessimistic.
auto same_hash_keys(const HashDistribution& lhs, const HashDistribution& rhs)
  -> bool;

/// Combines two requirements into the least requirement that satisfies both.
///
/// Despite the lattice-theoretic name, this returns the *more restrictive* of
/// the two, which is what a subpipeline host needs when folding the
/// requirements of its child pipelines: the host can only promise `Any` if
/// every child can consume `Any`.
///
///   meet(Any, x)          == x
///   meet(Single, x)       == Single
///   meet(Hash{k}, Any)    == Hash{k}
///   meet(Hash{k}, Hash{k})== Hash{k}       (same keys)
///   meet(Hash{k}, Hash{j})== Single        (different keys)
///   meet(Hash{k}, Single) == Single
auto meet(const Distribution& lhs, const Distribution& rhs) -> Distribution;

/// Returns whether `available` satisfies the `required` distribution, i.e. an
/// operator requiring `required` can correctly consume a stream that is
/// currently distributed as `available`.
///
///   satisfies(available, Any)      == true      (any input works)
///   satisfies(Single,    Single)   == true
///   satisfies(Any,       Single)   == false     (needs a gather)
///   satisfies(Hash{k},   Hash{k})  == true      (same keys)
///   satisfies(Hash{k},   Hash{j})  == false     (needs a rekey)
auto satisfies(const Distribution& available, const Distribution& required)
  -> bool;

} // namespace tenzir
