//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/detail/flat_map.hpp"
#include "tenzir/detail/request_cache.hpp"
#include "tenzir/expression.hpp"
#include "tenzir/partition_synopsis.hpp"
#include "tenzir/taxonomies.hpp"
#include "tenzir/uuid.hpp"

#include <caf/mail_cache.hpp>
#include <caf/response_type.hpp>
#include <caf/settings.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <list>
#include <vector>

namespace tenzir {

/// The result of a catalog query.
struct catalog_lookup_result {
  struct candidate_info {
    expression exp;
    std::vector<partition_info> partition_infos;

    template <class Inspector>
    friend auto inspect(Inspector& f, candidate_info& x) -> bool {
      return f.object(x)
        .pretty_name("tenzir.system.catalog_result.candidate_info")
        .fields(f.field("expression", x.exp),
                f.field("partition-infos", x.partition_infos));
    }
  };

  std::unordered_map<type, candidate_info> candidate_infos;

  auto empty() const noexcept -> bool;

  auto size() const noexcept -> size_t;

  template <class Inspector>
  friend auto inspect(Inspector& f, catalog_lookup_result& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.catalog_lookup_result")
      .fields(f.field("candidate-infos", x.candidate_infos));
  }
};

/// A memory-bounded LRU cache of fully-loaded partition synopses. It serves
/// Bloom-filter pruning for partitions whose sketches were deferred at startup
/// (see `tenzir.index.lazy-sketches`): the authoritative synopses keep their
/// deferred (null) sketches, and the catalog loads them on demand into this
/// cache when a query needs them. Entries are owned solely by the cache and
/// never alias the resident synopses, so eviction is O(1) and cannot disturb
/// resident state.
class sketch_cache {
public:
  sketch_cache() = default;
  explicit sketch_cache(size_t budget_bytes) : budget_{budget_bytes} {
  }

  /// Returns the cached synopsis for `id` without changing its recency, or
  /// nullptr on a miss. Used on the read (lookup) path, which must stay const.
  [[nodiscard]] auto peek(const uuid& id) const -> partition_synopsis_ptr;

  /// Inserts a loaded synopsis and marks it most-recently-used, evicting
  /// least-recently-used entries until the total is within budget. Returns the
  /// number of bytes actually cached, which is zero if the entry alone exceeds
  /// the budget (it is then not cached at all).
  auto put(const uuid& id, partition_synopsis_ptr synopsis) -> size_t;

  /// Removes `id` from the cache if present (used on erase/merge/replace).
  void erase(const uuid& id);

  [[nodiscard]] auto budget() const -> size_t {
    return budget_;
  }
  [[nodiscard]] auto used() const -> size_t {
    return used_;
  }

private:
  struct entry {
    partition_synopsis_ptr synopsis = {};
    size_t bytes = 0;
    std::list<uuid>::iterator pos = {};
  };
  size_t budget_ = 0;
  size_t used_ = 0;
  std::list<uuid> lru_ = {}; // front = most-recently-used
  std::unordered_map<uuid, entry> entries_ = {};
};

/// The state of the CATALOG actor.
struct catalog_state {
public:
  catalog_state() = default;

  constexpr static auto name = "catalog";

  /// Creates the catalog from a set of partition synopses.
  auto initialize(std::vector<partition_synopsis_pair> partitions)
    -> caf::result<atom::ok>;

  /// Add a new partition synopsis.
  auto merge(std::vector<partition_synopsis_pair> partitions)
    -> caf::result<atom::ok>;

  /// Erase this partition from the catalog.
  void erase(const uuid& partition);

  /// Retrieves the list of candidate partition IDs for a given expression.
  /// @param expr The expression to lookup.
  /// @returns A lookup result of candidate partitions categorized by type.
  auto lookup(expression expr) -> caf::expected<catalog_lookup_result>;

  auto lookup_impl(const expression& expr, const type& schema) const
    -> catalog_lookup_result::candidate_info;

  /// Loads the deferred Bloom-filter sketches of the given partition into the
  /// sketch cache, if possible. Returns the number of bytes loaded, or 0 if
  /// the partition was already cached, has no loadable sketches, or could not
  /// be loaded (in which case the partition stays a conservative candidate).
  auto ensure_sketches_loaded(const uuid& id, const type& schema) -> size_t;

  /// @returns A best-effort estimate of the amount of memory used for this
  /// catalog (in bytes).
  auto memusage() const -> size_t;

  // -- data members -----------------------------------------------------------

  /// A pointer to the parent actor.
  catalog_actor::pointer self = {};

  /// For each type, maps a partition ID to the synopses for that partition.
  // We mainly iterate over the whole map and return a sorted set, for which
  // the `flat_map` proves to be much faster than `std::{unordered_,}set`.
  // See also ae9dbed.
  std::unordered_map<tenzir::type,
                     detail::flat_map<uuid, partition_synopsis_ptr>>
    synopses_per_type;

  /// On-demand cache of partitions whose deferred Bloom-filter sketches have
  /// been loaded for pruning. Bounded by `tenzir.index.sketch-cache-bytes`.
  sketch_cache sketches;

  /// Set by `lookup_impl` when a predicate matched a partition only because a
  /// Bloom-filter sketch was deferred (null) and could be loaded to prune it.
  /// Drives whether `lookup` performs the on-demand load + re-evaluation.
  mutable bool encountered_deferred_sketch = false;

  std::optional<detail::request_cache> cache;

  tenzir::taxonomies taxonomies = {};
};

/// The CATALOG is the first index actor that queries hit. The result
/// represents a list of candidate partition IDs that may contain the desired
/// data. The CATALOG may return false positives but never false negatives.
/// @param self The actor handle.
/// @param sketch_cache_bytes Memory budget for on-demand loading of deferred
/// Bloom-filter sketches; zero disables on-demand loading.
auto catalog(catalog_actor::stateful_pointer<catalog_state> self,
             size_t sketch_cache_bytes = 0) -> catalog_actor::behavior_type;

} // namespace tenzir
