/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include <functional>

#include <caf/detail/unordered_flat_map.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/fwd.hpp>

#include "vast/aliases.hpp"
#include "vast/detail/assert.hpp"
#include "vast/fwd.hpp"
#include "vast/system/fwd.hpp"
#include "vast/system/spawn_indexer.hpp"
#include "vast/system/table_indexer.hpp"
#include "vast/type.hpp"
#include "vast/uuid.hpp"

namespace vast::system {

/// The horizontal data scaling unit of the index. A partition represents a
/// slice of indexes for a specific ID interval.
class partition {
public:
  // -- member types -----------------------------------------------------------

  /// Persistent meta state for the partition.
  struct meta_data {
    /// Maps type digests (used as directory names) to layouts (i.e. record
    /// types).
    caf::detail::unordered_flat_map<std::string, record_type> types;

    /// Stores whether we modified `types` after loading it.
    bool dirty = false;
  };

  /// Maps table slice layouts their table indexer.
  using table_indexer_map = caf::detail::unordered_flat_map<record_type,
                                                            table_indexer>;

  // -- constructors, destructors, and assignment operators --------------------

  /// @param self The parent actor.
  /// @param base_dir The base directory for all partition. This partition will
  ///                 save to and load from `base_dir / to_string(id)`.
  /// @param id Unique identifier for this partition.
  /// @pre `self != nullptr`
  /// @pre `factory != nullptr`
  partition(index_state* state, uuid id, size_t max_capacity);

  ~partition() noexcept;

  // -- persistence ------------------------------------------------------------

  /// Materializes the partition layouts from disk.
  /// @returns an error if I/O operations fail.
  caf::error init();

  /// Persists the partition layouts to disk.
  /// @returns an error if I/O operations fail.
  caf::error flush_to_disk();

  // -- properties -------------------------------------------------------------

  /// @returns whether the meta data was changed.
  auto dirty() const noexcept {
    return meta_data_.dirty;
  }

  /// @returns the unique ID of the partition.
  auto& id() const noexcept {
    return id_;
  }

  /// @returns the state of the owning INDEX actor.
  auto& state() noexcept {
    return *state_;
  }

  /// @returns the remaining capacity in this partition.
  auto capacity() const noexcept {
    return capacity_;
  }

  /// Decreases the remaining capacity by `x`.
  /// @pre `capacity() >= x`
  auto reduce_capacity(size_t x) noexcept {
    VAST_ASSERT(capacity_ >= x);
    capacity_ -= x;
  }

  /// @returns all INDEXER actors of all matching layouts.
  evaluation_map eval(const expression& expr);

  /// @returns all layouts in this partition.
  std::vector<record_type> layouts() const;

  /// @returns the directory for persistent state.
  path base_dir() const;

  /// @returns the file name for saving or loading the ::meta_data.
  path meta_file() const;

  std::pair<table_indexer&, bool> get_or_add(const record_type& key);

  // -- operations -------------------------------------------------------------

  /// Iterates over all INDEXER actors that are managed by this partition.
  template <class F>
  void for_each_indexer(F f) {
    for (auto& kvp : table_indexers_) {
      kvp.second.spawn_indexers();
      kvp.second.for_each_indexer(f);
    }
  }

private:
  /// Called from the INDEXER manager whenever a new layout gets added during
  /// ingestion.
  void add_layout(const std::string& digest, const record_type& t) {
    if (meta_data_.types.emplace(digest, t).second && !meta_data_.dirty)
      meta_data_.dirty = true;
  }

  /// State of the INDEX actor that owns this partition.
  index_state* state_;

  /// Keeps track of row types in this partition.
  meta_data meta_data_;

  /// Uniquely identifies this partition.
  uuid id_;

  /// Stores one meta indexer per layout that in turn manages INDEXER actors.
  table_indexer_map table_indexers_;

  /// Remaining capacity in this partition.
  size_t capacity_;

  friend struct index_state;
};

// -- related types ------------------------------------------------------------

/// @relates partition
using partition_ptr = std::unique_ptr<partition>;

// -- free functions -----------------------------------------------------------

/// @relates partition::meta_data
template <class Inspector>
auto inspect(Inspector& f, partition::meta_data& x) {
  return f(x.types);
}

} // namespace vast::system

namespace std {

template <>
struct hash<vast::system::partition_ptr> {
  size_t operator()(const vast::system::partition_ptr& ptr) const;
};

} // namespace std
