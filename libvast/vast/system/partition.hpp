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
#include "vast/filesystem.hpp"
#include "vast/fwd.hpp"
#include "vast/system/indexer_manager.hpp"
#include "vast/type.hpp"
#include "vast/uuid.hpp"

namespace vast::system {

/// A horizontal partition of the INDEX.
class partition : public caf::ref_counted {
public:
  // -- friends ----------------------------------------------------------------

  friend class indexer_manager;

  // -- member types -----------------------------------------------------------

  /// Persistent meta state for the partition.
  struct meta_data {
    /// Maps type digests (used as directory names) to types.
    caf::detail::unordered_flat_map<std::string, type> types;

    /// Stores whether we modified `types` after loading it.
    bool dirty = false;
  };

  // -- constructors, destructors, and assignment operators --------------------

  /// @param base_dir The base directory for all partition. This partition will
  ///                 save to and load from `base_dir / id`.
  /// @param id Unique identifier for this partition.
  /// @param factory Factory function for INDEXER actors.
  partition(const path& base_dir, uuid id,
            indexer_manager::indexer_factory factory);

  ~partition() noexcept override;

  // -- properties -------------------------------------------------------------

  /// Returns the INDEXER manager.
  indexer_manager& manager() noexcept {
    return mgr_;
  }

  /// Returns the INDEXER manager.
  const indexer_manager& manager() const noexcept {
    return mgr_;
  }

  /// Returns whether the meta data was changed.
  inline bool dirty() const noexcept {
    return meta_data_.dirty;
  }

  /// Returns the working directory of the partition.
  inline const path& dir() const noexcept {
    return dir_;
  }
  /// Returns the unique ID of the partition.
  inline const uuid& id() const noexcept {
    return id_;
  }

  /// Returns a list of all types in this partition.
  std::vector<type> types() const;

  /// Returns the file name for saving or loading the ::meta_data.
  path meta_file() const;

  // -- operations -------------------------------------------------------------

  /// Checks what types could match `expr` and calls
  /// `self->request(...).then(f)` for each matching INDEXER.
  /// @returns the number of matched INDEXER actors.
  template <class F>
  size_t lookup_requests(caf::event_based_actor* self, const expression& expr,
                         F callback) {
    return mgr_.for_each_match(expr, [&](caf::actor& indexer) {
      self->request(indexer, caf::infinite, expr).then(callback);
    });
  }

private:
  /// Called from the INDEXER manager whenever a new type gets added during
  /// ingestion.
  inline void add_type(const std::string& digest, const type& t) {
    if (meta_data_.types.emplace(digest, t).second && !meta_data_.dirty)
      meta_data_.dirty = true;
  }

  /// Spawns one INDEXER per type in the partition (lazily).
  indexer_manager mgr_;

  /// Keeps track of row types in this partition.
  meta_data meta_data_;

  /// Directory for persisting the meta data.
  path dir_;

  /// Uniquely identifies this partition.
  uuid id_;
};

// -- related types ------------------------------------------------------------

/// @relates partition
using partition_ptr = caf::intrusive_ptr<partition>;

// -- free functions -----------------------------------------------------------

/// @relates partition::meta_data
template <class Inspector>
auto inspect(Inspector& f, partition::meta_data& x) {
  return f(x.types);
}

/// Creates a partition.
/// @relates partition
partition_ptr make_partition(const path& base_dir, uuid id,
                             indexer_manager::indexer_factory f);

/// Creates a partition that spawns regular INDEXER actors as children of
/// `self`.
/// @relates partition
partition_ptr make_partition(caf::local_actor* self, const path& base_dir,
                             uuid id);

} // namespace vast::system

namespace std {

template <>
struct hash<vast::system::partition_ptr> {
  size_t operator()(const vast::system::partition_ptr& ptr) const;
};

} // namespace std
