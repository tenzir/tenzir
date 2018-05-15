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

#include <caf/actor.hpp>
#include <caf/fwd.hpp>
#include <caf/ref_counted.hpp>

#include "vast/detail/assert.hpp"
#include "vast/expression.hpp"
#include "vast/expression.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/filesystem.hpp"
#include "vast/logger.hpp"
#include "vast/type.hpp"
#include "vast/uuid.hpp"

namespace vast::system {

/// Manages a set of INDEXER actors for a single partition.
class indexer_manager : public caf::ref_counted {
public:
  /// Persistent meta state for manager instances.
  struct meta_data {
    /// Maps type digests (used as directory name) to types.
    std::map<std::string, type> types;

    /// Stores whether we modified `types` after loading it.
    bool dirty = false;
  };

  using indexer_factory = std::function<caf::actor (path, type)>;

  indexer_manager(path dir, uuid partition_id, indexer_factory f);

  ~indexer_manager() noexcept override;

  /// Applies all matching INDEXER actors for `expr` to `f` and returns the
  /// number of type matches.
  template <class F>
  size_t for_each_match(const expression& expr, F f) const {
    size_t num = 0;
    for (auto& [t, a] : indexers_) {
      VAST_ASSERT(a != nullptr);
      auto resolved = visit(type_resolver{t}, expr);
      if (resolved && visit(matcher{t}, *resolved)) {
        VAST_DEBUG("found matching type for expression:", t);
        f(a);
        ++num;
      }
    }
    return num;
  }

  /// Applies all matching INDEXER actors for `expr` to `f` and returns the
  /// number of type matches.
  template <class F>
  void for_each(F f) const {
    for (auto& [t, a] : indexers_) {
      VAST_ASSERT(a != nullptr);
      f(a);
    }
  }

  /// Adds an INDEXER to the manager if no INDEXER is assigned to `key` yet.
  /// @returns The INDEXER assigned to `key` and whether the INDEXER was
  ///          newly added.
  std::pair<caf::actor, bool> get_or_add(const type& key);

  /// Returns whether the meta data was changed.
  inline bool dirty() const noexcept {
    return meta_data_.dirty;
  }

  /// Returns a list of all types known by the manager.
  std::vector<type> types() const;

private:
  caf::actor make_event_indexer(const type& key, std::string digest);

  caf::actor make_event_indexer(const type& key);

  static std::string to_digest(const type& x);

  /// Stores one INDEXER actor per type.
  std::unordered_map<type, caf::actor> indexers_;

  /// Persistent state for the partition.
  meta_data meta_data_;

  /// ID of the managed partition.
  uuid partition_id_;

  /// Factory for spawning INDEXER actors.
  indexer_factory make_event_indexer_;

  /// Directory for persisting the meta data.
  path dir_;
};

/// @relates indexer_manager::meta_data
template <class Inspector>
auto inspect(Inspector& f, indexer_manager::meta_data& x) {
  return f(x.types);
}

using indexer_manager_ptr = caf::intrusive_ptr<indexer_manager>;

/// Creates an indexer manager.
indexer_manager_ptr make_indexer_manager(path dir, uuid partition_id,
                                         indexer_manager::indexer_factory f);

/// Creates an indexer manager that spawns `event_indexer` instances as
/// children of `self`.
/// @param `self` The parent actor.
/// @warning `self` must outlive the returned indexer manager and no other
///           actor (or thread) may acquire non-const access to the returned
///           indexer manager.
indexer_manager_ptr make_indexer_manager(caf::local_actor* self, path dir);

} // namespace vast::system
