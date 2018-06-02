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

#include <caf/actor.hpp>
#include <caf/detail/unordered_flat_map.hpp>
#include <caf/fwd.hpp>

#include "vast/expression.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/fwd.hpp"
#include "vast/logger.hpp"
#include "vast/system/fwd.hpp"
#include "vast/type.hpp"

namespace vast::system {

/// Manages a set of INDEXER actors for a single partition.
class indexer_manager {
public:
  using indexer_factory = std::function<caf::actor (path, type)>;

  indexer_manager(partition& parent, indexer_factory f);

  /// Applies all matching INDEXER actors for `expr` to `f` and returns the
  /// number of type matches.
  template <class F>
  size_t for_each_match(const expression& expr, F f) {
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

/*
  /// Applies all matching INDEXER actors for `expr` to `f` and returns the
  /// number of type matches.
  template <class F>
  void for_each(F f) const {
    for (auto& kvp : indexers_) {
      VAST_ASSERT(kvp->second != nullptr);
      f(kvp->second);
    }
  }
*/

  /// Adds an INDEXER to the manager if no INDEXER is assigned to `key` yet.
  /// @returns The INDEXER assigned to `key` and whether the INDEXER was
  ///          newly added.
  std::pair<caf::actor, bool> get_or_add(const type& key);

private:
  caf::actor make_indexer(const type& key, const std::string& digest);

  caf::actor make_indexer(const type& key);

  /// Stores one INDEXER actor per type.
  caf::detail::unordered_flat_map<type, caf::actor> indexers_;

  /// Factory for spawning INDEXER actors.
  indexer_factory make_indexer_;

  /// Pointer to the owning object.
  partition& parent_;
};

} // namespace vast::system
