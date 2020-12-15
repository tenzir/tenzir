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

#include "vast/fwd.hpp"

#include "vast/expression.hpp"
#include "vast/path.hpp"
#include "vast/schema.hpp"
#include "vast/system/accountant_actor.hpp"
#include "vast/system/type_registry_actor.hpp"
#include "vast/taxonomies.hpp"
#include "vast/type.hpp"
#include "vast/type_set.hpp"

#include <caf/expected.hpp>

#include <map>
#include <string>
#include <unordered_set>

namespace vast::system {

struct type_registry_state {
  /// The name of the actor.
  static inline constexpr auto name = "type-registry";

  /// Generate a telemetry report for the accountant.
  report telemetry() const;

  /// Summarizes the actors state.
  caf::dictionary<caf::config_value> status(status_verbosity v) const;

  /// Create the path that the type-registry is persisted at on disk.
  vast::path filename() const;

  /// Save the type-registry to disk.
  caf::error save_to_disk() const;

  /// Load the type-registry from disk.
  caf::error load_from_disk();

  /// Store a new layout in the registry.
  void insert(vast::type layout);

  /// Get a list of known types from the registry.
  type_set types() const;

  type_registry_actor::pointer self = {};
  accountant_actor accountant = {};
  std::map<std::string, type_set> data = {};
  vast::schema configuration_schema = {};
  vast::taxonomies taxonomies = {};
  vast::path dir = {};
};

type_registry_actor::behavior_type
type_registry(type_registry_actor::stateful_pointer<type_registry_state> self,
              const path& dir);

} // namespace vast::system
