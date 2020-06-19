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

#include "vast/filesystem.hpp"
#include "vast/fwd.hpp"
#include "vast/schema.hpp"
#include "vast/system/accountant.hpp"
#include "vast/type.hpp"

#include <caf/expected.hpp>
#include <caf/typed_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <map>
#include <string>
#include <unordered_set>

namespace vast::system {

// TODO: Operate on vast::schema directly, or find a way to properly forward
// declare the inner type without having to include <unordered_set> in fwd.hpp.
struct type_set {
  std::unordered_set<vast::type> value;

  template <class Inspector>
  friend auto inspect(Inspector& f, type_set& x) {
    return f(caf::meta::type_name("type_set"), x.value);
  }
};

// clang-format off
using type_registry_type = caf::typed_actor<
  caf::reacts_to<atom::telemetry>,
  caf::replies_to<atom::status>::with<caf::dictionary<caf::config_value>>,
  caf::reacts_to<caf::stream<table_slice_ptr>>,
  caf::reacts_to<atom::put, vast::type>,
  caf::reacts_to<atom::put, vast::schema>,
  caf::replies_to<atom::get>::with<type_set>,
  caf::replies_to<atom::get, std::string>::with<type_set>
>;
// clang-format on

struct type_registry_state;

using type_registry_actor
  = type_registry_type::stateful_pointer<type_registry_state>;

using type_registry_behavior = type_registry_type::behavior_type;

struct type_registry_state {
  /// The name of the actor.
  static inline constexpr auto name = "type-registry";

  /// Generate a telemetry report for the accountant.
  report telemetry() const;

  /// Summarizes the actors state.
  caf::dictionary<caf::config_value> status() const;

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

  /// Get a list of known types from the registry for a name.
  type_set types(std::string key) const;

  type_registry_actor self = {};
  accountant_type accountant = {};
  std::map<std::string, type_set> data = {};
  vast::path dir = {};
};

type_registry_behavior type_registry(type_registry_actor self, const path& dir);

} // namespace vast::system
