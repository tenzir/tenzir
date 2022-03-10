//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/expression.hpp"
#include "vast/schema.hpp"
#include "vast/system/actors.hpp"
#include "vast/taxonomies.hpp"
#include "vast/type.hpp"
#include "vast/type_set.hpp"

#include <caf/expected.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <filesystem>
#include <map>
#include <string>
#include <unordered_set>

namespace vast::system {

struct type_registry_state {
  /// The name of the actor.
  static inline constexpr auto name = "type-registry";

  /// Generate a telemetry report for the accountant.
  [[nodiscard]] report telemetry() const;

  /// Summarizes the actors state.
  [[nodiscard]] record status(status_verbosity v) const;

  /// Create the path that the type-registry is persisted at on disk.
  [[nodiscard]] std::filesystem::path filename() const;

  /// Save the type-registry to disk.
  [[nodiscard]] caf::error save_to_disk() const;

  /// Load the type-registry from disk.
  caf::error load_from_disk();

  /// Store a new layout in the registry.
  void insert(vast::type layout);

  /// Get a list of known types from the registry.
  [[nodiscard]] type_set types() const;

  type_registry_actor::pointer self = {};
  accountant_actor accountant = {};
  std::map<std::string, type_set> data = {};
  vast::module configuration_schema = {};
  vast::taxonomies taxonomies = {};
  std::filesystem::path dir = {};
};

type_registry_actor::behavior_type
type_registry(type_registry_actor::stateful_pointer<type_registry_state> self,
              const std::filesystem::path& dir);

} // namespace vast::system
