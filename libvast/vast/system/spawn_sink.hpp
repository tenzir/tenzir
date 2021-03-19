//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/aliases.hpp"
#include "vast/fwd.hpp"

namespace vast::system {

/// Tries to spawn a new SINK for the PCAP format.
/// @param self Points to the parent actor.
/// @param args Configures the new actor.
/// @returns a handle to the spawned actor on success, an error otherwise
caf::expected<caf::actor>
spawn_pcap_sink(caf::local_actor* self, spawn_arguments& args);

/// Tries to spawn a new SINK for the Zeek format.
/// @param self Points to the parent actor.
/// @param args Configures the new actor.
/// @returns a handle to the spawned actor on success, an error otherwise
caf::expected<caf::actor>
spawn_zeek_sink(caf::local_actor* self, spawn_arguments& args);

/// Tries to spawn a new SINK for the ASCII format.
/// @param self Points to the parent actor.
/// @param args Configures the new actor.
/// @returns a handle to the spawned actor on success, an error otherwise
caf::expected<caf::actor>
spawn_ascii_sink(caf::local_actor* self, spawn_arguments& args);

/// Tries to spawn a new SINK for the CSV format.
/// @param self Points to the parent actor.
/// @param args Configures the new actor.
/// @returns a handle to the spawned actor on success, an error otherwise
caf::expected<caf::actor>
spawn_csv_sink(caf::local_actor* self, spawn_arguments& args);

/// Tries to spawn a new SINK for the JSON format.
/// @param self Points to the parent actor.
/// @param args Configures the new actor.
/// @returns a handle to the spawned actor on success, an error otherwise
caf::expected<caf::actor>
spawn_json_sink(caf::local_actor* self, spawn_arguments& args);

} // namespace vast::system
