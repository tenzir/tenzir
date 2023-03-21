//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/data.hpp"
#include "vast/endpoint.hpp"
#include "vast/system/actors.hpp"

namespace vast::system {

// TODO: Move to other header
caf::expected<endpoint> get_node_endpoint(const caf::settings& opts);

/// Connects to a remote VAST server.
caf::expected<node_actor>
connect_to_node(caf::scoped_actor& self, const caf::settings& opts);

} // namespace vast::system
