//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/data.hpp"

namespace vast::system {

/// Puts the version information into a record.
record retrieve_versions();

/// Prints the version information to stdout.
void print_version(const record& extra_content = {});

/// Displays the software version to the user.
caf::message version_command(const invocation& inv, caf::actor_system& sys);

} // namespace vast::system
