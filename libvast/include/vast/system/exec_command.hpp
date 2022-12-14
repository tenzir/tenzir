//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/aliases.hpp"

namespace vast::system {

/// Returns context around the results of a VAST pipeline.
caf::message exec_command(const invocation& inv, caf::actor_system& sys);

} // namespace vast::system
