//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/aliases.hpp"

namespace tenzir {

/// Format-independent implementation for export sub-commands.
caf::message
sink_command(const invocation& inv, caf::actor_system& sys, caf::actor snk);

} // namespace tenzir
