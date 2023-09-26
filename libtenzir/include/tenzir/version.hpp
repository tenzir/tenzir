//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/data.hpp"

namespace tenzir {

/// Puts the version information into a record.
record retrieve_versions();

/// Returns a list of features supported by this build of the node.
std::vector<std::string> tenzir_features();

} // namespace tenzir
