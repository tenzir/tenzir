//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/module.hpp"

namespace vast::event_types {

/// Initializes the system-wide type registry.
/// @param s The module.
/// @returns true on success or false if registry was already initialized.
bool init(module m);

/// Retrieves a pointer to the system-wide type registry.
/// @returns nullptr if registry is not initialized.
const module* get();

} // namespace vast::event_types
