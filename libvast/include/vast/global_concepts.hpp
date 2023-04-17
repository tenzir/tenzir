//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/taxonomies.hpp"

namespace vast::global_concepts {

/// Initializes the system-wide concept registry.
/// @param m The concept_map to initialize the registry with.
/// @returns true on success or false if registry was already initialized.
auto init(concepts_map m) -> bool;

/// Retrieves a pointer to the system-wide concept registry.
/// @returns nullptr if registry is not initialized.
auto get() -> const concepts_map*;

} // namespace vast::global_concepts
