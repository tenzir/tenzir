//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

// TODO: remove this entire header after all persistent state has been made
// upgradeable and converted to our new default hash function.

#pragma once

#include "vast/concept/hashable/xxhash.hpp"

namespace vast {

/// The hash algorithm that we use in data structures where the choice of hash
/// function changes persistent state.
using legacy_hash = xxh64;

} // namespace vast
