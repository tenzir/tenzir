//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/hash/concepts.hpp"
#include "vast/hash/xxhash.hpp"

namespace vast {

/// The default hash algorithm.
using default_hash = xxh3_64;

// To avoid performance regression, the default hash algorithm in VAST must
// support both incremental and oneshot hashing.
static_assert(oneshot_hash<default_hash> && incremental_hash<default_hash>);

} // namespace vast
