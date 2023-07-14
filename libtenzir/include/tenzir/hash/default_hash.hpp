//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/hash/concepts.hpp"
#include "tenzir/hash/xxhash.hpp"

namespace tenzir {

/// The default hash algorithm.
using default_hash = xxh3_64;

// To avoid performance regression, the default hash algorithm in Tenzir must
// support both incremental and oneshot hashing.
static_assert(oneshot_hash<default_hash> && incremental_hash<default_hash>);

} // namespace tenzir
