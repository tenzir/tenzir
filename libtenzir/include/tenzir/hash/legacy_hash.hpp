//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// TODO: remove this entire header after all persistent state has been made
// upgradeable and converted to our new default hash function.

#pragma once

#include "tenzir/hash/xxhash.hpp"

namespace tenzir {

/// The hash algorithm that we use in data structures where the choice of hash
/// function changes persistent state.
using legacy_hash = xxh64;

} // namespace tenzir
