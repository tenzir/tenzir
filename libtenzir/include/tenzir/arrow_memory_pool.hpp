//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <arrow/memory_pool.h>

namespace tenzir {

/// Returns the custom Arrow memory pool implementation for Tenzir.
///
/// @return A pointer to the Tenzir Arrow memory pool singleton.
///         The pointer is valid for the lifetime of the program.

[[nodiscard]] auto arrow_memory_pool() noexcept -> arrow::MemoryPool*;

} // namespace tenzir
