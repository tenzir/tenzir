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
/// This function provides a thread-safe singleton instance of a custom
/// memory pool that uses mimalloc as the underlying allocator. All Arrow
/// operations in Tenzir should use this pool instead of Arrow's default
/// memory pool to ensure consistent memory management and improved
/// performance characteristics.
///
/// The pool is configured with:
/// - MIMALLOC_RESET_DELAY=100: Delays memory reset for better reuse
/// - MIMALLOC_RESET_DECOMMITS=1: Returns memory to the OS when reset
///
/// @return A pointer to the Tenzir Arrow memory pool singleton.
///         The pointer is valid for the lifetime of the program.
[[nodiscard]] auto arrow_memory_pool() noexcept -> arrow::MemoryPool*;

} // namespace tenzir
