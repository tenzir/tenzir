//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/chunk.hpp"
#include "tenzir/generator.hpp"

namespace tenzir {

// Drains a generator of bytes, yielding at most one non-empty chunk. Yields an
// empty chunk whenever the input yields an empty chunk to allow usage in an
// operator's generator.
inline auto drain_bytes(generator<chunk_ptr> input) -> generator<chunk_ptr> {
  auto result = chunk_ptr{};
  auto it = input.begin();
  while (it != input.end()) {
    auto chunk = std::move(*it);
    ++it;
    if (not chunk) {
      co_yield {};
      continue;
    }
    result = std::move(chunk);
    break;
  }
  if (not result) {
    co_return;
  }
  auto byte_buffer = std::vector<std::byte>{};
  while (it != input.end()) {
    auto chunk = std::move(*it);
    ++it;
    if (not chunk) {
      co_yield {};
      continue;
    }
    if (result) [[unlikely]] {
      byte_buffer.reserve(result->size());
      byte_buffer.insert(byte_buffer.end(), result->begin(), result->end());
      result = {};
    }
    byte_buffer.reserve(byte_buffer.size() + chunk->size());
    byte_buffer.insert(byte_buffer.end(), chunk->begin(), chunk->end());
  }
  if (not result) {
    result = chunk::make(std::move(byte_buffer));
  } else {
    TENZIR_ASSERT(byte_buffer.empty());
  }
  co_yield std::move(result);
}

} // namespace tenzir
