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

/// Returns a stateful function that retrieves a given number of bytes in a
/// contiguous buffer from a generator of chunks. The last span is underful,
/// i.e., smaller than the number of bytes requested, and zero-sized if the
/// input boundaries are aligned. The function returns nullopt whenever it
/// merges buffers from multiple chunks. This does not indicate completion.
inline auto make_byte_reader(generator<chunk_ptr> input) {
  input.begin(); // prime the pump
  return
    [input = std::move(input), chunk = chunk_ptr{}, chunk_offset = size_t{0},
     buffer = std::vector<std::byte>{}, buffer_offset = size_t{0}](
      size_t num_bytes) mutable -> std::optional<std::span<const std::byte>> {
      // The internal chunk is not available when we first enter this function
      // and as well when we have no more chunks (at the end).
      if (!chunk) {
        TENZIR_ASSERT(chunk_offset == 0);
        // Can we fulfill our request from the buffer?
        if (buffer.size() - buffer_offset >= num_bytes) {
          auto result = as_bytes(buffer).subspan(buffer_offset, num_bytes);
          buffer_offset += num_bytes;
          return result;
        }
        // Can we get more chunks?
        auto current = input.unsafe_current();
        if (current == input.end()) {
          // We're done and return an underful chunk.
          auto result = as_bytes(buffer).subspan(buffer_offset);
          buffer_offset = buffer.size();
          TENZIR_ASSERT(result.size() < num_bytes);
          return result;
        }
        chunk = std::move(*current);
        ++current;
        if (!chunk) {
          return std::nullopt;
        }
      }
      // We have a chunk.
      TENZIR_ASSERT(chunk != nullptr);
      if (buffer.size() == buffer_offset) {
        // Have consumed the entire chunk last time? Then reset and try again.
        if (chunk_offset == chunk->size()) {
          chunk_offset = 0;
          chunk = nullptr;
          return std::nullopt;
        }
        TENZIR_ASSERT(chunk_offset < chunk->size());
        // If we have a chunk, but not enough bytes, then we must buffer.
        if (chunk->size() - chunk_offset < num_bytes) {
          buffer = {chunk->begin() + chunk_offset, chunk->end()};
          buffer_offset = 0;
          chunk = nullptr;
          chunk_offset = 0;
          return std::nullopt;
        }
        // Enough in the chunk, simply yield from it.
        auto result = as_bytes(*chunk).subspan(chunk_offset, num_bytes);
        chunk_offset += num_bytes;
        return result;
      }
      // If we need to process both a buffer and chunk, we copy over the chunk
      // remainder into the buffer.
      buffer.erase(buffer.begin(), buffer.begin() + buffer_offset);
      buffer_offset = 0;
      buffer.reserve(buffer.size() + chunk->size() - chunk_offset);
      buffer.insert(buffer.end(), chunk->begin() + chunk_offset, chunk->end());
      chunk = nullptr;
      chunk_offset = 0;
      if (buffer.size() >= num_bytes) {
        auto result = as_bytes(buffer).subspan(0, num_bytes);
        buffer_offset = num_bytes;
        return result;
      }
      return std::nullopt;
    };
}

} // namespace tenzir
