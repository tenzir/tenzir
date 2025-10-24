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
/// contiguous buffer from a generator of chunks. The last chunk is underful,
/// i.e., smaller than the number of bytes requested, and zero-sized if the
/// input boundaries are aligned. The function returns nullptr whenever it
/// merges buffers from multiple chunks. This does not indicate completion.
inline auto make_byte_reader(generator<chunk_ptr> input) {
  input.begin(); // prime the pump
  return [input = std::move(input), buffer = chunk::make_empty(),
          offset = size_t{0}](size_t num_bytes) mutable -> chunk_ptr {
    TENZIR_ASSERT(num_bytes > 0);
    TENZIR_ASSERT(buffer);
    // If the buffer size exactly matches what is requested, then we can just
    // return the buffer itself.
    if (offset == 0 and buffer->size() == num_bytes) {
      TENZIR_ASSERT(buffer->size() - offset == num_bytes);
      offset = buffer->size();
      return buffer;
    }
    // If we have a buffer and it's non-empty, then we can just return a slice
    // of the buffer.
    if (buffer->size() - offset >= num_bytes) {
      auto result = buffer->slice(offset, num_bytes);
      offset += num_bytes;
      TENZIR_ASSERT(result->size() == num_bytes);
      return result;
    }
    // Otherwise we need to read more.
    auto current = input.unsafe_current();
    if (current == input.end()) {
      if (offset == 0) {
        offset = buffer->size();
        TENZIR_ASSERT(buffer->size() <= num_bytes);
        return buffer;
      }
      auto result = buffer->slice(offset, num_bytes);
      offset = buffer->size();
      TENZIR_ASSERT(result->size() <= num_bytes);
      return result;
    }
    auto chunk = std::move(*current);
    ++current;
    if (not chunk) {
      return {};
    }
    if (buffer->size() == offset) {
      buffer = std::move(chunk);
    } else {
      auto merged_buffer = std::make_unique<chunk::value_type[]>(
        buffer->size() - offset + chunk->size());
      std::memcpy(merged_buffer.get(), buffer->data() + offset,
                  buffer->size() - offset);
      std::memcpy(merged_buffer.get() + buffer->size() - offset, chunk->data(),
                  chunk->size());
      const auto merged_buffer_view = std::span{
        merged_buffer.get(), buffer->size() - offset + chunk->size()};
      buffer
        = chunk::make(merged_buffer_view,
                      [merged_buffer = std::move(merged_buffer)]() noexcept {
                        static_cast<void>(merged_buffer);
                      });
    }
    offset = 0;
    // Now that we read more, we can try again to return something.
    if (buffer->size() == num_bytes) {
      TENZIR_ASSERT(buffer->size() - offset == num_bytes);
      offset = buffer->size();
      return buffer;
    }
    if (buffer->size() > num_bytes) {
      auto result = buffer->slice(0, num_bytes);
      offset = num_bytes;
      TENZIR_ASSERT(result->size() == num_bytes);
      return result;
    }
    return {};
  };
}

/// Returns a stateful function that retrieves a given number of bytes in a
/// contiguous buffer from a generator of chunks. The last span is underful,
/// i.e., smaller than the number of bytes requested, and zero-sized if the
/// input boundaries are aligned. The function returns nullopt whenever it
/// merges buffers from multiple chunks. This does not indicate completion.
inline auto make_byte_view_reader(generator<chunk_ptr> input) {
  return
    [byte_reader = make_byte_reader(std::move(input))](
      size_t num_bytes) mutable -> std::optional<std::span<const std::byte>> {
      if (auto bytes = byte_reader(num_bytes)) {
        return as_bytes(bytes);
      }
      return std::nullopt;
    };
}

} // namespace tenzir
