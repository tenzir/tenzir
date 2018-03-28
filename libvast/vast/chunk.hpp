/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include <cstddef>
#include <functional>
#include <string>

#include "caf/ref_counted.hpp"
#include "caf/intrusive_ptr.hpp"

#include "vast/detail/operators.hpp"

namespace vast {

class chunk;
class path;

/// A pointer to a chunk.
/// @relates chunk
using chunk_ptr = caf::intrusive_ptr<chunk>;

/// A contiguous block of memory.
class chunk : public caf::ref_counted,
              detail::totally_ordered<chunk> {
  chunk() = delete;
  chunk& operator=(const chunk&) = delete;

public:
  using deleter_type = std::function<void(char*, size_t)>;
  using value_type = const char;
  using size_type = size_t;
  using const_iterator = const char*;

  /// Factory function to construct a chunk.
  template <class... Ts>
  static chunk_ptr make(Ts&&... xs) {
    return chunk_ptr{new chunk(std::forward<Ts>(xs)...), false};
  }

  /// Memory-maps a chunk from a read-only file.
  /// @param filename The name of the file to memory-map.
  /// @param size The number of bytes to map. If 0, map the entire file.
  /// @param offset Where to start in terms of number of bytes from the start.
  static chunk_ptr mmap(const path& filename,
                        size_t size = 0, size_t offset = 0);

  /// Destroys the chunk and deallocates any owned memory.
  ~chunk();

  /// @returns The pointer to the chunk buffer.
  const char* data() const;

  /// @returns The size of the chunk.
  size_t size() const;

  /// @returns A pointer to the first byte in the chunk.
  const_iterator begin() const;

  /// @returns A pointer to one past the last byte in the chunk.
  const_iterator end() const;

  /// Creates a new chunk that structurally shares the data of this chunk.
  /// @param start The offset from the beginning where to begin the new chunk.
  /// @param length The length of the slice, beginning at *start*. If 0, the
  ///               slice ranges from *start* to the end of the chunk.
  /// @returns A new chunk over the subset.
  /// @pre `start + length < size()`
  chunk_ptr slice(size_t start, size_t length = 0) const;

private:
  /// Construct a chunk of a particular size using `::operator new`.
  /// @param size The number of bytes to allocate.
  /// @pre `size > 0`
  explicit chunk(size_t size);

  /// Constructs a chunk with a custom deallocation policy.
  /// @tparam Deleter The function to invoke when destroying the chunk.
  /// @param size The number of bytes starting at *ptr*.
  /// @param ptr A pointer to a contiguous memory region of size *size*.
  /// @param deleter The function to invoke on when destroying the chunk.
  chunk(size_t size, void* ptr, deleter_type deleter = deleter_type{});

  char* data_;
  size_t size_;
  deleter_type deleter_;
};

/// @relates chunk
bool operator==(const chunk& x, const chunk& y);

/// @relates chunk
bool operator<(const chunk& x, const chunk& y);

} // namespace vast

