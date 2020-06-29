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

#include "vast/byte.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/operators.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/die.hpp"
#include "vast/fwd.hpp"
#include "vast/span.hpp"

#include <caf/fwd.hpp>
#include <caf/intrusive_ptr.hpp>
#include <caf/ref_counted.hpp>

#include <cstddef>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <utility>

namespace vast {

/// A contiguous block of memory. A chunk must not be empty.
class chunk : public caf::ref_counted {
  chunk() = delete;
  chunk& operator=(const chunk&) = delete;

public:
  // -- member types ----------------------------------------------------------

  using value_type = char;
  using pointer = value_type*;
  using const_pointer = const value_type*;
  using size_type = size_t;
  using const_iterator = const char*;
  using deleter_type = std::function<void()>;

  // -- factory functions -----------------------------------------------------

  /// Constructs a chunk of a particular size using `::operator new`.
  /// @param size The number of bytes to allocate.
  /// @returns A chunk pointer or `nullptr` on failure.
  /// @pre `size > 0`
  static chunk_ptr make(size_type size);

  /// Constructs a chunk of particular size and pointer to data.
  /// @param size The number of bytes *data* points to.
  /// @param data The raw byte data.
  /// @param deleter The function to delete the data.
  /// @returns A chunk pointer or `nullptr` on failure.
  /// @pre `size > 0 && static_cast<bool>(deleter)`
  static chunk_ptr make(size_type size, void* data, deleter_type deleter);

  /// Construct a chunk from a std::vector of bytes.
  /// @param xs The std::vector of bytes.
  /// @returns A chunk pointer or `nullptr` on failure.
  /// @pre `std::size(xs) != 0`
  // FIXME: Make a type whitelist. This optimization only makes sense for owning
  // containers of bytes that implement move constructors. (i.e. we want to
  // allow vector<char>, vector<byte>, flatbuffer, etc. but not string_view,
  // vast::span, etc.)
  template <typename Byte>
  static chunk_ptr make(std::vector<Byte>&& xs) {
    static_assert(sizeof(Byte) == 1);
    VAST_ASSERT(std::size(xs) != 0);
    auto ys = std::make_shared<std::vector<Byte>>(std::move(xs));
    auto deleter = [=]() mutable { ys.reset(); };
    return make(ys->size(), ys->data(), deleter);
  }

  // Construct a chunk by copying a range of bytes.
  template <typename Byte>
  static chunk_ptr copy(span<Byte> span) {
    static_assert(sizeof(Byte) == 1);
    auto size = span.size();
    VAST_ASSERT(size > 0);
    auto chunk = make(size);
    if (chunk)
      ::memcpy(chunk->data_, span.data(), size);
    return chunk;
  }

  /// Memory-maps a chunk from a read-only file.
  /// @param filename The name of the file to memory-map.
  /// @param size The number of bytes to map. If 0, map the entire file.
  /// @param offset Where to start in terms of number of bytes from the start.
  /// @returns A chunk pointer or `nullptr` on failure.
  static chunk_ptr
  mmap(const path& filename, size_type size = 0, size_type offset = 0);

  /// Destroys the chunk and releases owned memory via the deleter.
  ~chunk() override;

  // -- container API ---------------------------------------------------------

  /// @returns The pointer to the chunk buffer.
  const_pointer data() const;

  /// @returns The size of the chunk.
  size_type size() const;

  // -- iteration -------------------------------------------------------------

  /// @returns A pointer to the first byte in the chunk.
  const_iterator begin() const;

  /// @returns A pointer to one past the last byte in the chunk.
  const_iterator end() const;

  // -- accessors -------------------------------------------------------------

  /// Retrieves a value at given offset.
  /// @param i The position of the byte.
  /// @returns The value at position *i*.
  /// @pre `i < size()`
  value_type operator[](size_type i) const;

  /// Casts the chunk data into a immutable pointer of a desired type.
  /// @tparam T the type to cast to.
  /// @param offset The offset to start at.
  /// @returns a pointer of type `const T*` at position *offset*.
  template <class T>
  const T* as(size_type offset = 0) const {
    VAST_ASSERT(offset < size());
    auto ptr = data() + offset;
    return reinterpret_cast<const T*>(std::launder(ptr));
  }

  /// Creates a new chunk that structurally shares the data of this chunk.
  /// @param start The offset from the beginning where to begin the new chunk.
  /// @param length The length of the slice, beginning at *start*. If 0, the
  ///               slice ranges from *start* to the end of the chunk.
  /// @returns A new chunk over the subset.
  /// @pre `start + length < size()`
  chunk_ptr slice(size_type start, size_type length = 0) const;

  /// Adds an additional step for deleting this chunk.
  /// @param f Function object that gets called after all previous deletion
  ///          steps ran.
  template <class F>
  void add_deletion_step(F f) {
    auto g = [first = std::move(deleter_), second = std::move(f)] {
      first();
      second();
    };
    deleter_ = std::move(g);
  }

  // -- concepts --------------------------------------------------------------

  friend span<const byte> as_bytes(const chunk_ptr& x);

  friend caf::error write(const path& filename, const chunk_ptr& x);

  friend caf::error read(const path& filename, chunk_ptr& x);

  friend caf::error inspect(caf::serializer& sink, const chunk_ptr& x);

  friend caf::error inspect(caf::deserializer& source, chunk_ptr& x);

private:
  chunk(void* ptr, size_type size, deleter_type deleter);

  pointer data_;
  size_type size_;
  deleter_type deleter_;
};

// We need this template in order to be able to send chunks as messages,
// but we never actually want to have to use it since it will only be invoked
// if chunk messages would be sent over the network. And we only want to send
// chunks around internally. So it is declared here but never defined, leading
// to a linker error if its used.
// TODO: Replace with a static assert.
template <typename Inspector>
typename Inspector::return_type inspect(Inspector&, chunk&);

} // namespace vast
