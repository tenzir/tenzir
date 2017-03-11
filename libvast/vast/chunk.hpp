#ifndef VAST_CHUNK_HPP
#define VAST_CHUNK_HPP

#include <cstddef>
#include <functional>

#include "caf/ref_counted.hpp"
#include "caf/intrusive_ptr.hpp"

#include "vast/detail/operators.hpp"

namespace vast {

/// A contiguous block of memory with customizable ownership semantics.
class chunk : public caf::ref_counted,
              detail::totally_ordered<chunk> {
public:
  /// The storage of memory.
  using byte_type = char;

  static_assert(sizeof(byte_type) == 1, "byte type must have size of 1");

  /// Default-constructs an empty chunk.
  chunk();

  /// Construct an owning pointer of a particular size.
  /// @param size The number of bytes to allocate.
  chunk(size_t size);

  /// Constructs a chunk that doesn't own its memory.
  /// @param size The number of bytes of *ptr*.
  /// @param ptr A pointer to a contiguous memory region of size *size*.
  chunk(size_t size, void* ptr);

  /// Constructs a chunk with a custom deallocation policy.
  /// @tparam Deleter The function to invoke when destroying the chunk.
  /// @param size The number of bytes of *ptr*.
  /// @param ptr A pointer to a contiguous memory region of size *size*.
  /// @param deleter The function to invoke on when destroying the chunk.
  template <class Deleter>
  chunk(size_t size, void* ptr, Deleter deleter = [](byte_type*, size_t) {})
    : data_{reinterpret_cast<byte_type*>(ptr)},
      size_{size},
      deleter_{std::move(deleter)} {
  }

  /// Destroys the chunk and deallocates any owned memory.
  ~chunk();

  /// @returns The pointer to the chunk buffer.
  byte_type* data() const;

  /// @returns The size of the chunk.
  size_t size() const;

private:
  byte_type* data_;
  size_t size_;
  std::function<void(byte_type*, size_t)> deleter_;
};

/// @relates chunk
bool operator==(const chunk& x, const chunk& y);

/// @relates chunk
bool operator<(const chunk& x, const chunk& y);

/// A pointer to a chunk.
/// @relates chunk
using chunk_ptr = caf::intrusive_ptr<chunk>;

/// Factory function to create a chunk without calling `new`.
/// @relates chunk
template <class... Ts>
chunk_ptr make_chunk(Ts&&... xs) {
  return new chunk(std::forward<Ts>(xs)...);
}

} // namespace vast

#endif
