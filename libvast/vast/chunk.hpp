#ifndef VAST_CHUNK_HPP
#define VAST_CHUNK_HPP

#include <cstddef>
#include <functional>
#include <string>

#include "caf/ref_counted.hpp"
#include "caf/intrusive_ptr.hpp"

#include "vast/detail/operators.hpp"

namespace vast {

class chunk;

/// A pointer to a chunk.
/// @relates chunk
using chunk_ptr = caf::intrusive_ptr<chunk>;

/// A contiguous block of memory with customizable ownership semantics.
class chunk : public caf::ref_counted,
              detail::totally_ordered<chunk> {
  chunk() = delete;
  chunk& operator=(const chunk&) = delete;

public:
  /// Factory function to create a chunk without calling `new`.
  /// @relates chunk
  template <class... Ts>
  static chunk_ptr make(Ts&&... xs) {
    return new chunk(std::forward<Ts>(xs)...);
  }

  /// Memory-maps a chunk from a file.
  /// @param filename The name of the file to memory-map.
  /// @param size The number of bytes to map. If 0, map the entire file.
  /// @param offset Where to start in terms of number of bytes from the start. 
  static chunk_ptr mmap(const std::string& filename,
                        size_t size = 0, size_t offset = 0);

  /// Destroys the chunk and deallocates any owned memory.
  ~chunk();

  /// @returns The pointer to the chunk buffer.
  const char* data() const;

  /// @returns The size of the chunk.
  size_t size() const;

private:
  /// Construct an owning pointer of a particular size.
  /// @param size The number of bytes to allocate.
  explicit chunk(size_t size);

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
  chunk(size_t size, void* ptr, Deleter deleter = [](char*, size_t) {})
    : data_{reinterpret_cast<char*>(ptr)},
      size_{size},
      deleter_{std::move(deleter)} {
  }

  char* data_;
  size_t size_;
  std::function<void(char*, size_t)> deleter_;
};

/// @relates chunk
bool operator==(const chunk& x, const chunk& y);

/// @relates chunk
bool operator<(const chunk& x, const chunk& y);

} // namespace vast

#endif
