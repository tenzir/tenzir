#ifndef VAST_PACKER_HPP
#define VAST_PACKER_HPP

#include <memory>
#include <vector>

#include <caf/streambuf.hpp>

#include "vast/chunk.hpp"
#include "vast/expected.hpp"

#include "vast/detail/coded_serializer.hpp"

namespace vast {

/// Serializes elements into a contiguous chunk of memory, where each element
/// is identified by an offset relative to the beginning of the chunk.
/// @relates unpacker overlay
class packer {
public:
  /// Default-constructs a packer with one builder.
  /// @param buffer_size The initial buffer size used for serialization.
  explicit packer(size_t buffer_size = 2048);

  /// Serializes an element into the churrently currently built.
  /// @param x The element to pack.
  template <class T>
  expected<void> pack(T&& x) {
    offsets_.push_back(buffer_.size());
    return serializer_.apply(const_cast<T&>(x));
  }

  /// Finalize packing of elements in the current block and return the
  /// corresponding contiguous buffer prepended with an offset table.
  /// @returns A chunk of memory representing offset table and element buffer.
  chunk_ptr finish();

private:
  using serializer_type = detail::coded_serializer<caf::vectorbuf>;

  std::vector<uint32_t> offsets_;
  std::vector<char> buffer_;
  serializer_type serializer_;
};

} // namespace vast

#endif
