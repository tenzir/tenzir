#ifndef VAST_PACK_HPP
#define VAST_PACK_HPP

#include <cstddef>
#include <cstdint>

#include <flatbuffers/flatbuffers.h>

#include "vast/chunk.hpp"

namespace vast {

/// Packs a instance of a packable type into a chunk.
/// @param x The instance to pack.
/// @returns A chunk containing the packed version of *x*.
template <class T>
chunk_ptr pack(const T& x) {
  flatbuffers::FlatBufferBuilder builder;
  auto offset = build(builder, x);
  builder.Finish(offset);
  auto size = builder.GetSize();
  auto ptr = builder.ReleaseBufferPointer();
  // Flatbuffers uses a unique_ptr<uint8_t, D> to represent a buffer, where D
  // is a "custom" deleter that simply wraps ::operator new[] and delete[].
  // We're transferring ownership to the chunk by releasing the original
  // unique_ptr and then handing it to the chunk.
  auto deleter = [](chunk::byte_type* buf, size_t) {
    delete[] reinterpret_cast<uint8_t*>(buf);
  };
  return make_chunk(size, ptr.release(), std::move(deleter));
}

} // namespace vast

#endif
