#include <algorithm>

#include "vast/packer.hpp"

#include "vast/detail/assert.hpp"
#include "vast/detail/byte_swap.hpp"

namespace vast {

namespace {

// Compes the adjacent difference similar to the standard library function,
// except that it drops the first (identity) computation. For examples, instead
// of converting the sequence [1, 3, 7] to [1, 2, 4], this algorithm produces
// the result [2, 4].
template <class T>
void delta_encode(std::vector<T>& xs) {
  VAST_ASSERT(!xs.empty());
  for (auto i = 0u; i < xs.size() - 1; ++i)
    xs[i] = xs[i + 1] - xs[i];
  xs.pop_back();
}

} // namespace <anonymous>

packer::packer() : serializer_{nullptr, buffer_} {
}

chunk_ptr packer::finish() {
  std::vector<char> buffer;
  buffer.reserve(buffer_.size());
  buffer.resize(sizeof(uint32_t)); // reserved space for data offset
  // Serialize offsets, buffer, and data start.
  serializer_type sink{nullptr, buffer};
  delta_encode(offsets_);
  sink << offsets_;
  auto start = static_cast<uint32_t>(buffer.size());
  sink.apply_raw(buffer_.size(), buffer_.data());
  auto ptr = reinterpret_cast<uint32_t*>(buffer.data());
  *ptr = detail::to_network_order(start);
  buffer.shrink_to_fit();
  // Construct overlay from buffer.
  auto data = buffer.data();
  auto size = buffer.size();
  auto deleter = [buf=std::move(buffer)](char*, size_t) { /* nop */ };
  return chunk::make(size, data, std::move(deleter));
}

} // namespace vast
