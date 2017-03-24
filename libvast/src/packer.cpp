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

packer::packer(size_t buffer_size) : serializer_{nullptr, buffer_} {
  buffer_.reserve(buffer_size);
  // Reserve space for location of offset table.
  buffer_.resize(sizeof(uint32_t));
}

chunk_ptr packer::finish() {
  // Embed location of offset table.
  auto off = static_cast<uint32_t>(buffer_.size());
  auto ptr = reinterpret_cast<uint32_t*>(buffer_.data());
  *ptr = detail::to_network_order(off);
  // Serialize offset table.
  delta_encode(offsets_);
  serializer_ << offsets_;
  buffer_.shrink_to_fit();
  // Construct overlay from buffer.
  auto data = buffer_.data();
  auto size = buffer_.size();
  auto deleter = [buf=std::move(buffer_)](char*, size_t) { /* nop */ };
  buffer_ = {};
  return chunk::make(size, data, std::move(deleter));
}

} // namespace vast
