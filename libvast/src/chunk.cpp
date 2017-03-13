#include <tuple>

#include "vast/chunk.hpp"

#include "vast/detail/assert.hpp"

namespace vast {

chunk::chunk() : data_{nullptr}, size_{0} {
  // nop
}

chunk::chunk(size_t size)
  : data_{new byte_type[size]},
    size_{size},
    deleter_{[](byte_type* ptr, size_t) { delete[] ptr; }} {
  VAST_ASSERT(size_ > 0);
}

chunk::chunk(size_t size, void* ptr)
  : data_{reinterpret_cast<byte_type*>(ptr)},
    size_{size} {
}

chunk::~chunk() {
  if (deleter_)
    deleter_(data_, size_);
}

chunk::byte_type*chunk::data() const {
  return data_;
}

size_t chunk::size() const {
  return size_;
}

bool operator==(const chunk& x, const chunk& y) {
  return x.data() == y.data() && x.size() == y.size();
}

bool operator<(const chunk& x, const chunk& y) {
  auto lhs = std::make_tuple(x.data(), x.size());
  auto rhs = std::make_tuple(y.data(), y.size());
  return lhs < rhs;
}

} // namespace vast
