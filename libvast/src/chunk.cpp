#include <tuple>

#include "vast/chunk.hpp"

#include "vast/detail/assert.hpp"

namespace vast {

chunk::~chunk() {
  if (deleter_)
    deleter_(data_, size_);
}

const char* chunk::data() const {
  return data_;
}

size_t chunk::size() const {
  return size_;
}

chunk::chunk(size_t size)
  : data_{new char[size]},
    size_{size},
    deleter_{[](char* ptr, size_t) { delete[] ptr; }} {
  VAST_ASSERT(size_ > 0);
}

chunk::chunk(size_t size, void* ptr)
  : data_{reinterpret_cast<char*>(ptr)},
    size_{size} {
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
