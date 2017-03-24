#include "vast/overlay.hpp"

#include "vast/detail/assert.hpp"
#include "vast/detail/byte_swap.hpp"
#include "vast/detail/varbyte.hpp"

namespace vast {

overlay::offset_table::offset_table(const char* ptr) : table_{ptr} {
  table_ += detail::varbyte::decode(size_, table_);
}

size_t overlay::offset_table::operator[](size_t i) const {
  VAST_ASSERT(i < size());
  auto result = size_t{0};
  auto ptr = table_;
  for (auto j = 0u; j < i; ++j) {
    size_t delta;
    ptr += detail::varbyte::decode(delta, ptr);
    result += delta;
  }
  return result;
}

size_t overlay::offset_table::size() const {
  return size_ + 1; // delta-coding reduces size by 1
}

overlay::overlay(chunk_ptr chk)
  : offsets_{chk->data() + detail::to_host_order(
                             *reinterpret_cast<const uint32_t*>(chk->data()))},
    chunk_{chk} {
}

const char* overlay::operator[](size_t i) const {
  VAST_ASSERT(chunk_);
  return chunk_->data() + sizeof(uint32_t) + offsets_[i];
}

size_t overlay::size() const {
  return offsets_.size();
}

chunk_ptr overlay::chunk() const {
  return chunk_;
}

} // namespace vast
