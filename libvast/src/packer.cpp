#include "vast/packer.hpp"

#include "vast/detail/byte_swap.hpp"

namespace vast {

packer::~packer() {
  if (!offsets_.empty())
    finish();
}

size_t packer::size() const {
  return offsets_.size();
}

size_t packer::finish() {
  if (offsets_.empty())
    return 0;
  // Delta-encode offsets and serialize them.
  for (auto i = 0u; i < offsets_.size() - 1; ++i)
    offsets_[i] = offsets_[i + 1] - offsets_[i];
  offsets_.pop_back();
  uint32_t offsets_position = streambuf_.put();
  serializer_ << offsets_;
  // Write offset position as trailing byte.
  char buf[sizeof(uint32_t)];
  auto ptr = reinterpret_cast<uint32_t*>(&buf);
  *ptr = detail::to_network_order(offsets_position);
  serializer_.apply_raw(sizeof(uint32_t), buf);
  // Enable re-use of packer by resetting offset table.
  offsets_.clear();
  return streambuf_.put();
}

} // namespace vast
