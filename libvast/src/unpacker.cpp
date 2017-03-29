#include "vast/unpacker.hpp"

namespace vast {

size_t unpacker::size() const {
  return offsets_.size();
}

} // namespace vast
