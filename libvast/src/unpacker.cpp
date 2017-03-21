#include "vast/unpacker.hpp"

namespace vast {

unpacker::unpacker(chunk_ptr chk) : overlay_{chk} {
}

size_t unpacker::size() const {
  return overlay_.size();
}

} // namespace vast
