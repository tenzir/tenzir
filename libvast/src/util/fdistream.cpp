#include "vast/util/fdistream.hpp"

namespace vast {
namespace util {

fdistream::fdistream(int fd, size_t buffer_size)
  : std::istream{nullptr},
    buf_{fd, buffer_size} {
  rdbuf(&buf_);
}

} // namespace util
} // namespace vast
