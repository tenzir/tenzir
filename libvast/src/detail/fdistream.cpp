#include "vast/detail/fdistream.hpp"

namespace vast {
namespace detail {

fdistream::fdistream(int fd, size_t buffer_size)
  : std::istream{nullptr},
    buf_{fd, buffer_size} {
  rdbuf(&buf_);
}

} // namespace detail
} // namespace vast
