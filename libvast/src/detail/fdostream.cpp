#include "vast/detail/fdostream.hpp"

namespace vast {
namespace detail {

fdostream::fdostream(int fd) : std::ostream{0}, buf_{fd} {
  rdbuf(&buf_);
}

} // namespace detail
} // namespace vast
