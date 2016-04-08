#include "vast/util/fdostream.hpp"

namespace vast {
namespace util {

fdostream::fdostream(int fd) : std::ostream{0}, buf_{fd} {
  rdbuf(&buf_);
}

} // namespace util
} // namespace vast
