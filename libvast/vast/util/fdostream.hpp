#ifndef VAST_UTIL_FDOSTREAM_HPP
#define VAST_UTIL_FDOSTREAM_HPP

#include <ostream>

#include "vast/util/fdoutbuf.hpp"

namespace vast {
namespace util {

/// An output stream which wraps a ::fdoutbuf.
class fdostream : public std::ostream {
public:
  fdostream(int fd);

private:
  fdoutbuf buf_;
};

} // namespace util
} // namespace vast

#endif
