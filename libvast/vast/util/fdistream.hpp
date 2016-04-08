#ifndef VAST_UTIL_FDISTREAM_HPP
#define VAST_UTIL_FDISTREAM_HPP

#include <cstddef>
#include <istream>

#include "vast/util/fdinbuf.hpp"

namespace vast {
namespace util {

/// An input stream which wraps a ::fdinbuf.
class fdistream : public std::istream {
public:
  fdistream(int fd, size_t buffer_size = 8192);

private:
  fdinbuf buf_;
};

} // namespace util
} // namespace vast

#endif
