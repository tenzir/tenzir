#ifndef VAST_UTIL_FDOSTREAM_H
#define VAST_UTIL_FDOSTREAM_H

#include <ostream>

#include "vast/util/fdoutbuf.h"

namespace vast {
namespace util {

/// An output stream which wraps a ::fdoutbuf.
class fdostream : public std::ostream
{
public:
  fdostream(int fd);

private:
  fdoutbuf buf_;
};

} // namespace util
} // namespace vast

#endif

