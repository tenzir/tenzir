#ifndef VAST_DETAIL_FDOSTREAM_HPP
#define VAST_DETAIL_FDOSTREAM_HPP

#include <ostream>

#include "vast/detail/fdoutbuf.hpp"

namespace vast {
namespace detail {

/// An output stream which wraps a ::fdoutbuf.
class fdostream : public std::ostream {
public:
  fdostream(int fd);

private:
  fdoutbuf buf_;
};

} // namespace detail
} // namespace vast

#endif
