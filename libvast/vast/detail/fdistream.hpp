#ifndef VAST_DETAIL_FDISTREAM_HPP
#define VAST_DETAIL_FDISTREAM_HPP

#include <cstddef>
#include <istream>

#include "vast/detail/fdinbuf.hpp"

namespace vast {
namespace detail {

/// An input stream which wraps a ::fdinbuf.
class fdistream : public std::istream {
public:
  fdistream(int fd, size_t buffer_size = 8192);

private:
  fdinbuf buf_;
};

} // namespace detail
} // namespace vast

#endif
