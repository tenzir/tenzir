#include <unistd.h>

#include <cstdio>

#include "vast/detail/fdoutbuf.hpp"

namespace vast {
namespace detail {

fdoutbuf::fdoutbuf(int fd) : fd_{fd} {
}

fdoutbuf::int_type fdoutbuf::overflow(int_type c) {
  if (c != EOF) {
    char z = c;
    if (::write(fd_, &z, 1) != 1)
      return EOF;
  }
  return c;
}

std::streamsize fdoutbuf::xsputn(char const* s, std::streamsize n) {
  return ::write(fd_, s, n);
}

} // namespace detail
} // namespace vast
