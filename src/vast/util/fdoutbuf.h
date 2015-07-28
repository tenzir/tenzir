#ifndef VAST_UTIL_FDOUTBUF_H
#define VAST_UTIL_FDOUTBUF_H

#include <streambuf>

namespace vast {
namespace util {

/// A streambuffer that proxies writes to an underlying POSIX file descriptor.
class fdoutbuf : public std::streambuf
{
public:
  /// Constructs an output streambuffer from a POSIX file descriptor.
  /// @param fd The file descriptor to construct the streambuffer for.
  fdoutbuf(int fd);

protected:
  int_type overflow(int_type c) override;
  std::streamsize xsputn(char const* s, std::streamsize n) override;

private:
  int fd_;
};

} // namespace util
} // namespace vast

#endif
