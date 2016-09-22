#ifndef VAST_DETAIL_FDINBUF_HPP
#define VAST_DETAIL_FDINBUF_HPP

#include <cstddef>
#include <streambuf>
#include <vector>

namespace vast {
namespace detail {

/// A streambuffer that proxies reads to an underlying POSIX file descriptor.
class fdinbuf : public std::streambuf {
  static constexpr size_t putback_area_size = 10;

public:
  /// Constructs an input streambuffer from a POSIX file descriptor.
  /// @param fd The file descriptor to construct the streambuffer for.
  /// @param buffer_size The size of the input buffer.
  /// @pre `buffer_size > putback_area_size`
  fdinbuf(int fd, size_t buffer_size = 8192);

protected:
  int_type underflow() override;

private:
  int fd_;
  std::vector<char> buffer_;
};

} // namespace detail
} // namespace vast

#endif
