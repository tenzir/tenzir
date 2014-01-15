#ifndef VAST_UTIL_ERROR_H
#define VAST_UTIL_ERROR_H

#include <string>
#include "vast/util/operators.h"
#include "vast/util/print.h"

namespace vast {
namespace util {

/// Holds an error message.
class error : printable<error>, totally_ordered<error>
{
public:
  /// Default-constructs an empty error message.
  error() = default;

  /// Constructs an error from a C-string.
  /// @param msg The error message.
  explicit error(char const* msg)
    : msg_{msg}
  {
  }

  /// Constructs an error from a C++ string.
  /// @param msg The error message.
  explicit error(std::string msg)
    : msg_{std::move(msg)}
  {
  }

  error(error const&) = default;
  error(error&&) = default;
  error& operator=(error const&) = default;
  error& operator=(error&&) = default;

  explicit operator std::string() const
  {
    return msg_;
  }

  /// Retrieves the error message.
  /// @returns The error string.
  std::string const& msg() const
  {
    return msg_;
  }

private:
  std::string msg_;

private:
  friend access;

  template <typename Iterator>
  bool print(Iterator& out) const
  {
    return render(out, msg_);
  }

  friend bool operator<(error const& x, error const& y)
  {
    return x.msg_ < y.msg_;
  }

  friend bool operator==(error const& x, error const& y)
  {
    return x.msg_ == y.msg_;
  }
};

} // namespace util

using util::error;

} // namespace vast

#endif
