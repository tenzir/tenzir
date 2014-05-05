#ifndef VAST_UTIL_ERROR_H
#define VAST_UTIL_ERROR_H

#include <string>
#include "vast/util/operators.h"

namespace vast {
namespace util {

template <typename T>
class trial;

/// Holds an error message.
class error : totally_ordered<error>
{
public:
  /// Default-constructs an empty error message.
  error() = default;

  /// Contructs an error from a sequence of arguments. The arguments get
  /// rendered space-separated into the error message.
  /// @param args The arguments to render as std::string.
  template <typename...Args>
  explicit error(Args&&... args)
  {
    render(std::forward<Args>(args)...);
  }

  error(error const&) = default;
  error(error&&) = default;
  error& operator=(error const&) = default;
  error& operator=(error&&) = default;

  /// Retrieves the error message.
  /// @returns The error string.
  std::string const& msg() const
  {
    return msg_;
  }

private:
  // These two functions are defined in vast/util/print.h so that they can make
  // use of the the built-in overloads for print().
  template <typename T>
  void render(T&& x);

  template <typename T, typename... Ts>
  void render(T&& x, Ts&&... xs);

  std::string msg_;

private:
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
} // namespace vast

#endif
