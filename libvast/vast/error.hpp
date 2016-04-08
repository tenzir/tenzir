#ifndef VAST_ERROR_HPP
#define VAST_ERROR_HPP

#include <string>

#include "vast/concept/printable/print.hpp"
#include "vast/concept/printable/numeric.hpp"
#include "vast/concept/printable/string/any.hpp"
#include "vast/concept/printable/string/string.hpp"
#include "vast/util/operators.hpp"

namespace vast {

struct access;

/// An untyped error message.
class error : util::totally_ordered<error>, util::addable<error> {
  friend access;

public:
  /// Default-constructs an empty error message.
  error() = default;

  /// Contructs an error from a sequence of arguments. The arguments get
  /// rendered space-separated into the error message.
  /// @param args The arguments to render as std::string.
  template <typename... Args>
  explicit error(Args&&... args) {
    render(std::forward<Args>(args)...);
  }

  error(error const&) = default;
  error(error&&) = default;
  error& operator=(error const&) = default;
  error& operator=(error&&) = default;

  error& operator+=(error const& e) {
    msg_ += ' ' + e.msg_;
    return *this;
  }

  /// Retrieves the error message.
  /// @returns The error string.
  std::string const& msg() const {
    return msg_;
  }

  friend bool operator<(error const& x, error const& y) {
    return x.msg_ < y.msg_;
  }

  friend bool operator==(error const& x, error const& y) {
    return x.msg_ == y.msg_;
  }

private:
  template <typename T>
  void render(T&& x) {
    auto i = std::back_inserter(msg_);
    print(i, std::forward<T>(x));
  }

  template <typename T, typename... Ts>
  void render(T&& x, Ts&&... xs) {
    render(std::forward<T>(x));
    // render(' ');
    render(std::forward<Ts>(xs)...);
  }

  std::string msg_;
};

} // namespace vast

#endif
