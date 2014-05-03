#ifndef VAST_UTIL_PRINT_H
#define VAST_UTIL_PRINT_H

#include <cmath>
#include <cstdio>
#include <cstring>
#include <ostream>
#include "vast/access.h"
#include "vast/util/coding.h"

namespace vast {

template <typename Iterator, typename T, typename... Opts>
bool render(Iterator&, T const&, Opts&&...);

template <typename T, typename... Opts>
bool stream_to(std::ostream& out, T const& x, Opts&&... opts)
{
  std::ostreambuf_iterator<char> i{out};
  return render(i, x, std::forward<Opts>(opts)...);
}

namespace util {

/// Allows classes to model the `Printable` concept. Requires that derived
/// types provide a member function with the following signature:
///
///      bool print(Iterator& out)
///
/// This class injects two free functions: `print` and an overload for
/// `operator<<`.
///
/// @tparam Derived The CRTP client.
///
/// @tparam Char The character type of the out stream.
///
/// @note Haskell calls this concept the `Show` typeclass.
template <typename Derived>
struct printable
{
  friend std::ostream& operator<<(std::ostream& out, Derived const& d)
  {
    if (! stream_to(out, d))
      throw std::runtime_error("print error");
    return out;
  }
};

} // namespace util

template <typename Iterator>
bool print(Iterator& out, bool b)
{
  *out++ = b ? 'T' : 'F';
  return true;
}

namespace detail {

template <typename Iterator, typename T>
bool print_integral(Iterator& out, T n)
{
  return render(out, std::to_string(n));
  // TODO: Switch to a custom formatter.
  //while (n > 10)
  //{
  //  auto magnitude = std::floor(std::log(n) / std::log(10));
  //  T nearest_power_of_10 = std::pow(10, magnitude);
  //  T digit = n / nearest_power_of_10;
  //  *out++ = util::byte_to_char(digit);
  //  n /= 10;
  //}
  //*out++ = util::byte_to_char(n);
  //return true;
}

template <typename Iterator, typename T>
EnableIf<All<std::is_signed<T>, Not<std::is_floating_point<T>>>, bool>
print_numeric(Iterator& out, T n)
{
  if (n >= 0)
    *out++ = '+';
  return print_integral(out, n);
}

template <typename Iterator, typename T>
EnableIf<std::is_unsigned<T>, bool>
print_numeric(Iterator& out, T n)
{
  return print_integral(out, n);
}


template <typename Iterator, typename T>
EnableIf<std::is_floating_point<T>, bool>
print_numeric(Iterator& out, T n)
{
  // TODO: do this manually at some point. We can't use std::to_string as
  // intermediate solution because it doesn't allow for setting precision.
  char buf[64];
  std::memset(buf, 64, 0);
  std::snprintf(buf, 64, "%.10f", n);
  auto p = buf;
  while (*p != '\0')
    *out++ = *p++;
  return true;
}

} // namespace detail

#define VAST_DEFINE_PRINT_NUMERIC(type)     \
  template <typename Iterator>              \
  bool print(Iterator& out, type i)         \
  {                                         \
    return detail::print_numeric(out, i);   \
  }

VAST_DEFINE_PRINT_NUMERIC(int8_t)
VAST_DEFINE_PRINT_NUMERIC(int16_t)
VAST_DEFINE_PRINT_NUMERIC(int32_t)
VAST_DEFINE_PRINT_NUMERIC(int64_t)
VAST_DEFINE_PRINT_NUMERIC(uint8_t)
VAST_DEFINE_PRINT_NUMERIC(uint16_t)
VAST_DEFINE_PRINT_NUMERIC(uint32_t)
VAST_DEFINE_PRINT_NUMERIC(uint64_t)
VAST_DEFINE_PRINT_NUMERIC(double)
VAST_DEFINE_PRINT_NUMERIC(float)

#undef VAST_DEFINE_PRINT_NUMERIC

template <typename Iterator>
bool print(Iterator& out, char const* str)
{
  while (*str != '\0')
    *out++ = *str++;
  return true;
}

template <typename Iterator, size_t N>
bool print(Iterator& out, char str[N])
{
  out = std::copy(str, str + N, out);
  return true;
}

template <typename Iterator>
bool print(Iterator& out, std::string const& str)
{
  out = std::copy(str.begin(), str.end(), out);
  return true;
}

struct access::printable
{
  template <typename Iterator, typename T, typename... Opts>
  static auto show(Iterator& out, T const& x, int, Opts&&... opts)
    -> decltype(x.print(out, std::forward<Opts>(opts)...), bool())
  {
    return x.print(out, std::forward<Opts>(opts)...);
  }

  template <typename Iterator, typename T, typename... Opts>
  static auto show(Iterator& out, T const& x, long, Opts&&... opts)
    -> decltype(print(out, x, std::forward<Opts>(opts)...), bool())
  {
    return print(out, x, std::forward<Opts>(opts)...);
  }
};

template <typename Iterator, typename T, typename... Opts>
bool render(Iterator& out, T const& x, Opts&&... opts)
{
  return access::printable::show(out, x, 0, std::forward<Opts>(opts)...);
}

} // namespace vast

#endif

