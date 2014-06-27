#ifndef VAST_UTIL_PRINT_H
#define VAST_UTIL_PRINT_H

#include <cmath>
#include <cstdio>
#include <algorithm>
#include <ostream>
#include <string>
#include <type_traits>
#include <vector>
#include "vast/util/trial.h"
#include "vast/util/coding.h"

namespace vast {
namespace util {

// Forward declaration
namespace detail { struct dummy; }
template <typename T, typename I, typename... Opts>
auto print(T const&, I&&, Opts&&...)
  -> std::enable_if_t<std::is_same<T, detail::dummy>::value, trial<void>>;

//
// Implementation for built-in types and STL.
//

template <typename T, typename I>
trial<void> print_numeric(T, I&&);

template <typename D, typename I, typename O>
trial<void> print_delimited(D const&, I, I, O&&);


template <typename Iterator>
trial<void> print(char c, Iterator&& out)
{
  *out++ = c;
  return nothing;
}

template <typename Iterator>
trial<void> print(bool b, Iterator&& out)
{
  return print(b ? 'T' : 'F', out);
}

template <typename Iterator, size_t N>
trial<void> print(char str[N], Iterator&& out)
{
  out = std::copy(str, str + N, out);
  return nothing;
}

template <typename Iterator>
trial<void> print(char const* str, Iterator&& out)
{
  while (*str != '\0')
    *out++ = *str++;
  return nothing;
}

template <typename Iterator>
trial<void> print(char* str, Iterator&& out)
{
  return print(const_cast<char const*>(str), out);
}

template <typename Iterator>
trial<void> print(std::string const& str, Iterator&& out)
{
  out = std::copy(str.begin(), str.end(), out);
  return nothing;
}

template <typename Iterator, typename T, typename Allocator>
trial<void> print(std::vector<T, Allocator> const& v, Iterator&& out,
                  std::string const& delim = ", ")
{
  return print_delimited(delim, v.begin(), v.end(), out);
}

template <typename T, typename Iterator>
auto print(T n, Iterator&& out)
  -> std::enable_if_t<
      std::is_signed<T>::value && ! std::is_floating_point<T>::value,
      trial<void>
    >
{
  if (n < 0)
  {
    print('-', out);
    return print_numeric(-n, out);
  }

  print('+', out);
  return print_numeric(n, out);
}

template <typename T, typename Iterator>
auto print(T n, Iterator&& out)
  -> std::enable_if_t<std::is_unsigned<T>::value, trial<void>>
{
  return print_numeric(n, out);
}

template <typename T, typename Iterator>
auto print(T n, Iterator&& out, size_t digits = 10)
  -> std::enable_if_t<std::is_floating_point<T>::value, trial<void>>
{
  if (n == 0)
    return print("0.0000000000", out);

  if (n < 0)
  {
    print('-', out);
    n = -n;
  }

  double left;
  uint64_t right = std::round(std::modf(n, &left) * std::pow(10, digits));

  print_numeric(static_cast<uint64_t>(left), out);
  print('.', out);
  print_numeric(right, out);

  return nothing;
}

//
// Helpers
//

/// Prints a numeric type.
template <typename T, typename Iterator>
trial<void> print_numeric(T n, Iterator&& out)
{
  if (n == 0)
    return print('0', out);

  char buf[std::numeric_limits<T>::digits10 + 1];
  auto p = buf;
  while (n > 0)
  {
    *p++ = util::byte_to_char(n % 10);
    n /= 10;
  }

  out = std::reverse_copy(buf, p, out);
  return nothing;
}

/// Prints a delimited Iterator range.
template <typename Delim, typename I, typename O>
trial<void> print_delimited(Delim const& delim, I begin, I end, O&& out)
{
  while (begin != end)
  {
    auto t = print(*begin++, out);
    if (! t)
      return t.error();

    if (begin != end)
    {
      t = print(delim, out);
      if (! t)
        return t.error();
    }
  }

  return nothing;
}

//
// The following definitions exist here because it would otherwise be
// impossible to make use of the above defined (built-in) overloads.
//

template <typename T>
void error::render(T&& x)
{
  print(x, std::back_inserter(msg_));
}

template <typename T, typename... Ts>
void error::render(T&& x, Ts&&... xs)
{
  render(std::forward<T>(x));
  render(' ');
  render(std::forward<Ts>(xs)...);
}

template <typename Iterator>
trial<void> print(error const& e, Iterator&& out)
{
  return print(e.msg(), out);
}

//
// Printable concept
//

namespace detail {

struct printable
{
  template <typename T, typename I>
  static auto test(T const* x, I*)
    -> decltype(print(*x, std::declval<I&&>()), std::true_type());

  template <typename, typename>
  static auto test(...) -> std::false_type;
};

struct streamable
{
  template <typename Stream, typename T>
  static auto test(Stream* out, T const* x)
    -> decltype(*out << *x, std::true_type());

  template <typename, typename>
  static auto test(...) -> std::false_type;
};

} // namespace detail

/// Type trait that checks whether a type is printable.
template <typename T, typename I>
struct printable : decltype(detail::printable::test<T, I>(0, 0)) {};

/// Type trait that checks whether a type is streamable via `operator<<`.
template <typename Stream, typename T>
struct streamable : decltype(detail::streamable::test<Stream, T>(0, 0)) {};

// Injects operator<< for all printable types that do not already have an
// overload of operator<< to a std::basic_ostream.
template <typename CharT, typename Traits, typename T>
auto operator<<(std::basic_ostream<CharT, Traits>& out, T const& x)
  -> std::enable_if_t<
       printable<T, std::ostreambuf_iterator<CharT>>::value
         && ! streamable<decltype(out), T>::value,
       decltype(out)
     >
{
  static_assert(printable<T, std::ostreambuf_iterator<CharT>>::value,
                "T must be a printable type");

  if (! print(x, std::ostreambuf_iterator<CharT>{out}))
    out.setstate(std::ios_base::failbit);

  return out;
}

template <typename To, typename From, typename... Opts>
auto to(From const& f, Opts&&... opts)
  -> std::enable_if_t<
       std::is_same<To, std::string>::value
         && ! std::is_same<From, std::string>::value,
       trial<std::string>
     >
{
  trial<std::string> str{std::string{}};
  auto t = print(f, std::back_inserter(*str), std::forward<Opts>(opts)...);
  if (! t)
    return t.error();
  else
    return str;
}

/// Converts a type modeling the Printable concept to a std::string.
/// This function exists for STL compliance.
template <typename From, typename... Opts>
std::string to_string(From const& f, Opts&&... opts)
{
  auto t = to<std::string>(f, std::forward<Opts>(opts)...);
  return t ? *t : std::string{"<" + t.error().msg() + ">"};
}

} // namespace util
} // namespace vast

#endif
