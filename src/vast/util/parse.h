#ifndef VAST_UTIL_PARSE_H
#define VAST_UTIL_PARSE_H

#include <cstdlib>
#include <locale>   // std::isdigit
#include <istream>
#include "vast/util/trial.h"

namespace vast {
namespace util {

// Forward declaration
namespace detail { struct dummy; }
template <typename T, typename I, typename... Opts>
auto parse(T&, I&, I, Opts&&...)
  -> std::enable_if_t<
       std::is_same<T, detail::dummy>::value,
       trial<void>
     >;

/// A type alias helper that acts as return type for ::parse functions.
template <bool B>
using need = std::enable_if_t<B, trial<void>>;

//
// Implementation for built-in types and STL.
//

/// Converts an iterator range to a positive decimal number.
template <typename T, typename Iterator>
trial<void> parse_positive_decimal(T& n, Iterator& begin, Iterator end)
{
  if (begin == end)
    return error{"empty iterator range"};

  if (! std::isdigit(*begin))
    return error{"not a digit:", *begin};

  n = 0;
  while (begin != end && std::isdigit(*begin))
  {
    T digit = *begin++ - '0';
    n = n * 10 + digit;
  }

  return nothing;
}

/// Converts an iterator range to a negative decimal number.
template <typename T, typename Iterator>
trial<void> parse_negative_decimal(T& n, Iterator& begin, Iterator end)
{
  if (begin == end)
    return error{"empty iterator range"};

  if (! std::isdigit(*begin))
    return error{"not a digit:", *begin};

  n = 0;
  while (begin != end && std::isdigit(*begin))
  {
    T digit = *begin++ - '0';
    n = n * 10 - digit;
  }

  return nothing;
}

template <typename T, typename Iterator>
need<std::is_signed<T>::value && ! std::is_floating_point<T>::value>
parse(T& n, Iterator& begin, Iterator end)
{
  switch (*begin)
  {
    default:
      return error{"not a digit: ", *begin};
    case '-':
      return parse_negative_decimal(n, ++begin, end);
    case '+':
      ++begin;
    case '0':
    case '1':
    case '2':
    case '3':
    case '4':
    case '5':
    case '6':
    case '7':
    case '8':
    case '9':
      return parse_positive_decimal(n, begin, end);
  }
}

template <typename T, typename Iterator>
need<std::is_unsigned<T>::value>
parse(T& n, Iterator& begin, Iterator end)
{
  return parse_positive_decimal(n, begin, end);
}

template <typename T, typename Iterator>
need<std::is_floating_point<T>::value>
parse(T& n, Iterator& begin, Iterator end, bool* is_double = nullptr)
{
  if (begin == end)
    return error{"empty iterator range"};

  char buf[64]; // Longest double: ~53 bytes.
  auto p = buf;

  // Skip sign.
  if (*begin == '+' || *begin == '-')
    *p++ = *begin++;

  while (begin != end && p < &buf[31])
  {
    if (*begin == '.')
    {
      if (is_double)
      {
        *is_double = true;
        is_double = nullptr;
      }
    }
    else if (! std::isdigit(*begin))
    {
      break;
    }

    *p++ = *begin++;
  }

  if (is_double)
    *is_double = false;

  *p = '\0';

  // TODO: Don't be lazy and parse manually!
  n = std::atof(buf);
  return nothing;
}

template <typename Iterator>
trial<void> parse(bool& b, Iterator& begin, Iterator end)
{
  if (begin == end)
    return error{"empty iterator range"};

  switch (*begin++)
  {
    default:
      return error{"not a boolean:", *begin};
    case 'T':
      b = true;
      break;
    case 'F':
      b = false;
      break;
  }

  return nothing;
}

template <typename Iterator>
trial<void> parse(std::string& str, Iterator& begin, Iterator end)
{
  if (begin == end)
    return error{"empty iterator range"};

  std::copy(begin, end, std::back_inserter(str));
  return nothing;
}

template <size_t N, typename Iterator>
trial<void> parse(char (&buf)[N], Iterator& begin, Iterator end)
{
  if (begin == end)
    return error{"empty iterator range"};

  size_t i = 0;
  while (i < N && begin != end)
    buf[i++] = *begin++;

  return nothing;
}

//
// Parseable concept
//

namespace detail {

struct parseable
{
  template <typename T, typename Iterator>
  static auto test(T* x, Iterator*)
  -> decltype(parse(*x, std::declval<Iterator&>(), std::declval<Iterator>()),
              std::true_type());

  template <typename, typename>
  static auto test(...) -> std::false_type;
};

} // namespace detail

/// Type trait that checks whether a type is parseable.
template <typename T, typename Iterator>
struct parseable : decltype(detail::parseable::test<T, Iterator>(0, 0)) {};

// Injects operator>> for all parseable types.
template <typename CharT, typename Traits, typename T>
auto operator>>(std::basic_istream<CharT, Traits>& in, T& x)
  -> std::enable_if_t<
       parseable<T, std::istreambuf_iterator<CharT>>::value,
       decltype(in)
     >
{
  std::istreambuf_iterator<CharT> begin{in}, end;
  if (parse(x, begin, end))
    in.setstate(std::ios_base::failbit);

  return in;
}

template <typename T, typename Iterator, typename... Opts>
trial<T> parse(Iterator& begin, Iterator end, Opts&&... opts)
{
  T x;
  auto t = parse(x, begin, end, std::forward<Opts>(opts)...);
  if (t)
    return std::move(x);
  else
    return t.error();
}

template <typename T, typename... Opts>
trial<T> to(std::string const& str, Opts&&... opts)
{
  auto first = str.begin();
  return parse<T>(first, str.end(), std::forward<Opts>(opts)...);
}

template <typename T, size_t N, typename... Opts>
trial<T> to(char const (&str)[N], Opts&&... opts)
{
  auto first = str;
  auto last = str + N - 1; // No NUL byte.
  return parse<T>(first, last, std::forward<Opts>(opts)...);
}

} // namespace util
} // namespace vast

#endif
