#ifndef VAST_UTIL_PARSE_H
#define VAST_UTIL_PARSE_H

#include <cstdlib>
#include <locale>   // std::isdigit
#include <istream>
#include "vast/access.h"
#include "vast/traits.h"

namespace vast {
namespace util {

/// Converts an iterator range to a positive decimal number.
template <typename Iterator, typename T>
bool parse_positive_decimal(Iterator& start, Iterator end, T& n)
{
  if (! (start != end && std::isdigit(*start)))
    return false;
  n = 0;
  while (start != end && std::isdigit(*start))
  {
    T digit = *start++ - '0';
    n = n * 10 + digit;
  }
  return true;
}

/// Converts an iterator range to a negative decimal number.
template <typename Iterator, typename T>
bool parse_negative_decimal(Iterator& start, Iterator end, T& n)
{
  if (! (start != end && std::isdigit(*start)))
    return false;
  n = 0;
  while (start != end && std::isdigit(*start))
  {
    T digit = *start++ - '0';
    n = n * 10 - digit;
  }
  return true;
}

template <typename T>
bool stream_from(std::istream& in, T& x)
{
  std::istreambuf_iterator<char> start{in}, end;
  return extract(start, end, x);
    throw std::runtime_error("parse error");
}

/// Implements the `Parsable` concept. Requires that derived types provide a
/// member function with the following signature:
///
///      bool parse(Iterator& start, Iterator end)
///
/// This class injects two free functions: `parse` and and overload for
/// `operator>>`.
///
/// @tparam Derived The CRTP client.
///
/// @note Haskell calls this concept the `Read` typeclass.
template <typename Derived>
struct parsable
{
  friend std::istream& operator>>(std::istream& in, Derived& d)
  {
    if (! stream_from(in, d))
      throw std::runtime_error("parse error");
    return in;
  }
};

template <typename Iterator, typename T>
EnableIf<All<std::is_signed<T>, Not<std::is_floating_point<T>>>, bool>
parse_numeric(Iterator& start, Iterator end, T& x)
{
  switch (*start)
  {
    default:
      return false;
    case '-':
        return util::parse_negative_decimal(++start, end, x);
    case '+':
      ++start;
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
      return util::parse_positive_decimal(start, end, x);
  }
}

template <typename Iterator, typename T>
EnableIf<std::is_unsigned<T>, bool>
parse_numeric(Iterator& start, Iterator end, T& x)
{
  return util::parse_positive_decimal(start, end, x);
}

template <typename Iterator, typename T>
EnableIf<std::is_floating_point<T>, bool>
parse_numeric(Iterator& start, Iterator end, T& x, bool* is_double = nullptr)
{
  char buf[64]; // Longest double: ~53 bytes.
  auto p = buf;
  if (*start == '+' || *start == '-')
    *p++ = *start++;
  while (start != end && p < &buf[31])
  {
    if (*start == '.')
    {
      if (is_double)
      {
        *is_double = true;
        is_double = nullptr;
      }
    }
    else if (! std::isdigit(*start))
    {
      break;
    }
    *p++ = *start++;
  }
  *p = '\0';
  x = std::atof(buf);
  return true; // impossible to check result
}

} // namespace util

template <typename Iterator>
bool parse(Iterator& start, Iterator /* end */, bool& x)
{
  switch (*start)
  {
    default:
      return false;
    case 'T':
      x = true;
      break;
    case 'F':
      x = false;
      break;
  }
  ++start;
  return true;
}

#define VAST_DEFINE_PARSE_NUMERIC(type)               \
  template <typename Iterator>                        \
  bool parse(Iterator& begin, Iterator end, type& i)  \
  {                                                   \
    return util::parse_numeric(begin, end, i);        \
  }

VAST_DEFINE_PARSE_NUMERIC(int8_t)
VAST_DEFINE_PARSE_NUMERIC(int16_t)
VAST_DEFINE_PARSE_NUMERIC(int32_t)
VAST_DEFINE_PARSE_NUMERIC(int64_t)
VAST_DEFINE_PARSE_NUMERIC(uint8_t)
VAST_DEFINE_PARSE_NUMERIC(uint16_t)
VAST_DEFINE_PARSE_NUMERIC(uint32_t)
VAST_DEFINE_PARSE_NUMERIC(uint64_t)
VAST_DEFINE_PARSE_NUMERIC(double)

#undef VAST_DEFINE_PARSE_NUMERIC

template <typename Iterator>
bool parse(Iterator& start, Iterator end, std::string& str)
{
  str = {start, end};
  start += end - start;
  return true;
}

struct access::parsable
{
  template <typename Iterator, typename T, typename... Opts>
  static auto read(Iterator& begin, Iterator end, T& x, int, Opts&&... opts)
    -> decltype(x.parse(begin, end, std::forward<Opts>(opts)...), bool())
  {
    return x.parse(begin, end, std::forward<Opts>(opts)...);
  }

  template <typename Iterator, typename T, typename... Opts>
  static auto read(Iterator& begin, Iterator end, T& x, long, Opts&&... opts)
    -> decltype(parse(begin, end, x, std::forward<Opts>(opts)...), bool())
  {
    return parse(begin, end, x, std::forward<Opts>(opts)...);
  }
};

template <typename Iterator, typename T, typename... Opts>
bool extract(Iterator& begin, Iterator end, T& x, Opts&&... opts)
{
  return access::parsable::read(begin, end, x, 0, std::forward<Opts>(opts)...);
}


} // namespace vast

#endif
