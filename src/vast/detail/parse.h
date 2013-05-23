#ifndef VAST_DETAIL_PARSE_H
#define VAST_DETAIL_PARSE_H

#include <locale>   // std::isdigit

namespace vast {
namespace detail {

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

/// Parses a double from string.
double to_double(char const* str);

} // namespace detail
} // namespace vast

#endif
