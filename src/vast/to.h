#ifndef VAST_TO_H
#define VAST_TO_H

#include <string>
#include "vast/traits.h"

namespace vast {

/// Converts a byte value into a character.
template <typename To>
auto to(size_t i) -> EnableIfIsSame<To, char>
{
  return static_cast<To>(i < 10 ? '0' + i : 'a' + i - 10);
}

/// Converts two characters representing a hex byte into a single byte value.
template <typename To>
auto to(char h, char l) -> EnableIfIsSame<To, char>
{
  char byte;
  byte =  (h > '9' ? h - 'a' + 10 : h - '0') << 4;
  byte |= (l > '9' ? l - 'a' + 10 : l - '0');
  return byte;
}

template <
  typename To,
  typename From,
  typename = EnableIf<std::is_arithmetic<From>>
>
auto to(From x) -> EnableIfIsSame<To, std::string>
{
  return std::to_string(x);
}

template <typename T>
std::string to_string(T const& x)
{
  return to<std::string>(x);
}

} // namespace vast

#endif
