#ifndef VAST_CONVERT_H
#define VAST_CONVERT_H

#include <stdexcept>
#include "vast/string.h"
#include "vast/util/print.h"

namespace vast {

inline bool convert(bool b, std::string& to)
{
  to = b ? 'T' : 'F';
  return true;
}

template <typename From>
EnableIf<std::is_arithmetic<From>, bool>
convert(From from, std::string& to)
{
  to = std::to_string(from);
  return true;
}

struct access::convertible
{
  template <typename From, typename To, typename... Opts>
  static auto conv(From const& from, To& to, int, Opts&&... opts)
    -> decltype(from.convert(to, std::forward<Opts>(opts)...), bool())
  {
    return from.convert(to, std::forward<Opts>(opts)...);
  }

  template <typename From, typename To, typename... Opts>
  static auto conv(From const& from, To& to, long, Opts&&... opts)
    -> decltype(convert(from, to, std::forward<Opts>(opts)...), bool())
  {
    return convert(from, to, std::forward<Opts>(opts)...);
  }
};

// Any type modeling the `Printable` concept should also be convertible to a
// string.
template <typename From, typename... Opts>
auto convert(From const& from, std::string& str, Opts&&... opts)
  -> decltype(
      render(std::declval<std::back_insert_iterator<std::string>&>(),
             from,
             std::forward<Opts>(opts)...))
{
  str.clear();
  auto i = std::back_inserter(str);
  return render(i, from, std::forward<Opts>(opts)...);
}

template <typename From, typename... Opts>
bool convert(From const& from, string& str, Opts&&... opts)
{
  std::string tmp;
  if (! convert(from, tmp, std::forward<Opts>(opts)...))
    return false;
  str = {tmp};
  return true;
}

// A single-argument convenience shortcut for conversion.
template <typename To, typename From, typename... Opts>
auto to(From const& from, Opts&&... opts)
  -> decltype(access::convertible::conv(from, std::declval<To&>(), 0, opts...),
              To())
{
  To x;
  if (! access::convertible::conv(from, x, 0, std::forward<Opts>(opts)...))
    throw std::invalid_argument("conversion error");
  return x;
}

// STL compliance.
template <typename From, typename... Opts>
auto to_string(From const& from, Opts&&... opts)
  -> decltype(convert(from, std::declval<std::string&>(), opts...),
              std::string())
{
  return to<std::string>(from, std::forward<Opts>(opts)...);
}

} // namespace vast

#endif
