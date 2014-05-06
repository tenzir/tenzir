#ifndef VAST_UTIL_CONVERT_H
#define VAST_UTIL_CONVERT_H

#include <string>
#include "vast/util/trial.h"

namespace vast {
namespace util {

namespace detail {

struct convertible
{
  template <typename From, typename To>
  static auto test(From*, To*)
    -> decltype(
        convert(std::declval<From const&>(), std::declval<To&>()),
        std::true_type());

  template <typename, typename>
  static auto test(...) -> std::false_type;
};

} // namespace detail

/// Type trait that checks whether a type is convertible to another.
template <typename From, typename To>
struct convertible : decltype(detail::convertible::test<From, To>(0, 0)) {};

/// Converts one type to another.
/// @tparam To The type to convert `From` to.
/// @tparam From The type to convert to `To`.
/// @param f The instance to convert.
/// @returns *f* converted to `T`.
template <typename To, typename From, typename... Opts>
auto to(From const& f, Opts&&... opts)
  -> typename std::enable_if<
       ! std::is_same<To, std::string>::value // Comes from Printable.
         && convertible<From, To>::value,
       trial<To>
     >::type
{
  trial<To> x{To()};
  auto t = convert(f, *x, std::forward<Opts>(opts)...);
  if (t)
    return x;
  else
    return t.error();
}

} // namespace util
} // namespace vast

#endif
