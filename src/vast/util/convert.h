#ifndef VAST_UTIL_CONVERT_H
#define VAST_UTIL_CONVERT_H

#include <string>
#include "vast/util/trial.h"
#include "vast/util/print.h"

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

struct has_push_back
{
  template <typename Container>
  static auto test(Container* c)
    -> decltype(
        c->push_back(typename Container::value_type{}),
        std::true_type());

  template <typename>
  static auto test(...) -> std::false_type;
};

} // namespace detail

/// Type trait that checks whether a type is convertible to another.
template <typename From, typename To>
struct convertible : decltype(detail::convertible::test<From, To>(0, 0)) {};

/// Converts a type that models the `Printable` conept to a std::string.
template <typename From, typename... Opts>
auto convert(From const& f, std::string& str, Opts&&... opts)
  -> typename std::enable_if<
       std::is_arithmetic<From>::value 
         || (! convertible<From, std::string>::value
             && printable<From, std::back_insert_iterator<std::string>>::value),
       trial<void>
     >::type
{
  return print(f, std::back_inserter(str), std::forward<Opts>(opts)...);
}

/// Conversion function for a *printable* type. One should be able to convert
/// any printable type into a type acting as container of characters, such as a
/// std::string or std::vector<char>.
//template <typename To, typename From, typename... Opts>
//auto convert(From const& f, To& to, Opts&&... opts)
//  -> typename std::enable_if<
//         std::is_class<To>::value
//         && decltype(detail::has_push_back::test<To>(0))::value
//         && ! convertible<From, To>::value
//         && printable<From, std::back_insert_iterator<To>>::value,
//       trial<void>
//     >::type
//{
//  return print(f, std::back_inserter(to), std::forward<Opts>(opts)...);
//}

/// Converts one type to another.
/// @tparam To The type to convert `From` to.
/// @tparam From The type to convert to `To`.
/// @param f The instance to convert.
/// @returns *f* converted to `T`.
template <typename To, typename From, typename... Opts>
auto to(From const& f, Opts&&... opts)
  -> decltype(convertible<From, To>(), trial<To>{To{}})
{
  trial<To> x{To()};
  auto t = convert(f, *x, std::forward<Opts>(opts)...);
  if (t)
    return x;
  else
    return t.error();
}

/// Converts a *convertible* to a std::string.
/// This function exists for STL compliance.
template <typename From, typename... Opts>
auto to_string(From const& f, Opts&&... opts)
  -> decltype(convertible<From, std::string>(), std::string())
{
  auto t = to<std::string>(f, std::forward<Opts>(opts)...);
  return t ? *t : std::string{"<" + t.error().msg() + ">"};
}

} // namespace util
} // namespace vast

#endif
