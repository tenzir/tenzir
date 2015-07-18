#ifndef VAST_CONCEPT_PRINTABLE_PRINT_H
#define VAST_CONCEPT_PRINTABLE_PRINT_H

#include <type_traits>

#include "vast/access.h"
#include "vast/concept/printable/core/printer.h"

namespace vast {

template <typename Iterator, typename T, typename... Args>
auto print(Iterator&& out, T const& x, Args&&... args)
  -> std::enable_if_t<has_printer<T>::value, bool>
{
  static auto p = make_printer<T>{std::forward<Args>(args)...};
  return p.print(out, x);
}

template <typename Iterator, typename T, typename... Args>
auto print(Iterator&& out, T const& x, Args&&... args)
  -> std::enable_if_t<! has_printer<T>::value && has_access_printer<T>::value,
                      bool>
{
  static auto p = access::printer<T>{std::forward<Args>(args)...};
  return p.print(out, x);
}

namespace detail {

template <typename Iterator, typename T>
bool conjunctive_print(Iterator& out, T const& x)
{
  return print(out, x);
}

template <typename Iterator, typename T, typename... Ts>
bool conjunctive_print(Iterator& out, T const& x, Ts const&... xs)
{
  return conjunctive_print(out, x) && conjunctive_print(out, xs...);
}

} // namespace detail

template <typename Iterator, typename T>
auto print(Iterator&& out, T const& x)
  -> std::enable_if_t<! has_printer<T>::value && has_access_state<T>::value,
                      bool>
{
  bool r;
  auto fun = [&](auto&... xs) { r = detail::conjunctive_print(out, xs...); };
  access::state<T>::call(x, fun);
  return r;
}

namespace detail {

struct is_printable
{
  template <typename I, typename T>
  static auto test(I* out, T const* x)
    -> decltype(print(*out, *x), std::true_type());

  template <typename, typename>
  static auto test(...) -> std::false_type;
};

} // namespace detail

template <typename I, typename T>
struct is_printable : decltype(detail::is_printable::test<I, T>(0, 0)) {};

} // namespace vast

#endif
