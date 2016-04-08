#ifndef VAST_CONCEPT_PARSEABLE_PARSE_HPP
#define VAST_CONCEPT_PARSEABLE_PARSE_HPP

#include <type_traits>

#include "vast/access.hpp"
#include "vast/concept/parseable/core/parser.hpp"

namespace vast {

template <typename Iterator, typename T, typename... Args>
auto parse(Iterator& f, Iterator const& l, T& x, Args&&... args)
  -> std::enable_if_t<has_parser<T>::value, bool> {
  return make_parser<T>{std::forward<Args>(args)...}.parse(f, l, x);
}

template <typename Iterator, typename T, typename... Args>
auto parse(Iterator& f, Iterator const& l, T& x, Args&&... args)
  -> std::enable_if_t<!has_parser<T>::value && has_access_parser<T>::value,
                      bool> {
  return access::parser<T>{std::forward<Args>(args)...}.parse(f, l, x);
}

namespace detail {

template <typename Iterator, typename T>
bool conjunctive_parse(Iterator& f, Iterator const& l, T& x) {
  return parse(f, l, x);
}

template <typename Iterator, typename T, typename... Ts>
bool conjunctive_parse(Iterator& f, Iterator const& l, T& x, Ts&... xs) {
  return conjunctive_parse(f, l, x) && conjunctive_parse(f, l, xs...);
}

} // namespace detail

template <typename Iterator, typename T>
auto parse(Iterator& f, Iterator const& l, T& x)
  -> std::enable_if_t<!has_parser<T>::value && has_access_state<T>::value,
                      bool> {
  bool r;
  auto fun = [&](auto&... xs) { r = detail::conjunctive_parse(f, l, xs...); };
  access::state<T>::call(x, fun);
  return r;
}

namespace detail {

struct is_parseable {
  template <typename I, typename T>
  static auto test(I* f, I const* l, T* x)
    -> decltype(parse(*f, *l, *x), std::true_type());

  template <typename, typename>
  static auto test(...) -> std::false_type;
};

} // namespace detail

template <typename I, typename T>
struct is_parseable : decltype(detail::is_parseable::test<I, T>(0, 0, 0)) {};

} // namespace vast

#endif
