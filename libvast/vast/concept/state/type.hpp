#ifndef VAST_CONCEPT_STATE_TYPE_HPP
#define VAST_CONCEPT_STATE_TYPE_HPP

#include "vast/type.hpp"
#include "vast/util/meta.hpp"

namespace vast {

template <>
struct access::state<type::attribute> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(x.key, x.value);
  }
};

template <>
struct access::state<type::base> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(x.name_, x.attributes_, x.digest_);
  }
};

namespace detail {

template <typename T>
using deduce_base = util::deduce<T, type::base>;

} // namespace detail

template <typename T>
struct access::state<T, std::enable_if_t<std::is_same<T, type::boolean>::value
                          || std::is_same<T, type::integer>::value
                          || std::is_same<T, type::count>::value
                          || std::is_same<T, type::real>::value
                          || std::is_same<T, type::time_point>::value
                          || std::is_same<T, type::time_interval>::value
                          || std::is_same<T, type::time_duration>::value
                          || std::is_same<T, type::time_period>::value
                          || std::is_same<T, type::string>::value
                          || std::is_same<T, type::pattern>::value
                          || std::is_same<T, type::address>::value
                          || std::is_same<T, type::subnet>::value
                          || std::is_same<T, type::port>::value>> {
  template <typename Z, typename F>
  static void call(Z&& x, F f) {
    f(static_cast<detail::deduce_base<decltype(x)>>(x));
  }
};

template <>
struct access::state<type::enumeration> {
  template <typename T, typename F>
  static void call(T& x, F f) {
    f(static_cast<detail::deduce_base<decltype(x)>>(x), x.fields_);
  }
};

template <>
struct access::state<type::vector> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(static_cast<detail::deduce_base<decltype(x)>>(x), x.elem_);
  }
};

template <>
struct access::state<type::set> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(static_cast<detail::deduce_base<decltype(x)>>(x), x.elem_);
  }
};

template <>
struct access::state<type::table> {
  template <typename T, typename F>
  static void call(T& x, F f) {
    f(static_cast<detail::deduce_base<decltype(x)>>(x), x.key_, x.value_);
  }
};

template <>
struct access::state<type::record::field> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(x.name, x.type);
  }
};

template <>
struct access::state<type::record> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(static_cast<detail::deduce_base<decltype(x)>>(x), x.fields_);
  }
};

template <>
struct access::state<type::alias> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(static_cast<detail::deduce_base<decltype(x)>>(x), x.type_);
  }
};

template <>
struct access::state<type> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(expose(*x.info_));
  }
};

} // namespace vast

#endif
