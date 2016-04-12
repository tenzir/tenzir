#ifndef VAST_CONCEPT_SERIALIZABLE_STATE_HPP
#define VAST_CONCEPT_SERIALIZABLE_STATE_HPP

#include <type_traits>

#include "vast/detail/variadic_serialization.hpp"

#include "vast/access.hpp"

// Generic implementation of CAF's free serialization functions for all types
// which model VAST's State concept. Because the Serializable concept operates
// via ADL, these functions must reside in the namespace of their arguments.
// Because we want to support types across multiple namespaces, but cannot
// inject free functions into their respective namespaces, our only option is
// to place these functions of the first argument, i.e., namespace caf.

namespace caf {
namespace detail {

// Dummy function that faciliates expression SFINAE below.
struct dummy {
  template <class...>
  void operator()(...);
};

} // namespace detail

template <class T>
auto serialize(caf::serializer& sink, T const& x)
-> decltype(vast::access::state<T>::call(x, std::declval<detail::dummy>())) {
  auto f = [&](auto&... xs) { vast::detail::write(sink, xs...); };
  vast::access::state<T>::call(x, f);
}

template <class T>
auto serialize(caf::deserializer& source, T& x)
-> decltype(vast::access::state<T>::call(x, std::declval<detail::dummy>())) {
  auto f = [&](auto&... xs) { vast::detail::read(source, xs...); };
  vast::access::state<T>::call(x, f);
}

template <class T>
auto serialize(caf::serializer& sink, T const& x)
-> decltype(vast::access::state<T>::read(x, std::declval<detail::dummy>())) {
  auto f = [&](auto&... xs) { vast::detail::write(sink, xs...); };
  vast::access::state<T>::read(x, f);
}

template <class T>
auto serialize(caf::deserializer& source, T& x)
-> decltype(vast::access::state<T>::read(x, std::declval<detail::dummy>())) {
  auto f = [&](auto&... xs) { vast::detail::read(source, xs...); };
  vast::access::state<T>::write(x, f);
}

} // namespace caf

#endif
