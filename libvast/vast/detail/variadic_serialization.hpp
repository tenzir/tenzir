#ifndef VAST_DETAIL_VARIADIC_SERIALIZATION_HPP
#define VAST_DETAIL_VARIADIC_SERIALIZATION_HPP

#include <caf/deserializer.hpp>
#include <caf/serializer.hpp>

namespace vast {
namespace detail {

// Variadic helpers to interface with CAF's serialization framework.

template <class Processor, class T>
void process(Processor& proc, T&& x) {
  proc & const_cast<T&>(x); // CAF promises not to touch it.
}

template <class Processor, class T, class... Ts>
void process(Processor& proc, T&& x, Ts&&... xs) {
  process(proc, std::forward<T>(x));
  process(proc, std::forward<Ts>(xs)...);
}

template <class T>
void write(caf::serializer& sink, T const& x) {
  sink << x;
}

template <class T, class... Ts>
void write(caf::serializer& sink, T const& x, Ts const&... xs) {
  write(sink, x);
  write(sink, xs...);
}

template <class T>
void read(caf::deserializer& source, T& x) {
  source >> x;
}

template <class T, class... Ts>
void read(caf::deserializer& source, T& x, Ts&... xs) {
  read(source, x);
  read(source, xs...);
}

} // namespace detail
} // namespace vast

#endif
