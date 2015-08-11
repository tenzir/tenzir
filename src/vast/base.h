#ifndef VAST_BASE_H
#define VAST_BASE_H

#include <cstdint>
#include <array>
#include <limits>

#include "vast/util/math.h"

namespace vast {
namespace detail {

template <size_t X>
constexpr bool is_well_defined() {
  return X >= 2;
}

template <size_t X0, size_t X1, size_t... Xs>
constexpr bool is_well_defined() {
  return is_well_defined<X0>() && is_well_defined<X1, Xs...>();
}

template <size_t X>
constexpr bool is_tractable() {
  return X < (1ull << 30);
}

template <size_t X0, size_t X1, size_t... Xs>
constexpr bool is_tractable() {
  return is_tractable<X0>() && is_tractable<X1, Xs...>();
}

template <size_t>
constexpr bool is_uniform() {
  return true;
}

template <size_t X0, size_t X1, size_t... Xs>
constexpr bool is_uniform() {
  return X0 == X1 && is_uniform<X1, Xs...>();
}

} // namespace detail

/// A generic base.
/// @tparam Xs The base values.
template <size_t... Xs>
struct base {
  static_assert(detail::is_well_defined<Xs...>(), "not a well-defined base");
  static_assert(detail::is_tractable<Xs...>(), "base has untractable values");
  static constexpr bool uniform = detail::is_uniform<Xs...>();
  static constexpr size_t components = sizeof...(Xs);
  static constexpr std::array<size_t, sizeof...(Xs)> values = {{Xs...}};
};

template <size_t... Xs>
constexpr bool base<Xs...>::uniform;

template <size_t... Xs>
constexpr size_t base<Xs...>::components;

template <size_t... Xs>
constexpr std::array<size_t, sizeof...(Xs)> base<Xs...>::values;

namespace detail {

template <size_t N, size_t X, size_t... Xs>
struct uniform_base_rec : uniform_base_rec<N - 1, X, X, Xs...> {};

template <size_t X, size_t... Xs>
struct uniform_base_rec<0, X, Xs...> {
  using type = base<Xs...>;
};

} // namespace deatil

/// A uniform base where each value is the same.
/// @tparam X The base value.
/// @tparam N The number of components.
template <size_t X, size_t N>
using uniform_base = typename detail::uniform_base_rec<N, X>::type;

template <size_t B, typename T>
using make_uniform_base = uniform_base<
  B, util::ilog<static_cast<int>(B)>(~0ull >> (64 - (sizeof(T) * 8))) + 1
>;

template <typename T>
using make_singleton_base = base<(1ull << sizeof(T) * 8)>;

template <typename Base>
constexpr bool is_uniform2() {
  return Base::uniform && Base::values[0] == 2;
}

} // namespace vast

#endif
