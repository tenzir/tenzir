#ifndef VAST_UTIL_TUPLE
#define VAST_UTIL_TUPLE

#include <tuple>
#include <caf/detail/apply_args.hpp>

namespace vast {
namespace util {

template <size_t I = 0, typename F, typename... Ts>
std::enable_if_t<I == sizeof...(Ts)>
static_for_each(F, std::tuple<Ts...>&)
{
}

template <size_t I = 0, typename F, typename... Ts>
std::enable_if_t<I == sizeof...(Ts)>
static_for_each(F, std::tuple<Ts...> const&)
{
}

/// Applies a function to each element in a tuple.
/// @param f The function overloaded for each tuple element.
/// @param t The tuple.
template <size_t I = 0, typename F, typename... Ts>
std::enable_if_t<I < sizeof...(Ts)>
static_for_each(F f, std::tuple<Ts...>& t)
{
  f(std::get<I>(t));
  static_for_each<I + 1, F, Ts...>(f, t);
}

template <size_t I = 0, typename F, typename... Ts>
std::enable_if_t<I < sizeof...(Ts)>
static_for_each(F f, std::tuple<Ts...> const& t)
{
  f(std::get<I>(t));
  static_for_each<I + 1, F, Ts...>(f, t);
}

/// Applies a function to each type in a tuple.
/// @param f The function to apply over the tuple.
/// @param t The tuple
template <typename F, typename... Ts>
auto apply(F f, std::tuple<Ts...> const& t)
{
  return caf::detail::apply_args(f, caf::detail::get_indices(t), t);
}

} // namespace util
} // namespace vast

#endif
