#ifndef VAST_UTIL_TUPLE
#define VAST_UTIL_TUPLE

namespace vast {
namespace util {

template <size_t I = 0, typename F, typename... Ts>
typename std::enable_if<I == sizeof...(Ts)>::type
for_each(std::tuple<Ts...> &, F)
{
  // Base case.
}

template <size_t I = 0, typename F, typename... Ts>
typename std::enable_if<I < sizeof...(Ts)>::type
for_each(std::tuple<Ts...>& t, F f)
{
  f(std::get<I>(t));
  for_each<I + 1, F, Ts...>(t, f);
}

} // namespace util
} // namespace vast

#endif
