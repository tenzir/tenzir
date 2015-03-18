#ifndef VAST_CONCEPT_SERIALIZABLE_STATE_H
#define VAST_CONCEPT_SERIALIZABLE_STATE_H

#include <type_traits>

#include "vast/access.h"

namespace vast {
namespace detail {

// Faciliates expression SFINAE below.
struct dummy { template <typename...> void operator()(...); };

} // namespace detail

//
// Versionized
//

template <typename Serializer, typename T>
auto serialize(Serializer& sink, T const& x, uint32_t version)
  -> decltype(access::state<T>::call(x, std::declval<detail::dummy>(), 0))
{
  access::state<T>::call(x, [&](auto&... xs) { sink.put(xs...); }, version);
}

template <typename Serializer, typename T>
auto serialize(Serializer& sink, T const& x, uint32_t version)
  -> decltype(access::state<T>::read(x, std::declval<detail::dummy>(), 0))
{
  access::state<T>::read(x, [&](auto&... xs) { sink.put(xs...); }, version);
}

template <typename Deserializer, typename T>
auto deserialize(Deserializer& source, T& x, uint32_t version)
  -> decltype(access::state<T>::call(x, std::declval<detail::dummy>(), 0))
{
  access::state<T>::call(x, [&](auto&... xs) { source.get(xs...); }, version);
}

template <typename Deserializer, typename T>
auto deserialize(Deserializer& source, T& x, uint32_t version)
  -> decltype(access::state<T>::write(x, std::declval<detail::dummy>(), 0))
{
  access::state<T>::write(x, [&](auto&... xs) { source.get(xs...); }, version);
}

//
// Un-versionized
//

template <typename Serializer, typename T>
auto serialize(Serializer& sink, T const& x)
  -> decltype(access::state<T>::call(x, std::declval<detail::dummy>()))
{
  access::state<T>::call(x, [&](auto&... xs) { sink.put(xs...); });
}

template <typename Serializer, typename T>
auto serialize(Serializer& sink, T const& x)
  -> decltype(access::state<T>::read(x, std::declval<detail::dummy>()))
{
  access::state<T>::read(x, [&](auto&... xs) { sink.put(xs...); });
}

template <typename Deserializer, typename T>
auto deserialize(Deserializer& source, T& x)
  -> decltype(access::state<T>::call(x, std::declval<detail::dummy>()))
{
  access::state<T>::call(x, [&](auto&... xs) { source.get(xs...); });
}

template <typename Deserializer, typename T>
auto deserialize(Deserializer& source, T& x)
  -> decltype(access::state<T>::write(x, std::declval<detail::dummy>()))
{
  access::state<T>::write(x, [&](auto&... xs) { source.get(xs...); });
}

} // namespace vast

#endif
