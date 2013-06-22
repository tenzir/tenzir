#ifndef VAST_SERIALIZATION_CONTAINER_H
#define VAST_SERIALIZATION_CONTAINER_H

#include <vector>
#include "vast/exception.h"
#include "vast/traits.h"
#include "vast/serialization/arithmetic.h"

namespace vast {

template <typename T, size_t N>
typename std::enable_if<
  std::is_arithmetic<T>::value && ! is_byte<T>::value
>::type
serialize(serializer& sink, T const (&array)[N])
{
  for (auto i = 0u; i < N; ++i)
    sink << array[i];
}

template <typename T, size_t N>
typename std::enable_if<
  std::is_arithmetic<T>::value && ! is_byte<T>::value
>::type
deserialize(deserializer& source, T (&array)[N])
{
  for (auto i = 0u; i < N; ++i)
    source >> array[i];
}

template <typename T, size_t N>
typename std::enable_if<is_byte<T>::value>::type
serialize(serializer& sink, T const (&array)[N])
{
  sink.write_raw(&array, N);
}

template <typename T, size_t N>
typename std::enable_if<is_byte<T>::value>::type
deserialize(deserializer& source, T (&array)[N])
{
  source.read_raw(&array, N);
}

template <typename T>
typename std::enable_if<is_byte<T>::value>::type
serialize(serializer& sink, std::vector<T> const& v)
{
  sink.begin_sequence(v.size());
  if (! v.empty())
    sink.write_raw(v.data(), v.size());
  sink.end_sequence();
}

template <typename T>
typename std::enable_if<is_byte<T>::value>::type
deserialize(deserializer& source, std::vector<T>& v)
{
  uint64_t size;
  source.begin_sequence(size);
  if (size > 0)
  {
    typedef typename std::vector<T>::size_type size_type;
    if (size > std::numeric_limits<size_type>::max())
      throw std::length_error("size too large for architecture");
    v.resize(size);
    source.read_raw(v.data(), size);
  }
  source.end_sequence();
}

template <typename T>
typename std::enable_if<!is_byte<T>::value>::type
serialize(serializer& sink, std::vector<T> const& v)
{
  sink.begin_sequence(v.size());
  for (auto const& x : v)
    sink << x;
  sink.end_sequence();
}

template <typename T>
typename std::enable_if<!is_byte<T>::value>::type
deserialize(deserializer& source, std::vector<T>& v)
{
  uint64_t size;
  source.begin_sequence(size);
  if (size > 0)
  {
    typedef typename std::vector<T>::size_type size_type;
    if (size > std::numeric_limits<size_type>::max())
      throw std::length_error("size too large for architecture");
    v.resize(size);
    for (auto& x : v)
      source >> x;
  }
  source.end_sequence();
}

template <typename T, typename U>
void serialize(serializer& sink, std::pair<T, U> const& pair)
{
  sink << pair.first;
  sink << pair.second;
}

template <typename T, typename U>
void deserialize(deserializer& source, std::pair<T, U>& pair)
{
  source >> pair.first;
  source >> pair.second;
}

} // namespace vast

#endif
