#ifndef VAST_SERIALIZATION_CONTAINER_H
#define VAST_SERIALIZATION_CONTAINER_H

#include <array>
#include <list>
#include <vector>
#include <unordered_map>
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

template <typename T, size_t N>
void serialize(serializer& sink, std::array<T, N> const& a)
{
  for (auto& x : a)
    sink << x;
}

template <typename T, size_t N>
void deserialize(deserializer& source, std::array<T, N>& a)
{
  for (auto& x : a)
    source >> x;
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
    v.clear();
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
    v.clear();
    using size_type = typename std::vector<T>::size_type;
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

template <typename Key, typename T>
void serialize(serializer& sink, std::unordered_map<Key, T> const& map)
{
  sink.begin_sequence(map.size());
  if (! map.empty())
    for (auto& i : map)
      sink << i;
  sink.end_sequence();
}

template <typename Key, typename T>
void deserialize(deserializer& source, std::unordered_map<Key, T>& map)
{
  uint64_t size;
  source.begin_sequence(size);
  if (size > 0)
  {
    map.clear();
    using size_type = typename std::unordered_map<Key, T>::size_type;
    if (size > std::numeric_limits<size_type>::max())
      throw std::length_error("size too large for architecture");
    map.reserve(size);
    for (size_type i = 0; i < size; ++i)
    {
      std::pair<Key, T> p;
      source >> p;
      map.insert(std::move(p));
    }
  }
  source.end_sequence();
}

template <typename T>
void serialize(serializer& sink, std::list<T> const& list)
{
  sink.begin_sequence(list.size());
  if (! list.empty())
    for (auto& i : list)
      sink << i;
  sink.end_sequence();
}

template <typename T>
void deserialize(deserializer& source, std::list<T>& list)
{
  uint64_t size;
  source.begin_sequence(size);
  if (size > 0)
  {
    list.clear();
    using size_type = typename std::list<T>::size_type;
    if (size > std::numeric_limits<size_type>::max())
      throw std::length_error("size too large for architecture");
    for (size_type i = 0; i < size; ++i)
    {
      T x;
      source >> x;
      list.push_back(std::move(x));
    }
  }
  source.end_sequence();
}

} // namespace vast

#endif
