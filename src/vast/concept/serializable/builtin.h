#ifndef VAST_CONCEPT_SERIALIZABLE_BUILTIN_H
#define VAST_CONCEPT_SERIALIZABLE_BUILTIN_H

#include <cstdint>
#include <type_traits>

namespace vast {

//
// Arithmetic types
//

template <typename Serializer>
void serialize(Serializer& sink, bool b)
{
  sink.write(b ? uint8_t{1} : uint8_t{0});
}

template <typename Deserializer>
void deserialize(Deserializer& source, bool& b)
{
  uint8_t x;
  source.read(x);
  b = !! x;
}

template <typename Serializer, typename T>
auto serialize(Serializer& sink, T x)
  -> std::enable_if_t<std::is_arithmetic<T>::value>
{
  sink.write(x);
}

template <typename Deserializer, typename T>
auto deserialize(Deserializer& source, T& x)
  -> std::enable_if_t<std::is_arithmetic<T>::value>
{
  source.read(x);
}

//
// Enums
//

template <typename Serializer, typename T>
auto serialize(Serializer& sink, T x)
  -> std::enable_if_t<std::is_enum<T>::value>
{
  sink.write(static_cast<std::underlying_type_t<T>>(x));
}

template <typename Deserializer, typename T>
auto deserialize(Deserializer& source, T& x)
  -> std::enable_if_t<std::is_enum<T>::value>
{
  std::underlying_type_t<T> u;
  source.read(u);
  x = static_cast<T>(u);
}

//
// Arrays
//

template <typename Serializer, typename T, size_t N>
auto serialize(Serializer& sink, T const (&array)[N])
  -> std::enable_if_t<sizeof(T) == 1>
{
  sink.write(&array, N);
}

template <typename Deserializer, typename T, size_t N>
auto deserialize(Deserializer& source, T (&array)[N])
  -> std::enable_if_t<sizeof(T) == 1>
{
  source.read(&array, N);
}

template <typename Serializer, typename T, size_t N>
auto serialize(Serializer& sink, T const (&array)[N])
  -> std::enable_if_t<sizeof(T) != 1>
{
  for (auto i = 0u; i < N; ++i)
    sink << array[i];
}

template <typename Deserializer, typename T, size_t N>
auto deserialize(Deserializer& source, T (&array)[N])
  -> std::enable_if_t<sizeof(T) != 1>
{
  for (auto i = 0u; i < N; ++i)
    source >> array[i];
}

//
// Pointers
//

/// @pre *x* must point to a valid instance of type `T`.
template <typename Serializer, typename T>
auto serialize(Serializer& sink, T x)
  -> std::enable_if_t<std::is_pointer<T>::value>
{
  if (x == nullptr)
    sink << false;
  else
    sink << true << *x;
}

/// @pre *x* must point to a valid instance of type `T`.
template <typename Deserializer, typename T>
auto deserialize(Deserializer& source, T& x)
  -> std::enable_if_t<std::is_pointer<T>::value>
{
  bool is_not_nullptr;
  source >> is_not_nullptr;
  if (is_not_nullptr)
    source >> *x;
}

} // namespace vast

#endif
