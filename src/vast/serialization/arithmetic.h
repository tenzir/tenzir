#ifndef VAST_SERIALIZATION_ARITHMETIC_H
#define VAST_SERIALIZATION_ARITHMETIC_H

namespace vast {

inline bool serialize(serializer& sink, bool x)
{
  return sink.write_bool(x);
}

inline bool serialize(serializer& sink, int8_t x)
{
  return sink.write_int8(x);
}

inline bool serialize(serializer& sink, uint8_t x)
{
  return sink.write_uint8(x);
}

inline bool serialize(serializer& sink, int16_t x)
{
  return sink.write_int16(x);
}

inline bool serialize(serializer& sink, uint16_t x)
{
  return sink.write_uint16(x);
}

inline bool serialize(serializer& sink, int32_t x)
{
  return sink.write_int32(x);
}

inline bool serialize(serializer& sink, uint32_t x)
{
  return sink.write_uint32(x);
}

inline bool serialize(serializer& sink, int64_t x)
{
  return sink.write_int64(x);
}

inline bool serialize(serializer& sink, uint64_t x)
{
  return sink.write_uint64(x);
}

template <typename T>
std::enable_if_t<
  std::is_same<T, long>::value || std::is_same<T, long long>::value,
  bool
>
serialize(serializer& sink, T x)
{
  return sink.write_int64(x);
}

template <typename T>
std::enable_if_t<
  std::is_same<T, unsigned long>::value ||
    std::is_same<T, unsigned long long>::value,
  bool
>
serialize(serializer& sink, T x)
{
  return sink.write_uint64(x);
}

inline bool serialize(serializer& sink, double x)
{
  return sink.write_double(x);
}


inline bool deserialize(deserializer& source, bool& x)
{
  return source.read_bool(x);
}

inline bool deserialize(deserializer& source, int8_t& x)
{
  return source.read_int8(x);
}

inline bool deserialize(deserializer& source, uint8_t& x)
{
  return source.read_uint8(x);
}

inline bool deserialize(deserializer& source, int16_t& x)
{
  return source.read_int16(x);
}

inline bool deserialize(deserializer& source, uint16_t& x)
{
  return source.read_uint16(x);
}

inline bool deserialize(deserializer& source, int32_t& x)
{
  return source.read_int32(x);
}

inline bool deserialize(deserializer& source, uint32_t& x)
{
  return source.read_uint32(x);
}

inline bool deserialize(deserializer& source, int64_t& x)
{
  return source.read_int64(x);
}

inline bool deserialize(deserializer& source, uint64_t& x)
{
  return source.read_uint64(x);
}

template <typename T>
std::enable_if_t<
  std::is_same<T, long>::value || std::is_same<T, long long>::value,
  bool
>
deserialize(deserializer& source, T& x)
{
  int64_t l;
  auto success = source.read_int64(l);
  assert(l <= std::numeric_limits<T>::max());
  x = static_cast<T>(l);
  return success;
}

template <typename T>
std::enable_if_t<
  std::is_same<T, unsigned long>::value ||
    std::is_same<T, unsigned long long>::value,
  bool
>
deserialize(deserializer& source, T& x)
{
  uint64_t ul;
  auto success = source.read_uint64(ul);
  assert(ul <= std::numeric_limits<T>::max());
  x = static_cast<T>(ul);
  return success;
}

inline bool deserialize(deserializer& source, double& x)
{
  return source.read_double(x);
}

} // namespace vast

#endif
