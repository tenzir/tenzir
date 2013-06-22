#ifndef VAST_SERIALIZATION_STRING_H
#define VAST_SERIALIZATION_STRING_H

#include <string>

namespace vast {

inline void serialize(serializer& sink, std::string const& str)
{
  sink.begin_sequence(str.size());
  if (! str.empty())
    sink.write_raw(str.data(), str.size());
  sink.end_sequence();
}

inline void deserialize(deserializer& source, std::string& str)
{
  uint64_t size;
  source.begin_sequence(size);
  if (size > 0)
  {
    if (size > std::numeric_limits<std::string::size_type>::max())
      throw std::length_error("size too large for architecture");
    str.resize(size);
    source.read_raw(const_cast<std::string::value_type*>(str.data()), size);
  }
  source.end_sequence();
}

} // namespace vast

#endif
