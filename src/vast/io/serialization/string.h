#ifndef VAST_IO_SERIALIZATION_STRING_H
#define VAST_IO_SERIALIZATION_STRING_H

#include <string>

namespace vast {
namespace io {

inline void serialize(serializer& sink, std::string const& str)
{
  sink.write_sequence_begin(str.size());
  if (! str.empty())
    sink.write_raw(str.data(), str.size());
  sink.write_sequence_end();
}

inline void deserialize(deserializer& source, std::string& str)
{
  uint64_t size;
  source.read_sequence_begin(size);
  if (size > 0)
  {
    if (size > std::numeric_limits<std::string::size_type>::max())
      throw error::io("size too large for architecture");
    str.resize(size);
    source.read_raw(const_cast<std::string::value_type*>(str.data()), size);
  }
  source.read_sequence_end();
}

} // namespace io
} // namespace vast

#endif
