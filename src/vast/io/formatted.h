#ifndef VAST_IO_FORMATTED_H
#define VAST_IO_FORMATTED_H

#include <sstream>
#include <stdexcept>
#include "vast/io/stream.h"

namespace vast {
namespace io {

/// Provides formatted output over an I/O stream.
template <typename T>
output_stream& operator<<(output_stream& out, T const& x)
{
  // TODO: Use somethings more efficient than a string stream for every T.
  std::ostringstream ss;
  ss << x;
  auto str = ss.str();
  uint8_t* data;
  size_t buf_size, str_size = str.size(), offset = 0;
  while (out.next(reinterpret_cast<void**>(&data), &buf_size))
  {
    if (str_size < buf_size)
    {
      std::memcpy(data, str.data() + offset, str_size);
      out.rewind(buf_size - str_size);
      return out;
    }
    else
    {
      std::memcpy(data, str.data() + offset, buf_size);
      offset += buf_size;
      str_size -= buf_size;
    }
  }
  throw std::runtime_error("bad file output stream");
}

/// Provides formatted input over an I/O stream.
template <typename T>
input_stream& operator>>(input_stream& in, T& x)
{
  std::stringstream ss;
  char const* data;
  size_t buf_size;
  // FIXME: we need to figure out a strategy on how to consume data across
  // buffer boundaries.
  if (! in.next(reinterpret_cast<void const**>(&data), &buf_size))
    throw std::runtime_error("bad file input stream");
  ss.write(data, buf_size);
  auto old_pos = ss.tellg();
  ss >> x;
  auto new_pos = ss.tellg() - old_pos;
  if (ss.bad() || new_pos == -1)
    in.rewind(buf_size);
  else if (! ss.eof())
    in.rewind(buf_size - new_pos);
  return in;
}

} // namespace io
} // namespace vast

#endif
