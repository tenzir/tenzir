#include "vast/io/iostream.h"

#include <iostream>

namespace vast {
namespace io {

istream_buffer::istream_buffer(std::istream& in)
  : in_{in}
{
}

bool istream_buffer::read(void* data, size_t bytes, size_t* got)
{
  in_.read(reinterpret_cast<char*>(data), bytes);
  auto n = in_.gcount();
  if (n == 0 && in_.fail() && ! in_.eof())
    return false;

  if (got)
    *got = n;

  return true;;
}

ostream_buffer::ostream_buffer(std::ostream& out)
  : out_{out}
{
}

bool ostream_buffer::write(void const* data, size_t bytes, size_t* put)
{
  out_.write(reinterpret_cast<char const*>(data), bytes);

  if (put)
    *put = bytes;

  return out_.good();
}

} // namespace io
} // namespace vast
