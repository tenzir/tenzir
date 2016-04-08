#include <iostream>

#include "vast/io/stream_device.hpp"

namespace vast {
namespace io {

istream_device::istream_device(std::istream& in) : in_{in} {
}

bool istream_device::read(void* data, size_t bytes, size_t* got) {
  in_.read(reinterpret_cast<char*>(data), bytes);
  auto n = in_.gcount();
  if (n == 0 && in_.fail() && !in_.eof())
    return false;
  if (got)
    *got = n;
  return true;
}

ostream_device::ostream_device(std::ostream& out) : out_{out} {
}

bool ostream_device::write(void const* data, size_t bytes, size_t* put) {
  out_.write(reinterpret_cast<char const*>(data), bytes);
  if (put)
    *put = bytes;
  return out_.good();
}

} // namespace io
} // namespace vast
