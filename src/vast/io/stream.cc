#include "vast/io/stream.h"

#include <cstring>
#include <algorithm>

namespace vast {
namespace io {

bool input_streambuffer::skip(size_t bytes, size_t* skipped)
{
  char buf[4096];
  size_t got = 0;
  if (skipped)
    *skipped = 0;
  while (*skipped < bytes)
  {
    auto n = std::min(bytes - *skipped, sizeof(buf));
    if (! read(buf, n, skipped ? &got : nullptr))
      return false;
    if (skipped)
      *skipped += got;
  }
  return true;
}

std::pair<size_t, size_t> copy(input_stream& source, output_stream& sink)
{
  auto in_bytes = source.bytes();
  auto out_bytes = sink.bytes();
  void const* in_buf;
  void* out_buf;
  size_t total = 0, in_size = 0, out_size = 0;
  while (source.next(&in_buf, &in_size))
    while (sink.next(&out_buf, &out_size))
      if (in_size <= out_size)
      {
        std::memcpy(out_buf, in_buf, in_size);
        total += in_size;
        sink.rewind(out_size - in_size);
        break;
      }
      else
      {
        std::memcpy(out_buf, in_buf, out_size);
        total += out_size;
        in_buf = reinterpret_cast<uint8_t const*>(in_buf) + out_size;
        in_size -= out_size;
      }

  return {source.bytes() - in_bytes, sink.bytes() - out_bytes};
}

} // namespace io
} // namespace vast
