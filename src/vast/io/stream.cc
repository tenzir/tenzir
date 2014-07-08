#include "vast/io/stream.h"

#include <cstring>
#include <algorithm>

namespace vast {
namespace io {

buffer<void const> input_stream::next_block()
{
  void const* in;
  size_t size;
  if (next(&in, &size))
    return make_buffer(in, size);

  return {};
}

buffer<void> output_stream::next_block()
{
  void* out;
  size_t size;
  if (next(&out, &size))
    return make_buffer(out, size);

  return {};
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
