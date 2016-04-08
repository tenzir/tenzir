#include <cstring>

#include "vast/io/algorithm.hpp"
#include "vast/util/assert.hpp"

namespace vast {
namespace io {

std::pair<size_t, size_t> copy(input_stream& source, output_stream& sink) {
  auto in_bytes = source.bytes();
  auto out_bytes = sink.bytes();
  void const* in_buf;
  void* out_buf;
  size_t total = 0, in_size = 0, out_size = 0;
  while (source.next(&in_buf, &in_size))
    while (sink.next(&out_buf, &out_size))
      if (in_size <= out_size) {
        std::memcpy(out_buf, in_buf, in_size);
        total += in_size;
        sink.rewind(out_size - in_size);
        break;
      } else {
        std::memcpy(out_buf, in_buf, out_size);
        total += out_size;
        in_buf = reinterpret_cast<uint8_t const*>(in_buf) + out_size;
        in_size -= out_size;
      }
  return {source.bytes() - in_bytes, sink.bytes() - out_bytes};
}

} // namespace io
} // namespace vast
