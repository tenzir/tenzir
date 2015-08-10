#include "vast/io/stream.h"

namespace vast {
namespace io {

buffer<void const> input_stream::next_block() {
  void const* in;
  size_t size;
  if (next(&in, &size))
    return make_buffer(in, size);

  return {};
}

buffer<void> output_stream::next_block() {
  void* out;
  size_t size;
  if (next(&out, &size))
    return make_buffer(out, size);

  return {};
}

bool output_stream::flush() {
  return true;
}

} // namespace io
} // namespace vast
