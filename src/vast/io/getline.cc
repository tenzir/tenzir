#include "vast/io/getline.h"

#include <cassert>
#include "vast/io/stream.h"

namespace vast {
namespace io {

bool getline(input_stream& in, std::string& line) {
  line.clear();
  char const* buf;
  size_t size;
  while (in.next(reinterpret_cast<const void**>(&buf), &size)) {
    for (size_t i = 0; i < size; ++i) {
      switch (buf[i]) {
        default:
          break;
        case '\r': {
          if (i + 1 < size && buf[i + 1] == '\n') {
            // An \r\n sequence in the middle or end of the buffer.
            line.append(buf, i);
            in.rewind(size - i - 2);
            return true;
          } else if (i == size - 1) {
            // If we encounter an \r at the end of the buffer, a \n may
            // follow at the beginning of the next one, which we simply
            // swallow. If there's no \n, we just return the line accumulated
            // so far and rewind the next buffer entirely.
            line.append(buf, i);
            if (in.next(reinterpret_cast<const void**>(&buf), &size))
              in.rewind(buf[0] == '\n' ? size - 1 : size);
            return true;
          }
        }
        // Falling through.
        case '\n': {
          if (i > 0)
            line.append(buf, i);
          in.rewind(size - i - 1);
          return true;
        }
      }
    }
    line.append(buf, size);
  }
  return false;
}

} // namespace io
} // namespace vast
