#include <algorithm>

#include "vast/logger.hpp"
#include "vast/io/coded_stream.hpp"

namespace vast {
namespace io {

coded_input_stream::coded_input_stream(input_stream& source) : source_(source) {
  refresh();
}

coded_input_stream::~coded_input_stream() {
  if (buffer_.size() > 0)
    source_.rewind(buffer_.size());
}

bool coded_input_stream::skip(size_t n) {
  VAST_ENTER_WITH(VAST_ARG(n));
  if (n <= buffer_.size()) {
    buffer_.advance(n);
    VAST_RETURN(true);
  }
  n -= buffer_.size();
  buffer_.reset();
  total_bytes_read_ += n;
  VAST_RETURN(source_.skip(n));
}

bool coded_input_stream::raw(void const** data, size_t* size) {
  if (!buffer_ && !refresh() && buffer_.size() == 0)
    return false;
  *data = buffer_.get();
  *size = buffer_.size();
  return true;
}

size_t coded_input_stream::read_raw(void* sink, size_t size) {
  VAST_ENTER_WITH(VAST_ARG(sink, size));
  size_t total = 0;
  size_t current;
  while ((current = buffer_.size()) < size) {
    buffer_.read(sink, current);
    sink = reinterpret_cast<uint8_t*>(sink) + current;
    size -= current;
    total += current;
    if (!refresh())
      VAST_RETURN(total);
  }
  buffer_.read(sink, size);
  VAST_RETURN(total + size);
}

bool coded_input_stream::refresh() {
  VAST_ENTER();
  void const* data;
  size_t size;
  bool success;
  do {
    success = source_.next(&data, &size);
  } while (success && size == 0); // Skip empty chunks.
  if (success) {
    buffer_.assign(data, size);
    total_bytes_read_ += size;
    VAST_RETURN(true);
  }
  buffer_.reset();
  VAST_RETURN(false);
}

coded_output_stream::coded_output_stream(output_stream& sink) : sink_(sink) {
  refresh();
}

coded_output_stream::~coded_output_stream() {
  if (buffer_.size() > 0)
    sink_.rewind(buffer_.size());
}

bool coded_output_stream::skip(size_t n) {
  VAST_ENTER_WITH(VAST_ARG(n));
  while (n > buffer_.size()) {
    n -= buffer_.size();
    if (!refresh())
      VAST_RETURN(false);
  }
  buffer_.advance(n);
  VAST_RETURN(true);
}

bool coded_output_stream::raw(void** data, size_t* size) {
  if (!buffer_ && !refresh() && buffer_.size() == 0)
    return false;
  *data = buffer_.get();
  *size = buffer_.size();
  return true;
}

size_t coded_output_stream::write_raw(void const* source, size_t size) {
  VAST_ENTER_WITH(VAST_ARG(source, size));
  size_t total = 0;
  size_t current;
  while ((current = buffer_.size()) < size) {
    buffer_.write(source, current);
    size -= current;
    total += current;
    source = reinterpret_cast<uint8_t const*>(source) + current;
    if (!refresh())
      VAST_RETURN(total);
  }
  buffer_.write(source, size);
  VAST_RETURN(total + size);
}

bool coded_output_stream::refresh() {
  VAST_ENTER();
  void* data;
  size_t size;
  if (sink_.next(&data, &size)) {
    buffer_.assign(data, size);
    total_sink_bytes_ += size;
    VAST_RETURN(true);
  }
  buffer_.reset();
  VAST_RETURN(false);
}

} // namespace io
} // namespace vast
