#ifndef VAST_IO_ALGORITHM_HPP
#define VAST_IO_ALGORITHM_HPP

#include <algorithm>

#include "vast/io/stream.hpp"

namespace vast {
namespace io {

/// Copies data from an input stream into an output stream.
/// @param source The input stream.
/// @param sink The output stream.
/// @returns The number of bytes copied for source and sink.
std::pair<size_t, size_t> copy(input_stream& source, output_stream& sink);

/// Writes into an output file stream.
/// @tparam An input iterator.
/// @param begin The beginning of the data.
/// @param end The end of the data.
template <typename Iterator>
bool copy(Iterator begin, Iterator end, output_stream& sink) {
  auto buf = sink.next_block();
  if (!buf)
    return false;
  while (begin != end) {
    size_t input_size = end - begin;
    if (input_size <= buf.size()) {
      std::copy(begin, end, buf.data());
      sink.rewind(buf.size() - input_size);
      break;
    } else if (buf.size() == 0) {
      return false;
    } else {
      std::copy(begin, begin + buf.size(), buf.data());
      begin += buf.size();
      buf = sink.next_block();
      if (!buf)
        break;
    }
  }
  return true;
}

} // namespace io
} // namespace vast

#endif
