#ifndef VAST_IO_ALGORITHM_H
#define VAST_IO_ALGORITHM_H

#include <algorithm>

#include "vast/io/buffer.h"
#include "vast/io/stream.h"
#include "vast/util/iterator.h"

namespace vast {
namespace io {

/// An input iterator which wraps an input stream.
class input_iterator : public util::iterator_facade<
                           input_iterator,
                           char,
                           std::input_iterator_tag,
                           char
                         >
{
public:
  input_iterator() = default;

  /// Constructs an input_iterator from an ::input_stream.
  input_iterator(input_stream& in);

private:
  friend util::iterator_access;

  void increment();
  char dereference() const;
  bool equals(input_iterator const& other) const;

  size_t i_ = 0;
  buffer<char const> buf_;
  input_stream* in_;
};

/// An output iterator which wraps an output stream.
class output_iterator : public util::iterator_facade<
                           output_iterator,
                           char,
                           std::output_iterator_tag
                         >
{
public:
  /// Constructs an output_iterator from an ::output_stream.
  output_iterator(output_stream& out);

  /// Rewinds the last block of the underlying ::output_stream. After working
  /// with the iterator, this function must be called to "flush" the output
  /// iterator, i.e., rewind the current block of the underlying output stream.
  ///
  /// @returns The number of bytes rewound.
  size_t rewind();

private:
  friend util::iterator_access;

  void increment();
  char& dereference() const;

  size_t i_ = 0;
  buffer<char> buf_;
  output_stream* out_;
};

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
bool copy(Iterator begin, Iterator end, output_stream& sink)
{
  auto buf = sink.next_block();
  if (! buf)
    return false;
  while (begin != end)
  {
    size_t input_size = end - begin;
    if (input_size <= buf.size())
    {
      std::copy(begin, end, buf.data());
      sink.rewind(buf.size() - input_size);
      break;
    }
    else if (buf.size() == 0)
    {
      return false;
    }
    else
    {
      std::copy(begin, begin + buf.size(), buf.data());
      begin += buf.size();
      buf = sink.next_block();
      if (! buf)
        break;
    }
  }
  return true;
}

} // namespace io
} // namespace vast

#endif
