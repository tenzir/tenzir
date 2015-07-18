#ifndef VAST_IO_ITERATOR_H
#define VAST_IO_ITERATOR_H

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

} // namespace io
} // namespace vast

#endif
