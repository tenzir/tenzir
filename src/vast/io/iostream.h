#ifndef VAST_IO_IOSTREAM_H
#define VAST_IO_IOSTREAM_H

#include <iosfwd>
#include "vast/io/buffered_stream.h"

namespace vast {
namespace io {

/// An 
class istream_buffer : public input_buffer
{
public:
  istream_buffer(std::istream& is);
  virtual bool read(void* data, size_t bytes, size_t* got) override;

private:
  std::istream& in_;
};

class ostream_buffer : public output_buffer
{
public:
  ostream_buffer(std::ostream& out);
  virtual bool write(void const* data, size_t bytes, size_t* put) override;

private:
  std::ostream& out_;
};

} // namespace io
} // namespace vast

#endif
