#ifndef VAST_IO_STREAM_DEVICE_H
#define VAST_IO_STREAM_DEVICE_H

#include <iosfwd>

#include "vast/io/device.h"

namespace vast {
namespace io {

/// An [input device](io::input_device) wrapping a `std::istream`.
class istream_device : public input_device
{
public:
  istream_device(std::istream& is);
  virtual bool read(void* data, size_t bytes, size_t* got) override;

private:
  std::istream& in_;
};

/// An [output device](io::output_buffer) wrapping a `std::ostream`.
class ostream_device : public output_device
{
public:
  ostream_device(std::ostream& out);
  virtual bool write(void const* data, size_t bytes, size_t* put) override;

private:
  std::ostream& out_;
};

} // namespace io
} // namespace vast

#endif
