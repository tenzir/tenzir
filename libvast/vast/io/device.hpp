#ifndef VAST_IO_DEVICE_HPP
#define VAST_IO_DEVICE_HPP

#include <cstddef>

namespace vast {
namespace io {

/// An interface for reading from an underlying device.
class input_device {
public:
  /// Attempts to read data into a given buffer.
  /// @param data The buffer receiving the result of the read operation.
  /// @param bytes The number of bytes to read.
  /// @param got The number of bytes actually read into *data*. A value of 0
  ///            means that EOF has been encountered.
  /// @returns `true` if *bytes* bytes could be copied into *data*  and `false`
  /// if an error occurred.
  virtual bool read(void* data, size_t bytes, size_t* got = nullptr) = 0;

  /// Skips a given number of bytes. The default implementation subsequently
  /// calls read() until the desired number of bytes have been read or an error
  /// occurred.
  /// @param bytes The number of bytes to skip.
  /// @param skipped The number of bytes actually skipped.
  /// @returns `true` if *bytes* bytes were successfully skipped and false
  ///          otherwise.
  virtual bool skip(size_t bytes, size_t* skipped = nullptr);
};

/// An interface for writing to an underlying device.
class output_device {
public:
  /// Attempts to write data to a given buffer.
  /// @param data The data to write.
  /// @param bytes The number of bytes to write.
  /// @param put The number of bytes actually read from *data*.
  /// @returns `true` if *bytes* bytes could be copied from *data* and false if
  ///          an error occurred.
  virtual bool write(void const* data, size_t bytes, size_t* put = nullptr) = 0;
};

} // namespace io
} // namespace vast

#endif
