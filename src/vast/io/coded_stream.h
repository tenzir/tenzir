#ifndef VAST_IO_CODED_STREAM_H
#define VAST_IO_CODED_STREAM_H

#include <type_traits>
#include "vast/logger.h"
#include "vast/io/stream.h"
#include "vast/util/byte_swap.h"
#include "vast/util/buffer.h"
#include "vast/util/coding.h"

namespace vast {
namespace io {

/// Wraps an input stream and offers a variety of decoding schemes to read from
/// the underlying data.
class coded_input_stream
{
  coded_input_stream(coded_input_stream const&) = delete;
  coded_input_stream& operator=(coded_input_stream) = delete;

public:
  /// Constructs a coded input stream from an underlying input stream.
  /// @param source The input stream to read from.
  coded_input_stream(input_stream& source);

  /// Destroys the input stream and rewinds the wrapped stream.
  ~coded_input_stream();

  /// Skips a given number of bytes of the wrapped stream.
  /// @param n The number of bytes to skip.
  /// @return `true` on success.
  bool skip(size_t n);

  /// Retrieves the raw buffer of the wrapped stream. In combination with skip,
  /// this function can be used to operate directly on the underlyig buffer.
  ///
  /// @param data Set to the raw buffer of the wrapped stream.
  ///
  /// @param size Set to the size of *data*.
  ///
  /// @return `true` if obtaining the raw buffer succeeded, i.e., the
  /// underlying buffer was not empty.
  bool raw(void const** data, size_t* size);

  /// Reads an arithmetic type from the input.
  /// @tparam T an arithmetic type.
  /// @param x The value to read into.
  /// @return `true` if reading *x* succeeded.
  /// @pre *x* must point to valid instance of type *T*.
  template <typename T>
  typename std::enable_if<std::is_arithmetic<T>::value, bool>::type
  read(void* x)
  {
    VAST_ENTER();
    auto val = reinterpret_cast<T*>(x);
    if (buffer_.size() >= sizeof(T))
    {
      auto buf = buffer_.cast<T const>();
      *val = util::byte_swap<network_endian, host_endian>(*buf);
      buffer_.advance(sizeof(T));
      VAST_RETURN(true, "got " << *val);
    }
    auto n = read_raw(val, sizeof(T));
    *val = util::byte_swap<network_endian, host_endian>(*val);
    VAST_RETURN((n == sizeof(T)), "got " << *val);
  }

  /// Reads a variable-byte encoded integral type from the input.
  /// @tparam T an integral type.
  /// @param x The value to read.
  /// @return `true` if reading *x* succeeded.
  template <typename T>
  typename std::enable_if<std::is_integral<T>::value, bool>::type
  read_varbyte(T* x)
  {
    VAST_ENTER();
    size_t n = 0;
    if (buffer_.size() >= util::varbyte::max_size<T>())
    {
      n = util::varbyte::decode(buffer_.get(), x);
      buffer_.advance(n);
      VAST_RETURN(true, "got " << *x);
    }

    *x = n = 0;
    uint8_t low7;
    do
    {
      if (n == util::varbyte::max_size<T>())
        VAST_RETURN(false, "reached varbyte max size");
      while (buffer_.size() == 0)
        if (! refresh())
          VAST_RETURN(false, "refresh failed");
      low7 = *buffer_.get();
      *x |= (low7 & 0x7F) << (7 * n);
      buffer_.advance(1);
      ++n;
    }
    while (low7 & 0x80);
    VAST_RETURN(true, "got " << *x);
  }

  /// Reads raw bytes.
  /// @param sink The buffer to copy into.
  /// @param size The number of bytes to copy into *sink*.
  /// @return The number of bytes read.
  size_t read_raw(void* sink, size_t size);

private:
  bool refresh();

  util::buffer<uint8_t const> buffer_;
  size_t total_bytes_read_ = 0;
  input_stream& source_;
};

/// Wraps an output stream and offers a variety of encoding schemes to write to
/// the underlying data.
class coded_output_stream
{
  coded_output_stream(coded_output_stream const&) = delete;
  coded_output_stream& operator=(coded_output_stream) = delete;

public:
  /// Constructs a coded input stream from an underlying input stream.
  /// @param source The input stream to read from.
  coded_output_stream(output_stream& sink);

  /// Destroys the coded output stream and rewinds the wrapped sink.
  ~coded_output_stream();

  /// Skips a given number of bytes in the wrapped stream.
  /// @param n The number of bytes to skip.
  /// @return `true` on success.
  bool skip(size_t n);

  /// Retrieves the raw buffer of the unwritten data portions. In combination
  /// with skip, this function can be used to operate directly on the underlyig
  /// buffer.
  ///
  /// @param data Set to the raw buffer of unwritten data.
  ///
  /// @param size Set to the size of *data*.
  ///
  /// @return `true` if obtaining the raw buffer succeeded, i.e., the
  /// underlying buffer was not empty.
  bool raw(void** data, size_t* size);

  /// Writes an arithmetic type to the input.
  /// @tparam T an arithmetic type.
  /// @param x The value to write from.
  /// @return The number of bytes written.
  /// @pre *x* must point to an instance of type *T*.
  template <typename T>
  typename std::enable_if<std::is_arithmetic<T>::value, size_t>::type
  write(void const* x)
  {
    VAST_ENTER();
    auto ptr = reinterpret_cast<T const*>(x);
    T val = util::byte_swap<host_endian, network_endian>(*ptr);
    if (buffer_.size() < sizeof(T))
    {
      auto n = write_raw(&val, sizeof(T));
      VAST_RETURN(n);
    }

    *buffer_.cast<T>() = val;
    buffer_.advance(sizeof(T));
    VAST_RETURN(sizeof(T));
  }

  /// Writes a variable-byte encoded integral type to the output.
  /// @tparam T an integral type.
  /// @param x The value to write.
  /// @return The number of bytes written.
  template <typename T>
  typename std::enable_if<std::is_integral<T>::value, size_t>::type
  write_varbyte(T const* x)
  {
    VAST_ENTER();
    size_t n;
    if (buffer_.size() >= util::varbyte::max_size<T>() ||
        buffer_.size() >= util::varbyte::size(*x))
    {
      n = util::varbyte::encode(*x, buffer_.get());
      assert(n == util::varbyte::size(*x));
      buffer_.advance(n);
      VAST_RETURN(n);
    }

    // If we might write across buffers, we first write the result into a
    // temporary space and then write it out in raw form.
    uint8_t buf[util::varbyte::max_size<T>()];
    n = util::varbyte::encode(*x, buf);
    VAST_RETURN(write_raw(buf, n));
  }

  /// Writes raw bytes.
  /// @param source The buffer to copy from.
  /// @param size The number of bytes to copy from *source*.
  /// @return The number of bytes written.
  size_t write_raw(void const* source, size_t size);

private:
  bool refresh();

  util::buffer<uint8_t> buffer_;
  size_t total_sink_bytes_ = 0;
  output_stream& sink_;
};

} // namespace io
} // namespace vast

#endif
