#ifndef VAST_IO_SERIALIZATION_H
#define VAST_IO_SERIALIZATION_H

#include <memory>
#include "vast/config.h"
#include "vast/intrusive.h"
#include "vast/traits.h"
#include "vast/io/coded_stream.h"

namespace vast {
namespace io {

/// Interface for serialization of objects.
class serializer
{
public:
#ifdef VAST_CLANG
  virtual ~serializer() = default;
#else
  virtual ~serializer() { };
#endif

  /// Writes a value.
  /// @param x The value to write.
  /// @return `true` on success.
  virtual bool write_bool(bool x) = 0;
  virtual bool write_int8(int8_t x) = 0;
  virtual bool write_uint8(uint8_t x) = 0;
  virtual bool write_int16(int16_t x) = 0;
  virtual bool write_uint16(uint16_t x) = 0;
  virtual bool write_int32(int32_t x) = 0;
  virtual bool write_uint32(uint32_t x) = 0;
  virtual bool write_int64(int64_t x) = 0;
  virtual bool write_uint64(uint64_t x) = 0;
  virtual bool write_double(double x) = 0;

  /// Begins writing a sequence.
  /// @param size The size of the sequence.
  /// @return `true` on success.
  virtual bool write_sequence_begin(uint64_t size) = 0;

  /// Finishes writing a sequence.
  /// @return `true` on success.
  virtual bool write_sequence_end() = 0;

  /// Writes raw bytes.
  /// @param data The data to write.
  /// @param size The number of bytes to write.
  /// @return `true` on success.
  virtual bool write_raw(void const* data, size_t size) = 0;

protected:
  serializer() = default;
};

/// Interface for deserialization of objects.
class deserializer
{
public:
#ifdef VAST_CLANG
  virtual ~deserializer() = default;
#else
  virtual ~deserializer() { };
#endif

  /// Reads a value.
  /// @param x The value to read into.
  /// @return `true` on success.
  virtual bool read_bool(bool& x) = 0;
  virtual bool read_int8(int8_t& x) = 0;
  virtual bool read_uint8(uint8_t& x) = 0;
  virtual bool read_int16(int16_t& x) = 0;
  virtual bool read_uint16(uint16_t& x) = 0;
  virtual bool read_int32(int32_t& x) = 0;
  virtual bool read_uint32(uint32_t& x) = 0;
  virtual bool read_int64(int64_t& x) = 0;
  virtual bool read_uint64(uint64_t& x) = 0;
  virtual bool read_double(double& x) = 0;

  /// Begins reading a sequence.
  /// @param size The size of the sequence.
  /// @return `true` on success.
  virtual bool read_sequence_begin(uint64_t& size) = 0;

  /// Finishes reading a sequence.
  /// @return `true` on success.
  virtual bool read_sequence_end() = 0;

  /// Reads raw bytes.
  /// @param data The location to read into.
  /// @param size The number of bytes to read.
  /// @return `true` on success.
  virtual bool read_raw(void* data, size_t size) = 0;

protected:
  deserializer() = default;
};

/// Serializes binary objects into an input stream.
class binary_serializer : public serializer
{
public:
  /// Constructs a deserializer with an output stream.
  /// @param source The output stream to write into.
  binary_serializer(output_stream& sink);

  virtual bool write_bool(bool x) override;
  virtual bool write_int8(int8_t x) override;
  virtual bool write_uint8(uint8_t x) override;
  virtual bool write_int16(int16_t x) override;
  virtual bool write_uint16(uint16_t x) override;
  virtual bool write_int32(int32_t x) override;
  virtual bool write_uint32(uint32_t x) override;
  virtual bool write_int64(int64_t x) override;
  virtual bool write_uint64(uint64_t x) override;
  virtual bool write_double(double x) override;
  virtual bool write_sequence_begin(uint64_t size) override;
  virtual bool write_sequence_end() override;
  virtual bool write_raw(void const* data, size_t size) override;
  virtual size_t bytes() const;

private:
  coded_output_stream sink_;
  size_t bytes_ = 0;
};

/// Deserializes objects from an input stream.
class binary_deserializer : public deserializer
{
public:
  /// Constructs a deserializer with an input stream.
  /// @param source The input stream to read from.
  binary_deserializer(input_stream& source);

  virtual bool read_bool(bool& x) override;
  virtual bool read_int8(int8_t& x) override;
  virtual bool read_uint8(uint8_t& x) override;
  virtual bool read_int16(int16_t& x) override;
  virtual bool read_uint16(uint16_t& x) override;
  virtual bool read_int32(int32_t& x) override;
  virtual bool read_uint32(uint32_t& x) override;
  virtual bool read_int64(int64_t& x) override;
  virtual bool read_uint64(uint64_t& x) override;
  virtual bool read_double(double& x) override;
  virtual bool read_sequence_begin(uint64_t& size) override;
  virtual bool read_sequence_end() override;
  virtual bool read_raw(void* data, size_t size) override;
  virtual size_t bytes() const;

private:
  coded_input_stream source_;
  size_t bytes_ = 0;
};


/// Provides clean access of private class internals to the serialization
/// framework.
class access
{
public:
  template <typename T>
  static inline auto save(serializer& sink, T x, int)
    -> decltype(x.serialize(sink), void())
  {
    x.serialize(sink);
  }

  template <typename T>
  static inline auto save(serializer& sink, T x, long)
    -> decltype(serialize(sink, x), void())
  {
    serialize(sink, x);
  }

  template <typename T>
  static inline auto load(deserializer& source, T& x, int)
    -> decltype(x.deserialize(source), void())
  {
    x.deserialize(source);
  }

  template <typename T>
  static inline auto load(deserializer& source, T& x, long)
    -> decltype(deserialize(source, x), void())
  {
    deserialize(source, x);
  }
};

/// Serializes an object.
/// @param sink The serializer to use as sink.
/// @param The object to serialize into *sink*.
/// @return A reference to *sink*.
template <typename T>
serializer& operator<<(serializer& sink, T const& x)
{
  access::save(sink, x, 0);
  return sink;
}

/// Deserializes an object.
/// @param source The deserializer to use as source.
/// @param The object to deserialize from *source*.
/// @return A reference to *source*.
template <typename T>
deserializer& operator>>(deserializer& source, T& x)
{
  access::load(source, x, 0);
  return source;
}

} // namespace io
} // namespace vast

#include "vast/io/serialization/arithmetic.h"
#include "vast/io/serialization/container.h"
#include "vast/io/serialization/pointer.h"
#include "vast/io/serialization/string.h"
#include "vast/io/serialization/time.h"

#endif
