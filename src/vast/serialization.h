#ifndef VAST_SERIALIZATION_H
#define VAST_SERIALIZATION_H

#include <cstdint>
#include <memory>
#include "vast/config.h"
#include "vast/intrusive.h"
#include "vast/traits.h"
#include "vast/object.h"
#include "vast/type_info.h"
#include "vast/io/coded_stream.h"

namespace vast {

/// Interface for serialization of objects.
class serializer
{
public:
  virtual ~serializer() = default;

  /// Begins writing an object of a given type.
  ///
  /// @param ti The type information describing the object to write afterwards.
  ///
  /// @return `true` on success.
  ///
  /// @note The default implementation checks whether the given type info maps
  /// to an announced type.
  virtual bool begin_instance(std::type_info const& ti);

  /// Finishes writing an object.
  /// @return `true` on success.
  /// @note The default implementation does nothing.
  virtual bool end_instance();

  /// Begins writing a sequence.
  /// @param size The size of the sequence.
  /// @return `true` on success.
  virtual bool begin_sequence(uint64_t size) = 0;

  /// Finishes writing a sequence.
  /// @return `true` on success.
  /// @note The default implementation does nothing.
  virtual bool end_sequence();

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

  /// Writes raw bytes.
  /// @param data The data to write.
  /// @param size The number of bytes to write.
  /// @return `true` on success.
  virtual bool write_raw(void const* data, size_t size) = 0;

  /// Writes type information.
  /// @param gti The type information to write.
  /// @return `true` on success.
  /// @note The default implementation writes out the type ID.
  /// @pre `gti != nullptr`
  virtual bool write_type(global_type_info const* gti);

  /// Writes an object.
  /// @param o The object to write.
  /// @return `true` on success.
  /// @note The default implementation writes object type information followed
  /// by an instance as described by its type information.
  virtual bool write_object(object const& o);

protected:
  serializer() = default;
};

/// Interface for deserialization of objects.
class deserializer
{
public:
  virtual ~deserializer() = default;

  /// Begins reading an object of a given type.
  ///
  /// @param ti The type information describing the object to read afterwards.
  ///
  /// @return `true` on success.
  ///
  /// @note The default implementation checks whether the given type info maps
  /// to an announced type.
  virtual bool begin_instance(std::type_info const& ti);

  /// Finishes reading an object.
  /// The default implementation does nothing.
  /// @return `true` on success.
  virtual bool end_instance();

  /// Begins reading a sequence.
  /// @param size The size of the sequence.
  /// @return `true` on success.
  virtual bool begin_sequence(uint64_t& size) = 0;

  /// Finishes reading a sequence.
  /// The default implementation does nothing.
  /// @return `true` on success.
  virtual bool end_sequence();

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

  /// Reads raw bytes.
  /// @param data The location to read into.
  /// @param size The number of bytes to read.
  /// @return `true` on success.
  virtual bool read_raw(void* data, size_t size) = 0;

  /// Reads type information.
  ///
  /// @param gti The result parameter which receives either a pointer to an
  /// announced type or `nullptr` if the type identifer does not map to an
  /// announced type.
  ///
  /// @return `true` on success.
  virtual bool read_type(global_type_info const*& gti);

  /// Reads an object.
  ///
  /// @param o The object to read into.
  ///
  /// @return `true` on success.
  ///
  /// @note The default implementation reads object type information followed
  /// by an instance as described by its type information.
  virtual bool read_object(object& o);

protected:
  deserializer() = default;
};

/// Serializes binary objects into an input stream.
class binary_serializer : public serializer
{
public:
  /// Constructs a deserializer with an output stream.
  /// @param source The output stream to write into.
  binary_serializer(io::output_stream& sink);

  virtual bool begin_sequence(uint64_t size) override;
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
  virtual bool write_raw(void const* data, size_t size) override;
  virtual size_t bytes() const;

private:
  io::coded_output_stream sink_;
  size_t bytes_ = 0;
};

/// Deserializes objects from an input stream.
class binary_deserializer : public deserializer
{
public:
  /// Constructs a deserializer with an input stream.
  /// @param source The input stream to read from.
  binary_deserializer(io::input_stream& source);

  virtual bool begin_sequence(uint64_t& size) override;
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
  virtual bool read_raw(void* data, size_t size) override;
  virtual size_t bytes() const;

private:
  io::coded_input_stream source_;
  size_t bytes_ = 0;
};


/// Provides clean access of private class internals to the serialization
/// framework.
struct access
{
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

/// Serializes an instance.
/// @tparam T the type of the instance to serialize.
/// @param sink The serializer to write a `T` into.
/// @param x An instance of type `T`.
template <typename T>
void save(serializer& sink, T const& x)
{
  access::save(sink, x, 0);
}

/// Deserializes an instance.
/// @tparam T the type of the instance to serialize.
/// @param source The deserializer to extract a `T` from.
/// @param x An instance of type `T`.
template <typename T>
void load(deserializer& source, T& x)
{
  access::load(source, x, 0);
}

/// Serializes a serializable instance.
/// @tparam T the type of the instance to serialize.
/// @param sink The serializer to write a `T` into.
/// @param x An instance of type `T`.
/// @return A reference to *sink*.
template <typename T>
serializer& operator<<(serializer& sink, T const& x)
{
  sink.begin_instance(typeid(T));
  save(sink, x);
  sink.end_instance();
  return sink;
}

/// Deserializes a deserializable instance.
/// @tparam T the type of the instance to serialize.
/// @param source The deserializer to extract a `T` from.
/// @param x An instance of type `T`.
/// @return A reference to *source*.
template <typename T>
deserializer& operator>>(deserializer& source, T& x)
{
  source.begin_instance(typeid(T));
  load(source, x);
  source.end_instance();
  return source;
}

} // namespace vast

#include "vast/serialization/arithmetic.h"
#include "vast/serialization/container.h"
#include "vast/serialization/pointer.h"
#include "vast/serialization/string.h"
#include "vast/serialization/time.h"

#endif
