#ifndef VAST_CHUNK_H
#define VAST_CHUNK_H

#include "vast/serialization.h"
#include "vast/io/array_stream.h"
#include "vast/io/container_stream.h"
#include "vast/io/compressed_stream.h"
#include "vast/util/operators.h"

namespace vast {

/// A compressed buffer of serialized objects.
class chunk : util::equality_comparable<chunk>
{
public:
  /// A proxy class to serialize into the chunk.
  class writer
  {
  public:
    /// Constructs a writer from a chunk.
    /// @param chk The chunk to serialize into.
    writer(chunk& chk);

    /// Destructs a chunks.
    ~writer();

    /// Move-constructs a writer.
    /// @param other The writer to move.
    writer(writer&& other) = default;

    /// Move-assigns a writer.
    /// @param other The writer to assign to this instance.
    writer& operator=(writer&& other) = default;

    /// Serializes an object into the underlying chunk.
    /// @param x The object to serialize.
    /// @param count The number of elements *x* should count for.
    template <typename T>
    bool write(T const& x, size_t count = 1)
    {
      serializer_ << x;
      chunk_.elements_ += count;
      return true;
    }

    /// Retrieves the number of bytes serialized so far.
    ///
    /// @returns The number of serialized bytes.
    ///
    /// @note In order to compute the space reduction, use the following
    /// formula *after* the writer has been destructed:
    ///
    ///     `long((1.0 - double(chunk_bytes) / bytes()) * 100)`
    ///
    size_t bytes() const;

  private:
    chunk& chunk_;
    io::container_output_stream<std::vector<uint8_t>> base_stream_;
    std::unique_ptr<io::compressed_output_stream> compressed_stream_;
    binary_serializer serializer_;
  };

  /// A proxy class to deserialize from the chunk.
  class reader
  {
  public:
    /// Constructs a reader from a chunk.
    /// @param chk The chunk to extract objects from.
    reader(chunk const& chk);

    /// Deserializes an object from the chunk.
    /// @param x The object to deserialize into.
    /// @param count The number of elements *x* should count for.
    template <typename T>
    bool read(T& x, size_t count = 1)
    {
      if (available_ == 0)
        return false;

      deserializer_ >> x;
      available_ -= count > available_ ? available_ : count;
      return true;
    }

    /// Retrieves the number of objects available for deserialization.
    /// @returns The number of times one can call ::read on this chunk.
    uint32_t available() const;

    /// Retrieves the number of bytes deserialized so far.
    /// @returns The number of deserialized bytes.
    size_t bytes() const;

  private:
    chunk const& chunk_;
    uint32_t available_ = 0;
    io::array_input_stream base_stream_;
    std::unique_ptr<io::compressed_input_stream> compressed_stream_;
    binary_deserializer deserializer_;
  };

  /// Constructs a chunk.
  /// @param method The compression method to use.
  explicit chunk(io::compression method = io::lz4);

  /// Checks whether the chunk is empty.
  /// @returns `true` if the chunk has no elements.
  bool empty() const;

  /// Retrieves the number of serialized elements in the chunk.
  /// @returns The number of elements in the chunk.
  uint32_t elements() const;

  /// Retrieves the size in bytes of the compressed/serialized buffer.
  /// @returns The number of bytes of the serialized/compressed buffer.
  size_t compressed_bytes() const;

  /// Retrieves the size in bytes of the compressed/serialized buffer.
  /// @returns The number of bytes of the serialized/compressed buffer.
  size_t uncompressed_bytes() const;

  friend bool operator==(chunk const& x, chunk const& y);

private:
  friend access;
  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  io::compression compression_;
  uint32_t elements_ = 0;
  uint32_t bytes_ = 0;
  std::vector<uint8_t> buffer_;
};

} // namespace vast

#endif
