#ifndef VAST_BLOCK_H
#define VAST_BLOCK_H

#include "vast/serialization.h"
#include "vast/io/array_stream.h"
#include "vast/io/container_stream.h"
#include "vast/io/compressed_stream.h"
#include "vast/util/operators.h"

namespace vast {

/// A compressed buffer of serialized objects.
class block : util::equality_comparable<block>
{
public:
  /// A helper class to to write into the block.
  class writer
  {
  public:
    /// Constructs a writer from a block.
    /// @param blk The block to serialize into.
    writer(block& blk);

    /// Destructs a writer.
    ~writer();

    writer(writer&&) = default;
    writer& operator=(writer&&) = default;

    /// Serializes an object into the underlying block.
    /// @param x The object to serialize.
    /// @param count The number of elements *x* should count for.
    template <typename T>
    bool write(T const& x, size_t count = 1)
    {
      serializer_ << x;
      block_.elements_ += count;
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
    block& block_;
    io::container_output_stream<std::vector<uint8_t>> base_stream_;
    std::unique_ptr<io::compressed_output_stream> compressed_stream_;
    binary_serializer serializer_;
  };

  /// A proxy class to deserialize from the block.
  class reader
  {
  public:
    /// Constructs a reader from a block.
    /// @param blk The block to extract objects from.
    reader(block const& blk);

    /// Deserializes an object from the block.
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
    /// @returns The number of times one can call ::read on this block.
    uint64_t available() const;

    /// Retrieves the number of bytes deserialized so far.
    /// @returns The number of deserialized bytes.
    size_t bytes() const;

  private:
    block const& block_;
    uint64_t available_ = 0;
    io::array_input_stream base_stream_;
    std::unique_ptr<io::compressed_input_stream> compressed_stream_;
    binary_deserializer deserializer_;
  };

  /// Constructs a block.
  /// @param method The compression method to use.
  explicit block(io::compression method = io::lz4);

  /// Checks whether the block is empty.
  /// @returns `true` if the block has no elements.
  bool empty() const;

  /// Retrieves the number of serialized elements in the block.
  /// @returns The number of elements in the block.
  uint64_t elements() const;

  /// Retrieves the size in bytes of the compressed/serialized buffer.
  /// @returns The number of bytes of the serialized/compressed buffer.
  size_t compressed_bytes() const;

  /// Retrieves the size in bytes of the compressed/serialized buffer.
  /// @returns The number of bytes of the serialized/compressed buffer.
  size_t uncompressed_bytes() const;

  friend bool operator==(block const& x, block const& y);

private:
  friend access;
  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  io::compression compression_;
  uint64_t elements_ = 0;
  uint64_t uncompressed_bytes_ = 0;
  std::vector<uint8_t> buffer_;
};

} // namespace vast

#endif
