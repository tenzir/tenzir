#ifndef VAST_CHUNK_H
#define VAST_CHUNK_H

#include "vast/logger.h"
#include "vast/serialization.h"
#include "vast/io/array_stream.h"
#include "vast/io/container_stream.h"
#include "vast/io/compressed_stream.h"
#include "vast/util/make_unique.h"
#include "vast/util/operators.h"

namespace vast {

/// A compressed buffer of serialized objects.
template <typename T>
class chunk : util::equality_comparable<chunk<T>>
{
public:
  typedef std::vector<uint8_t> buffer_type;

  /// A proxy class to serialize into the chunk.
  class putter
  {
    putter(putter const&) = delete;
    putter& operator=(putter const&) = delete;
    typedef io::container_output_stream<buffer_type> output_stream_type;

  public:
    /// Constructs a putter from a chunk.
    /// @param chk The chunk to serialize into.
    putter(chunk* chk = nullptr)
    {
      reset(chk);
    }

    ~putter()
    {
      VAST_ENTER();
      if (sink_)
        reset();
    }

    /// Tells the putter to use a given chunk.
    /// @param chk The chunk to use.
    void reset(chunk* chk = nullptr)
    {
      VAST_ENTER();
      auto processed = bytes();

      sink_.reset();
      compressed_stream_.reset();
      base_stream_.reset();

      if (processed > 0)
      {
        assert(chunk_);
        VAST_LOG_VERBOSE(
            "putter wrote " << processed << " bytes into chunk of size " <<
            chunk_->bytes() << " bytes (" <<
            long((1.0 - double(chunk_->bytes()) / processed) * 100) <<
            "% space reduction)");
      }

      if (chk == nullptr)
        return;

      chunk_ = chk;
      base_stream_ = make_unique<output_stream_type>(chunk_->buffer_);
      compressed_stream_.reset(io::make_compressed_output_stream(
          chunk_->compression_, *base_stream_));
      sink_ = make_unique<binary_serializer>(*compressed_stream_);
    }

    /// Move-constructs a putter.
    /// @param other The putter to move.
    putter(putter&& other) = default;

    /// Move-assigns a putter.
    /// @param other The putter to assign to this instance.
    putter& operator=(putter&& other) = default;

    /// Serializes an object into the underlying chunk.
    /// @param x The object to serialize.
    void operator<<(T const& x)
    {
      VAST_ENTER();
      assert(chunk_);
      *sink_ << x;
      ++chunk_->elements_;
    }

    /// Retrieves the number of bytes serialized so far.
    /// @return The number of serialized bytes.
    size_t bytes() const
    {
      return sink_ ? sink_->bytes() : 0;
    }

  private:
    chunk* chunk_;
    std::unique_ptr<output_stream_type> base_stream_;
    std::unique_ptr<io::compressed_output_stream> compressed_stream_;
    std::unique_ptr<binary_serializer> sink_;
  };

  /// A proxy class to deserialize from the chunk.
  class getter
  {
    getter(getter const&) = delete;
    getter& operator=(getter const&) = delete;

  public:
    /// Constructs a getter from a chunk.
    /// @param chk The chunk to extract objects from.
    getter(chunk const* chk = nullptr)
    {
      reset(chk);
    }

    /// Move-constructs a getter.
    /// @param other The getter to move.
    getter(getter&& other) = default;

    /// Move-assigns a getter.
    /// @param other The getter to assign to this instance.
    getter& operator=(getter&& other) = default;

    /// Tells the putter to use a given chunk.
    /// @param chk The chunk to use.
    void reset(chunk const* chk = nullptr)
    {
      source_.reset();
      compressed_stream_.reset();
      base_stream_.reset();
      if (chk == nullptr)
        return;

      chunk_ = chk;
      available_ = chunk_->elements_;
      base_stream_ = make_unique<io::array_input_stream>(
          chunk_->buffer_.data(),
          chunk_->buffer_.size());
      compressed_stream_.reset(io::make_compressed_input_stream(
          chunk_->compression_,
          *base_stream_));
      source_ = make_unique<binary_deserializer>(*compressed_stream_);
    }

    /// Deserializes an object from the chunk.
    /// @param x The object to deserialize into.
    void operator>>(T& x)
    {
      VAST_ENTER();
      assert(chunk_);
      if (available_ > 0)
      {
        *source_ >> x;
        --available_;
      }
    }

    /// Deserializes the entire chunk.
    /// @param f The callback to invoke on each deserialized object.
    void get(std::function<void(T)> f)
    {
      VAST_ENTER();
      while (available_ > 0)
      {
        T x;
        *source_ >> x;
        f(std::move(x));
        --available_;
      }
    }

    /// Retrieves the number of objects available for deserialization.
    /// @return The number of times one can call operator>> on this chunk.
    uint32_t available() const
    {
      return available_;
    }

    /// Retrieves the number of bytes deserialized so far.
    /// @return The number of deserialized bytes.
    size_t bytes() const
    {
      return source_ ? source_->bytes() : 0;
    }

  private:
    chunk const* chunk_;
    uint32_t available_ = 0;
    std::unique_ptr<io::array_input_stream> base_stream_;
    std::unique_ptr<io::compressed_input_stream> compressed_stream_;
    std::unique_ptr<binary_deserializer> source_;
  };

  /// Constructs a chunk.
  /// @param method The compression method to use.
  explicit chunk(io::compression method = io::lz4)
    : compression_(method)
  {
  }

  /// Checks whether the chunk is empty.
  /// @return `true` if the chunk has no elements.
  bool empty() const
  {
    return elements_ == 0;
  }

  /// Retrieves the number of serialized elements in the chunk.
  /// @return The number of elements in the chunk.
  uint32_t size() const
  {
    return elements_;
  }

  /// Retrieves the size in bytes of the compressed buffer.
  /// @return The number of bytes of the serialized and compressed buffer.
  size_t bytes() const
  {
    return sizeof(elements_) + buffer_.size();
  }

  friend bool operator==(chunk const& x, chunk const& y)
  {
    return x.elements_ == y.elements_ && x.buffer_ == y.buffer_;
  }

private:
  friend access;
  void serialize(serializer& sink) const
  {
    sink << elements_;
    sink << buffer_;
  }

  void deserialize(deserializer& source)
  {
    source >> elements_;
    source >> buffer_;
  }

  io::compression compression_;
  uint32_t elements_ = 0;
  buffer_type buffer_;
};

} // namespace vast

#endif
