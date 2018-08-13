/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include <iomanip>
#include <streambuf>
#include <vector>

#include <caf/expected.hpp>
#include <caf/streambuf.hpp>

#include "vast/chunk.hpp"

#include "vast/detail/byte.hpp"
#include "vast/detail/counting_stream_buffer.hpp"
#include "vast/detail/coded_deserializer.hpp"
#include "vast/detail/coded_serializer.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/detail/span.hpp"

namespace vast::detail {

/// A random-access abstraction over a contiguous block of bytes.
/// The memory has the following layout:
///
///                              footer
///                      ______________________
///                     /                      \
///     +---........---+---........---+---------+
///     |     data     | offset table |  entry  |
///     +---........---+---........---+---------|
///                    ^                  |
///                    |__________________|
///
/// The *offset table* contains unsigned integer values the represent the
/// starting positions for the elements from the beginning of *data*. The
/// trailing *entry* offset is a 32-bit unsigned integer that points to the
/// beginning of the *offset table*.
struct overlay {
  /// The type of the last offset that points to the beginning of the offset
  /// table.
  using entry_type = uint32_t;

  /// Constructs an overlay by writing into a stream buffer.
  class writer {
    writer(const writer&) = delete;
    writer& operator=(const writer&) = delete;

  public:
    /// Constructs a writer from a stream buffer.
    /// @param streambuf A reference to a streambuf to write into.
    explicit writer(std::streambuf& streambuf);

    /// If not called previously, invokes finish().
    ~writer() {
      finish();
    }

    /// Writes an object into the overlay.
    /// @param x The object to serialize.
    template <class T>
    caf::expected<void> write(T&& x) {
      auto offset = streambuf_.put();
      if (auto e = serializer_.apply(const_cast<T&>(x))) {
        // Restore previous stream buffer position in case of failure.
        if (streambuf_.put() != offset)
          streambuf_.pubseekpos(offset, std::ios::out);
        return e;
      }
      offsets_.push_back(offset);
      return caf::no_error;
    }

    /// Deserializes an object at a given position *i*.
    /// @tparam T The type to deserialize at position *i*.
    /// @param i The offset at which to deserialize.
    /// @returns An instance of type `T`.
    template <class T>
    caf::expected<T> read(size_t i) {
      if (i >= size())
        return ec::unspecified;
      auto offset = streambuf_.put();
      auto pos = streambuf_.pubseekpos(offsets_[i], std::ios::in);
      if (pos == -1)
        return ec::unspecified;
      T x;
      deserializer_ >> x;
      // In case put and get area are coupled, we must seek back to the end for
      // further appending.
      streambuf_.pubseekpos(offset, std::ios::out);
      return x;
    }

    /// Completes writing the segment by adding the trailer.
    /// @returns The total number of bytes written into the stream buffer.
    size_t finish();

    /// @returns the number of elements packed so far.
    size_t size() const {
      return offsets_.size();
    }

    /// @returns the nubmer of bytes written so far into the underlying stream
    /// buffer.
    friend uint64_t bytes(const writer& p) {
      return p.streambuf_.put();
    }

  private:
    using streambuf_type = counting_stream_buffer<std::streambuf>;

    std::vector<uint32_t> offsets_;
    streambuf_type streambuf_;
    coded_serializer<streambuf_type&> serializer_;
    coded_deserializer<streambuf_type&> deserializer_;
  };

  /// Reads an overlay from a stream buffer.
  class reader {
  public:
    /// Constructs an overlay reader froma stream buffer.
    /// @param streambuf The stream buffer to read from.
    reader(std::streambuf& streambuf);

    /// Deserializes an object at a given position.
    /// @tparam T The type to deserialize.
    /// @param i The offset at which to deserialize.
    /// @returns An instance of type `T`.
    /// @pre `i < size()`
    template <class T>
    caf::expected<T> read(size_t i) {
      if (i >= size())
        return ec::unspecified;
      auto pos = streambuf_.pubseekoff(offsets_[i], std::ios::beg,
                                       std::ios::in);
      if (pos == -1)
        return ec::unspecified;
      T x;
      deserializer_ >> x;
      return x;
    }

    /// Deserializes the next object in the stream.
    /// @tparam T The type to deserialize at position *i*.
    /// @returns An instance of type `T`.
    template <class T>
    caf::expected<T> read() {
      T x;
      deserializer_ >> x;
      return x;
    }

    /// @returns the number of elements in the packed seqeuence.
    size_t size() const {
      return offsets_.size();
    }

  private:
    std::vector<uint32_t> offsets_;
    std::streambuf& streambuf_;
    coded_deserializer<std::streambuf&> deserializer_;
  };

  /// A lazy access mechanism to read from an in-memory overlay.
  class viewer {
  public:
    /// Default-constructs an empty viewer.
    viewer();

    /// Copy-constructs a viewer.
    /// @param other The viewer to copy.
    viewer(const viewer& other);

    /// Constructs a viewer from a chunk.
    /// @param chk The chunk to create a viewer from.
    /// @pre `chk != nullptr`
    explicit viewer(chunk_ptr chk);

    /// Accesses a block of memory at a particular offset.
    /// @param i The offset to get a pointer to.
    /// @pre `i < size()`
    span<const byte> view(size_t i) const;

    /// Deserializes an object at a given position.
    /// @tparam T The type to deserialize.
    /// @param i The offset at which to deserialize.
    /// @returns An instance of type `T`.
    template <class T>
    caf::expected<T> read(size_t i) {
      auto xs = view(i);
      auto ptr = const_cast<byte*>(xs.data()); // to comply w/ streambuffer API
      auto size = narrow_cast<size_t>(xs.size());
      using stream_buffer_type = caf::arraybuf<byte>;
      stream_buffer_type buf{ptr, size};
      coded_deserializer<stream_buffer_type&> deserializer{buf};
      T x;
      deserializer >> x;
      return x;
    }

    /// @returns The number of elements in the viewer.
    size_t size() const;

    /// Retrieves a pointer to the underlying chunk.
    chunk_ptr chunk() const;

  private:
    class offset_table {
    public:
      offset_table() = default;
      explicit offset_table(chunk_ptr chunk);
      size_t operator[](size_t i) const;
      size_t size() const;
    private:
      const char* table_;
      size_t size_ = 0;
    };

    chunk_ptr chunk_;
    offset_table offsets_;
  };
};

} // namespace vast::detail
