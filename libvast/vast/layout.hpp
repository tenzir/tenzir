#ifndef VAST_LAYOUT_HPP
#define VAST_LAYOUT_HPP

#include <cstdint>
#include <cstddef>
#include <streambuf>
#include <vector>

#include "vast/chunk.hpp"
#include "vast/error.hpp"
#include "vast/expected.hpp"

#include "vast/detail/assert.hpp"
#include "vast/detail/byte_swap.hpp"
#include "vast/detail/coded_deserializer.hpp"
#include "vast/detail/coded_serializer.hpp"
#include "vast/detail/tallybuf.hpp"

namespace vast {

/// An overlay over a contiguous byte of providing random-access semantics.
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
struct layout {
  using entry_type = uint32_t;

  /// Constructs a layout by writing into a stream buffer.
  class writer {
    writer(const writer&) = delete;
    writer& operator=(const writer&) = delete;

  public:
    /// Constructs a writer from a stream buffer.
    /// @param streambuf A reference to a streambuf to write into.
    explicit writer(std::streambuf& streambuf);

    /// If not called previously, invokes finish().
    ~writer();

    /// Writes an object into the layout.
    /// @param x The object to serialize.
    template <class T>
    expected<void> write(T&& x) {
      auto offset = streambuf_.put();
      if (auto e = serializer_.apply(const_cast<T&>(x))) {
        // Restore previous stream buffer position in case of failure.
        if (streambuf_.put() != offset)
          streambuf_.pubseekpos(offset, std::ios::out);
        return e;
      }
      offsets_.push_back(offset);
      return no_error;
    }

    /// Deserializes an object at a given position *i*.
    /// @tparam T The type to deserialize at position *i*.
    /// @param i The offset at which to deserialize.
    /// @returns An instance of type `T`.
    template <class T>
    expected<T> read(size_t i) {
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
    size_t size() const;

    /// @returns the nubmer of bytes written so far into the underlying stream
    /// buffer.
    friend uint64_t bytes(const writer& p);

  private:
    using streambuf_type = detail::tallybuf<std::streambuf>;
    using serializer_type = detail::coded_serializer<streambuf_type&>;
    using deserializer_type = detail::coded_deserializer<streambuf_type&>;

    std::vector<uint32_t> offsets_;
    streambuf_type streambuf_;
    serializer_type serializer_;
    deserializer_type deserializer_;
  };

  /// Reads a layout from a stream buffer.
  class reader {
  public:
    /// Constructs a layout reader froma stream buffer.
    /// @param streambuf The stream buffer to read from.
    reader(std::streambuf& streambuf);

    /// Deserializes an object at a given position *i*.
    /// @tparam T The type to deserialize at position *i*.
    /// @param i The offset at which to deserialize.
    /// @returns An instance of type `T`.
    /// @pre `i < size()`
    template <class T>
    expected<T> read(size_t i) {
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
    expected<T> read() {
      T x;
      deserializer_ >> x;
      return x;
    }

    /// @returns the number of elements in the packed seqeuence.
    size_t size() const;

  private:
    using deserializer_type = detail::coded_deserializer<std::streambuf&>;

    std::vector<uint32_t> offsets_;
    std::streambuf& streambuf_;
    deserializer_type deserializer_;
  };

  /// A lazy access mechanism to read from an in-memory layout.
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
    const char* operator[](size_t i) const;

    /// Accesses a block of memory at a particular offset.
    /// @param i The offset to get a pointer to.
    const char* at(size_t i) const;

    /// @returns The number of elements in the viewer.
    size_t size() const;

    /// Unpacks an object at a given offset.
    /// @param i The offset of the object to unpack.
    /// @returns A deserialized object of type `T`.
    /// @pre `i < size()`
    template <class T>
    expected<T> unpack(size_t i) const {
      VAST_ASSERT(i < size());
      if (!chunk_)
        return ec::unspecified;
      std::streamoff position = (*this)[i] - chunk_->data();
      if (charbuf_.pubseekpos(position, std::ios::in) != position)
        return ec::unspecified;
      T x;
      if (auto e = deserializer_.apply(x))
        return e;
      return x;
    }

    /// Retrieves a pointer to the underlying chunk.
    chunk_ptr chunk() const;

  private:
    class offset_table {
    public:
      offset_table() = default;
      explicit offset_table(const char* ptr);
      size_t operator[](size_t i) const;
      size_t size() const;
    private:
      const char* table_;
      size_t size_ = 0;
    };

    static const char* offset_table_start(chunk_ptr chk);

    mutable caf::charbuf charbuf_;
    mutable detail::coded_deserializer<caf::charbuf&> deserializer_;
    offset_table offsets_;
    chunk_ptr chunk_;
  };
};

} // namespace vast

#endif
