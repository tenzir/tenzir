#ifndef VAST_PACKER_HPP
#define VAST_PACKER_HPP

#include <cstdint>
#include <cstddef>
#include <streambuf>
#include <vector>

#include "vast/error.hpp"
#include "vast/expected.hpp"

#include "vast/detail/byte_swap.hpp"
#include "vast/detail/coded_deserializer.hpp"
#include "vast/detail/coded_serializer.hpp"
#include "vast/detail/tallybuf.hpp"

namespace vast {

/// Serializes elements into a streambuffer, recording its individual offsets
/// for later random access.
///
/// The byte stream has the following format:
///
///         varaible       variable    4 bytes
///     +---........---+---........---+--------+
///     |     data     | offset table | offset |
///     +---........---+---........---+--------|
///                    ^                  |
///                    |------------------|
///
/// The *offset* is a 32-bit unsigned integer that points to the beginning
/// of the *offset table*, which is a varbyte and delta-encoded sequence of
/// relative positions, where each position points to a contiguous memory
/// region inside *data*.
///
/// @relates unpacker overlay
class packer {
  packer(const packer&) = delete;
  packer& operator=(const packer&) = delete;

public:
  /// Constructs a packer from a stream buffer.
  /// @param streambuf A reference to a streambuf to write into.
  template <class Streambuf>
  explicit packer(Streambuf& streambuf)
    : streambuf_{streambuf},
      serializer_{streambuf_},
      deserializer_{streambuf_} {
  }

  /// If not called previously, invokes finish().
  ~packer();

  /// Writes an element into the segment.
  /// @param e The event to serialize.
  template <class T>
  void pack(T&& x) {
    offsets_.push_back(streambuf_.put());
    serializer_ << x;
  }

  /// Deserializes an object at a given position *i*.
  /// @tparam T The type to deserialize at position *i*.
  /// @param i The offset at which to deserialize.
  /// @returns An instance of type `T`.
  template <class T>
  expected<T> unpack(size_t i) {
    if (i >= size())
      return ec::unspecified;
    auto pos = streambuf_.pubseekoff(offsets_[i], std::ios::beg, std::ios::in);
    if (pos == -1)
      return ec::unspecified;
    T x;
    deserializer_ >> x;
    // In case put and get area are coupled, we must seek back to the end for
    // further appending.
    streambuf_.pubseekoff(0, std::ios::end, std::ios::out);
    return x;
  }

  /// @returns the number of elements packed so far.
  size_t size() const;

  /// Completes writing the segment by adding the trailer.
  /// @returns The total number of bytes written into the stream buffer.
  size_t finish();

private:
  using streambuf_type = detail::tallybuf<std::streambuf>;
  using serializer_type = detail::coded_serializer<streambuf_type&>;
  using deserializer_type = detail::coded_deserializer<streambuf_type&>;

  std::vector<uint32_t> offsets_;
  streambuf_type streambuf_;
  serializer_type serializer_;
  deserializer_type deserializer_;
};

} // namespace vast

#endif
