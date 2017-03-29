#ifndef VAST_UNPACKER_HPP
#define VAST_UNPACKER_HPP

#include "vast/expected.hpp"

#include "vast/detail/assert.hpp"
#include "vast/detail/byte_swap.hpp"
#include "vast/detail/coded_deserializer.hpp"

namespace vast {

/// Selectively deserializes from a packed sequence.
/// @relates packer
class unpacker {
public:
  template <class Streambuf>
  unpacker(Streambuf& streambuf)
    : streambuf_{streambuf},
      deserializer_{streambuf_} {
    // Locate offset table position.
    auto pos = streambuf.pubseekoff(-4, std::ios::end, std::ios::in);
    if (pos == -1)
      return;
    char buf[4];
    auto got = streambuf.sgetn(buf, 4);
    if (got != 4)
      return;
    auto ptr = reinterpret_cast<uint32_t*>(buf);
    auto offset_table = detail::to_host_order(*ptr);
    // Read offsets.
    pos = streambuf.pubseekoff(offset_table, std::ios::beg, std::ios::in);
    if (pos == -1)
      return;
    deserializer_ >> offsets_;
    VAST_ASSERT(!offsets_.empty());
    // Delta-decode offsets.
    auto cum = uint32_t{0};
    for (auto i = 0u; i < offsets_.size(); ++i) {
      auto delta = offsets_[i];
      offsets_[i] = cum;
      cum += delta;
    }
    offsets_.push_back(cum);
    // Reposition at beginning. This is optional, but facilitates sequential
    // reading.
    streambuf.pubseekoff(0, std::ios::beg, std::ios::in);
  }

  /// Deserializes an object at a given position *i*.
  /// @tparam T The type to deserialize at position *i*.
  /// @param i The offset at which to deserialize.
  /// @returns An instance of type `T`.
  /// @pre `i < size()`
  template <class T>
  expected<T> unpack(size_t i) {
    if (i >= size())
      return ec::unspecified;
    auto pos = streambuf_.pubseekoff(offsets_[i], std::ios::beg, std::ios::in);
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
  expected<T> unpack() {
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

} // namespace vast

#endif
