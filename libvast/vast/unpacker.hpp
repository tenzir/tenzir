#ifndef VAST_UNPACKER_HPP
#define VAST_UNPACKER_HPP

#include <caf/streambuf.hpp>

#include "vast/expected.hpp"
#include "vast/overlay.hpp"

#include "vast/detail/coded_deserializer.hpp"

namespace vast {

/// Selectively deserializes from a packed chunk.
/// @relates packer overlay
class unpacker {
  using deserializer_type = detail::coded_deserializer<caf::charbuf>;

public:
  /// Default-constructs an empty unpacker.
  unpacker() = default;

  /// Default-constructs a packer with one builder.
  explicit unpacker(chunk_ptr chk);

  /// Deserializes an object at a given position i.
  /// @tparam T The type to deserialize at position *i*.
  /// @param i The offset at which to deserialize.
  /// @returns An instance of type `T`.
  template <class T>
  expected<T> unpack(size_t i) {
    VAST_ASSERT(i < overlay_.size());
    auto ptr = const_cast<char*>(overlay_[i]); // won't touch it
    auto base = overlay_.chunk()->data();
    auto size = overlay_.chunk()->size() - (ptr - base);
    // TODO: make this guy a member and seek instead.
    deserializer_type deserializer{nullptr, ptr, size};
    T x;
    if (auto e = deserializer.apply(x))
      return e;
    return x;
  }

  size_t size() const;

private:
  overlay overlay_;
};

} // namespace vast

#endif
