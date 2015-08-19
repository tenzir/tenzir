#ifndef VAST_UTIL_BUFFER_H
#define VAST_UTIL_BUFFER_H

#include <cstring> // memcpy
#include <type_traits>

#include "vast/util/assert.h"

namespace vast {
namespace util {

/// A shallow buffer abstraction.
/// @tparam The Byte type of the buffer.
template <typename Byte>
class buffer {
  static_assert(sizeof(Byte) == 1, "invalid byte specification");
  using byte_type = typename std::remove_const<Byte>::type;
  using const_byte_type = typename std::add_const<Byte>::type;
  using void_type =
    typename std::conditional<
      std::is_const<Byte>::value, void const,
      void
    >::type;

public:
  buffer() = default;

  buffer(void_type* start, size_t size) {
    assign(start, size);
  }

  explicit operator bool() const {
    return get() != nullptr;
  }

  Byte* get() const {
    return start_;
  }

  template <typename T = Byte>
  T* cast() {
    return reinterpret_cast<T*>(start_);
  }

  template <typename U = Byte>
  std::enable_if_t<std::is_const<U>::value> read(void* data, size_t n) {
    VAST_ASSERT(n <= size());
    std::memcpy(reinterpret_cast<byte_type*>(data), start_, n);
    advance(n);
  }

  template <typename U = Byte>
  std::enable_if_t<!std::is_const<U>::value> write(void const* data, size_t n) {
    VAST_ASSERT(n <= size());
    std::memcpy(start_, reinterpret_cast<const_byte_type*>(data), n);
    advance(n);
  }

  void reset() {
    start_ = end_ = nullptr;
  }

  void assign(void_type* start, size_t size) {
    start_ = reinterpret_cast<Byte*>(start);
    end_ = start_ + size;
  }

  size_t size() const {
    return end_ - start_;
  }

  void advance(size_t n) {
    start_ += n;
  }

private:
  Byte* start_ = nullptr;
  Byte* end_ = nullptr;
};

} // namespace util
} // namespace vast

#endif
