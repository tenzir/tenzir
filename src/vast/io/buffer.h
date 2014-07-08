#ifndef VAST_IO_BUFFER_H
#define VAST_IO_BUFFER_H

#include <type_traits>

namespace vast {
namespace io {

/// A shallow buffer that can be used for reading or writing.
template <typename T>
class buffer
{
  using byte_type =
    std::conditional_t<std::is_const<T>::value, uint8_t const, uint8_t>;

public:
  buffer() = default;

  buffer(T* data, size_t size)
    : data_{reinterpret_cast<byte_type*>(data)},
      size_{size}
  {
  }

  /// Checks whether the buffer is valid.
  /// @returns `true` iff the data pointer is not `nullptr`.
  explicit operator bool() const
  {
    return data_ != nullptr;
  }

  template <typename U>
  auto as(size_t offset = 0) const
    -> std::conditional_t<std::is_const<T>::value, U const*, U*>
  {
    using type = std::conditional_t<std::is_const<T>::value, U const*, U*>;
    return reinterpret_cast<type>(at(offset));
  }

  byte_type* at(size_t offset = 0) const
  {
    assert(offset < size_);
    return data_ + offset;
  }

  byte_type* data() const
  {
    return data_;
  }

  size_t size() const
  {
    return size_;
  }

private:
  byte_type* data_ = nullptr;
  size_t size_ = 0;
};

template <typename T>
buffer<T> make_buffer(T* data, size_t size)
{
  return {data, size};
}

} // namespace io
} // namespace vast

#endif

