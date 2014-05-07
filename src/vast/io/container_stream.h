#ifndef VAST_IO_CONTAINER_STREAM_H
#define VAST_IO_CONTAINER_STREAM_H

#include "vast/io/array_stream.h"

namespace vast {
namespace io {

namespace detail {

template <typename T>
using is_string = std::is_same<T, std::string>;

template <typename T>
using is_vector = std::is_same<T, std::vector<typename T::value_type,
                                              typename T::allocator_type>>;

template <typename T>
using is_byte_container =
  std::integral_constant<
    bool,
    sizeof(typename T::value_type) == 1
      && (is_string<T>::value || is_vector<T>::value)
  >;

} // namespace detail

/// An output stream that appends to an STL container.
template <typename Container>
class container_output_stream : public output_stream
{
  container_output_stream(container_output_stream const&) = delete;
  container_output_stream& operator=(container_output_stream const&) = delete;

  static_assert(detail::is_byte_container<Container>::value,
                "need byte container for container_output_stream");

public:
  /// Constructs a container output stream.
  /// @param container The STL container.
  explicit container_output_stream(Container& container)
    : container_(container)
  {
  }

  container_output_stream(container_output_stream&&) = default;
  container_output_stream& operator=(container_output_stream&&) = default;

  virtual bool next(void** data, size_t* size) override
  {
    auto old_size = container_.size();
    container_.resize(old_size < container_.capacity()
                      ? container_.capacity()
                      : std::max(old_size * 2, size_t(16)));

    *data = &*container_.begin() + old_size;
    *size = container_.size() - old_size;
    return true;
  }

  virtual void rewind(size_t bytes) override
  {
    if (bytes > container_.size())
      container_.clear();
    else
      container_.resize(container_.size() - bytes);
  }

  virtual uint64_t bytes() const override
  {
    return container_.size();
  }

private:
  Container& container_;
};

template <typename Container>
inline container_output_stream<Container>
make_container_output_stream(Container& container)
{
  return container_output_stream<Container>(container);
}

/// @relates make_array_input_stream
template <typename Container>
inline array_input_stream
make_container_input_stream(Container const& container, size_t block_size = 0)
{
  return make_array_input_stream(container, block_size);
}

} // namespace io
} // namespace vast

#endif
