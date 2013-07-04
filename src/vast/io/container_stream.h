#ifndef VAST_IO_CONTAINER_STREAM_H
#define VAST_IO_CONTAINER_STREAM_H

#include "vast/io/stream.h"

namespace vast {
namespace io {

/// An output stream that appends to an STL container.
template <typename Container>
class container_output_stream : public output_stream
{
  container_output_stream(container_output_stream const&) = delete;
  container_output_stream& operator=(container_output_stream const&) = delete;

  template <typename T>
  using is_string = std::is_same<T, std::string>;

  template <typename T>
  using is_vector = std::is_same<T, std::vector<typename T::value_type,
                                                typename T::allocator_type>>;
  static_assert(
      is_string<Container>::value || is_vector<Container>::value,
      "container_output_stream only supports strings and byte vectors");

  static_assert(
      sizeof(typename Container::value_type) == 1,
      "container_output_stream requires a byte container");

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

} // namespace io
} // namespace vast

#endif
