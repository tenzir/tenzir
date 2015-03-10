#ifndef VAST_CONCEPT_SERIALIZABLE_BINARY_SERIALIZER_H
#define VAST_CONCEPT_SERIALIZABLE_BINARY_SERIALIZER_H

#include "vast/concept/serializable/serializer.h"
#include "vast/io/coded_stream.h"

namespace vast {

/// Serializes binary objects into an output stream.
class binary_serializer : public serializer<binary_serializer>
{
public:
  /// Constructs a serializer with an output stream.
  /// @param sink The output stream to write into.
  binary_serializer(io::output_stream& sink)
    : sink_{sink}
  {
  }

  void begin_sequence(uint64_t size)
  {
    sink_.write_varbyte(&size);
    bytes_ += util::varbyte::size(size);
  }

  void end_sequence()
  {
  }

  template <typename T>
  auto write(T x)
    -> std::enable_if_t<std::is_arithmetic<T>::value>
  {
    sink_.write<T>(&x);
    bytes_ += sizeof(T);
  }

  void write(void const* data, size_t size)
  {
    sink_.write_raw(data, size);
    bytes_ += size;
  }

  uint64_t bytes() const
  {
    return bytes_;
  }

private:
  io::coded_output_stream sink_;
  uint64_t bytes_ = 0;
};

} // namespace vast

#endif
