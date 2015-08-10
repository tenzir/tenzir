#ifndef VAST_CONCEPT_SERIALIZABLE_BINARY_DESERIALIZER_H
#define VAST_CONCEPT_SERIALIZABLE_BINARY_DESERIALIZER_H

#include "vast/concept/serializable/deserializer.h"
#include "vast/io/coded_stream.h"

namespace vast {

/// Deserializes binary objects from an input stream.
class binary_deserializer : public deserializer<binary_deserializer> {
public:
  /// Constructs a deserializer with an input stream.
  /// @param source The input stream to read from.
  binary_deserializer(io::input_stream& source) : source_{source} {
  }

  uint64_t begin_sequence() {
    uint64_t size;
    if (!source_.read_varbyte(&size))
      return 0;
    bytes_ += util::varbyte::size(size);
    return size;
  }

  template <typename T>
  auto read(T& x) -> std::enable_if_t<std::is_arithmetic<T>::value> {
    source_.read<T>(&x);
    bytes_ += sizeof(T);
  }

  void read(void* data, size_t size) {
    source_.read_raw(data, size);
    bytes_ += size;
  }

  uint64_t bytes() const {
    return bytes_;
  }

private:
  io::coded_input_stream source_;
  uint64_t bytes_ = 0;
};

} // namespace vast

#endif
