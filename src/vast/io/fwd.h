#ifndef VAST_IO_FWD_H
#define VAST_IO_FWD_H

#include <cstdint>

namespace vast {
namespace io {

class access;
class input_stream;
class output_stream;
class array_input_stream;
class array_output_stream;
class buffered_input_stream;
class buffered_output_stream;
class coded_input_stream;
class coded_output_stream;
class file_input_stream;
class file_output_stream;
class serializer;
class deserializer;

template <typename Container>
class container_output_stream;
enum compression : uint8_t;

} // namespace io
} // namespace vast

#endif
