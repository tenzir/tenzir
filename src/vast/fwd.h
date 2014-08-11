#ifndef VAST_FWD_H
#define VAST_FWD_H

#include <cstdint>

namespace vast {

struct access;
class serializer;
class deserializer;

struct value_invalid;
class value;
enum type_tag : uint8_t;

class time_point;
class time_range;
using time_duration = time_range;
using time_period = time_range;
class pattern;
class address;
class subnet;
class port;
class record;
class vector;
class set;
class table;

class bitstream;
class null_bitstream;
class ewah_bitstream;

namespace expr {
class ast;
}

namespace io {
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
template <typename Container> class container_output_stream;
enum compression : uint8_t;
} // namespace io

namespace util {
class json;
} // namespace util

} // namespace vast

#endif
