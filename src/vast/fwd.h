#ifndef VAST_FWD_H
#define VAST_FWD_H

#include <cstdint>

namespace vast {

struct access;
class serializer;
class deserializer;

class value;
enum value_type : uint8_t;

class address;
class time_range;
class time_point;
class port;
class prefix;
class record;
class regex;
class set;
class string;
class table;
class vector;

namespace detail {
template <typename T> void save(serializer& sink, T const& x);
template <typename T> void load(deserializer& source, T& x);
} // namespace detail

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

} // namespace vast

#endif
