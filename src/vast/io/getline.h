#ifndef VAST_IO_GETLINE_H
#define VAST_IO_GETLINE_H

#include <string>

namespace vast {
namespace io {

class input_stream;

/// Copies lines from an input stream into a string. Unlike `std::getline`,
/// this function treats all three kinds of new line sequences (i.e., `\r`,
/// `\n`, and `\r\n`) as a line separator.
///
/// @param in The input stream to read from.
///
/// @param line The result parameter which contains a single line.
///
/// @return `true` *iff* extracting a line from *in* succeeded.
bool getline(input_stream& in, std::string& line);

} // namespace io
} // namespace vast

#endif
