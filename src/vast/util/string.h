#ifndef VAST_UTIL_STRING_H
#define VAST_UTIL_STRING_H

#include <string>

namespace vast {
namespace util {

/// Escapes all non-printable characters in a string with `\xAA` where `AA` is
/// the byte in hexadecimal representation.
/// @param str The string to escape.
/// @param all If `true` escapes every single character in *str*.
/// @returns The escaped string of *str*.
/// @relates byte_unescape
std::string byte_escape(std::string const& str, bool all = false);

/// Unescapes a byte-escaped string, i.e., replaces all occurrences of `\xAA`
/// with the value of the byte `AA`.
/// @param str The string to unescape.
/// @returns The unescaped string of *str*.
/// @relates byte_escape
std::string byte_unescape(std::string const& str);

/// Escapes a string according to JSON escaping.
/// @param str The string to escape.
/// @returns The escaped string.
/// @relates json_unescape
std::string json_escape(std::string const& str);

/// Unescapes a string escaped with JSON escaping.
/// @param str The string to unescape.
/// @returns The unescaped string.
/// @relates json_escape
std::string json_unescape(std::string const& str);

} // namespace util
} // namespace vast

#endif
