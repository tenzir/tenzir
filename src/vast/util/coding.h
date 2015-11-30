#ifndef VAST_UTIL_CODING_H
#define VAST_UTIL_CODING_H

#include <iomanip>
#include <limits>
#include <sstream>
#include <type_traits>

#include "vast/maybe.h"

namespace vast {
namespace util {

/// Converts a byte value into an ASCII character.
/// @param b The byte to convert.
/// @returns The ASCII representation of *b*.
/// @relates byte_to_hex hex_to_byte
template <
  typename T,
  typename = std::enable_if_t<std::is_integral<T>::value>
>
char byte_to_char(T b) {
  return b < 10 ? '0' + b : 'a' + b - 10;
}

/// Converts a byte value into a hex value.
/// @param b The byte to convert.
/// @returns b The two hex nibbles as `(high, low)` pair.
/// @relates byte_to_char hex_to_byte
template <
  typename T,
  typename = std::enable_if_t<std::is_integral<T>::value>
>
std::pair<char, char> byte_to_hex(T b) {
  static constexpr char hex[] = "0123456789ABCDEF";
  return {hex[(b >> 4) & 0x0f], hex[b & 0x0f]};
}

/// Converts a single hex character into its byte value.
/// @param hex The hex character.
/// @returns The byte value of *hex* or 0 if *hex* is not a valid hex char.
/// @relates byte_to_hex byte_to_char
template <
  typename T,
  typename = std::enable_if_t<std::is_integral<T>::value>
>
char hex_to_byte(T hex) {
  if (hex >= '0' && hex <= '9')
    return hex - '0';
  if (hex >= 'A' && hex <= 'F')
    return hex - 'A' + 10;
  if (hex >= 'a' && hex <= 'f')
    return hex - 'a' + 10;
  return '\0';
}

/// Converts two characters representing a hex byte into a single byte value.
/// @param hi The high hex nibble.
/// @param lo The low hex nibble.
/// @relates byte_to_hex byte_to_char
template <
  typename T,
  typename = std::enable_if_t<std::is_integral<T>::value>
>
char hex_to_byte(T hi, T lo) {
  auto byte = hex_to_byte(hi) << 4;
  byte |= hex_to_byte(lo);
  return byte;
}

/// Converts a decimal value to a hex string.
/// @param x The value to convert.
/// @returns The hex representation of *x*.
template <typename T>
auto to_hex(T x)
  -> std::enable_if_t<std::is_integral<T>::value, std::string> {
  std::stringstream ss;
  ss << std::hex << std::setfill('0') << x;
  return ss.str();
}

/// Converts a hex string to a signed decimal value.
/// @param x The hex string to convert.
/// @returns The decimal value of *x*.
template <typename T>
auto from_hex(std::string const& str)
  -> std::enable_if_t<std::is_signed<T>::value, maybe<T>> {
  try {
    return std::stoll(str, nullptr, 16);
  } catch (...) {
    return nil;
  }
}

/// Converts a hex string to an unsigned decimal value.
/// @param x The hex string to convert.
/// @returns The decimal value of *x*.
template <typename T>
auto from_hex(std::string const& str)
  -> std::enable_if_t<std::is_unsigned<T>::value, maybe<T>> {
  try {
    return std::stoull(str, nullptr, 16);
  } catch (...) {
    return nil;
  }
}

namespace varbyte {
namespace detail {

template <typename T>
size_t constexpr cast_unsigned(T x) {
  return static_cast<std::make_unsigned_t<T>>(x);
}

} // namespace detail

template <typename T>
std::enable_if_t<(sizeof(T) > 4), size_t>
constexpr size(T x) {
  return detail::cast_unsigned(x) >= (T(1) << 63) ? 10 :
         detail::cast_unsigned(x) >= (T(1) << 56) ? 9 :
         detail::cast_unsigned(x) >= (T(1) << 49) ? 8 :
         detail::cast_unsigned(x) >= (T(1) << 42) ? 7 :
         detail::cast_unsigned(x) >= (T(1) << 35) ? 6 :
         detail::cast_unsigned(x) >= (T(1) << 28) ? 5 :
         detail::cast_unsigned(x) >= (T(1) << 21) ? 4 :
         detail::cast_unsigned(x) >= (T(1) << 14) ? 3 :
         detail::cast_unsigned(x) >= (T(1) << 7) ? 2 : 1;
}

template <typename T>
std::enable_if_t<(sizeof(T) > 2 && sizeof(T) <= 4), size_t>
constexpr size(T x) {
  return detail::cast_unsigned(x) >= (T(1) << 28) ? 5 :
         detail::cast_unsigned(x) >= (T(1) << 21) ? 4 :
         detail::cast_unsigned(x) >= (T(1) << 14) ? 3 :
         detail::cast_unsigned(x) >= (T(1) << 7) ? 2 : 1;
}

template <typename T>
std::enable_if_t<(sizeof(T) == 2), size_t>
constexpr size(T x) {
  return detail::cast_unsigned(x) >= (T(1) << 14) ? 3 :
         detail::cast_unsigned(x) >= (T(1) << 7) ? 2 : 1;
}

template <typename T>
std::enable_if_t<sizeof(T) == 1, size_t>
constexpr size(T x) {
  return detail::cast_unsigned(x) >= (1 << 7) ? 2 : 1;
}

/// The maximum number of bytes required to encode an integral type *T*.
template <typename T>
size_t constexpr max_size() {
  return std::numeric_limits<T>::digits % 7 == 0
    ? std::numeric_limits<T>::digits / 7
    : std::numeric_limits<T>::digits / 7 + 1;
}

/// Encodes an integral type with *Variable Byte*.
/// @tparam An integral type.
/// @param x The value to encode.
/// @param sink the output buffer to write into.
/// @returns The number of bytes written into *sink*.
template <typename T>
std::enable_if_t<std::is_integral<T>::value, size_t>
encode(T x, void* sink) {
  auto out = reinterpret_cast<uint8_t*>(sink);
  while (x > 0x7f)
  {
    *out++ = (static_cast<uint8_t>(x) & 0x7f) | 0x80;
    x >>= 7;
  }
  *out++ = static_cast<uint8_t>(x) & 0x7f;
  return out - reinterpret_cast<uint8_t*>(sink);
}

/// Decodes an integral type with *Variable Byte*.
/// @tparam An integral type.
/// @param source The source buffer.
/// @param x The result of the decoding.
/// @returns The number of bytes read from *source*.
template <typename T>
std::enable_if_t<std::is_integral<T>::value, size_t>
decode(void const* source, T* x) {
  auto in = reinterpret_cast<uint8_t const*>(source);
  size_t i = 0;
  T result = 0;
  uint8_t low7;
  do
  {
    low7 = *in++;
    result |= static_cast<T>(low7 & 0x7f) << (7 * i);
    ++i;
  }
  while (low7 & 0x80);
  *x = result;
  return i;
}

} // namespace varbyte
} // namespace util
} // namespace vast

#endif
