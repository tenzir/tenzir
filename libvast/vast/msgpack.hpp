/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include <caf/none.hpp>
#include <caf/optional.hpp>
#include <caf/variant.hpp>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include <vast/byte.hpp>
#include <vast/detail/assert.hpp>
#include <vast/detail/byte_swap.hpp>
#include <vast/detail/operators.hpp>
#include <vast/detail/overload.hpp>
#include <vast/detail/type_traits.hpp>
#include <vast/span.hpp>
#include <vast/time.hpp>

/// The [MessagePack](https://github.com/msgpack/msgpack/blob/master/spec.md)
/// object serialization specification.
/// - MessagePack is an object serialization specification like JSON.
/// - MessagePack has two concepts: type system and formats.
/// - Serialization is conversion from application objects into MessagePack
///   formats via MessagePack type system.
/// - Deserialization is conversion from MessagePack formats into application
///   objects via MessagePack type system.
///
/// MessagePack defines the following types:
/// - Integer: represents an integer
/// - Nil: represents nil
/// - Boolean: represents true or false
/// - Float: represents a IEEE 754 double precision floating point number
///          including NaN and Infinity
/// - Raw:
///   - String: extending Raw type represents a UTF-8 string
///   - Binary: extending Raw type represents a byte array
/// - Array: represents a sequence of objects
/// - Map: represents key-value pairs of objects
/// - Extension: represents a tuple of type information and a byte array where
///              type information is an integer whose meaning is defined by
///              applications or MessagePack specification
///   - Timestamp: represents an instantaneous point on the time-line in the
///     world that is independent from time zones or calendars. Maximum
///     precision is nanoseconds.
namespace vast::msgpack {

// -- types -------------------------------------------------------------------

/// Defines the data representation of this and subsequent bytes.
enum format : uint8_t {
  // nil format
  nil = 0xc0,
  // bool format family
  false_ = 0xc2,
  true_ = 0xc3,
  // int format family
  positive_fixint = 0b0111'1111, // 0x00 - 0x7f
  negative_fixint = 0b1111'1111, // 0xe0 - 0xff
  uint8 = 0xcc,
  uint16 = 0xcd,
  uint32 = 0xce,
  uint64 = 0xcf,
  int8 = 0xd0,
  int16 = 0xd1,
  int32 = 0xd2,
  int64 = 0xd3,
  // float format family
  float32 = 0xca,
  float64 = 0xcb,
  // str format family
  fixstr = 0b1011'1111, // 0xa0 - 0xbf
  str8 = 0xd9,
  str16 = 0xda,
  str32 = 0xdb,
  // bin format family
  bin8 = 0xc4,
  bin16 = 0xc5,
  bin32 = 0xc6,
  // array format family
  fixarray = 0b1001'1111, // 0x90 - 0x9f
  array16 = 0xdc,
  array32 = 0xdd,
  // map format family
  fixmap = 0b1000'1111, // 0x80 - 0x8f
  map16 = 0xde,
  map32 = 0xdf,
  // ext format family
  fixext1 = 0xd4,
  fixext2 = 0xd5,
  fixext4 = 0xd6,
  fixext8 = 0xd7,
  fixext16 = 0xd8,
  ext8 = 0xc7,
  ext16 = 0xc8,
  ext32 = 0xc9
};

/// The type of integers of extension types.
using extension_type = int8_t;

// -- utilities ---------------------------------------------------------------

/// @relates format
constexpr bool is_positive_fixint(format x) {
  return (x >> 7) == 0;
}

/// @relates format
constexpr bool is_negative_fixint(format x) {
  return (x >> 5) == 0b0000'0111;
}

/// @relates format
constexpr bool is_fixstr(format x) {
  return (x >> 5) == (fixstr >> 5);
}

/// @relates format
constexpr bool is_fixarray(format x) {
  return (x >> 4) == (fixarray >> 4);
}

/// @relates format
constexpr bool is_fixmap(format x) {
  return (x >> 4) == (fixmap >> 4);
}

/// @relates format
constexpr bool is_fix_sequence(format x) {
  return is_fixstr(x) || is_fixarray(x) || is_fixmap(x);
}

/// @relates format
constexpr bool is_bool(format x) {
  return x == false_ || x == true_;
}

/// @relates format
constexpr bool is_int(format x) {
  return x == int8 || x == int16 || x == int32 || x == int64;
}

/// @relates format
constexpr bool is_uint(format x) {
  return x == uint8 || x == uint16 || x == uint32 || x == uint64;
}

/// @relates format
constexpr bool is_float(format x) {
  return x == float32 || x == float64;
}

/// @relates format
constexpr bool is_str(format x) {
  return x == str8 || x == str16 || x == str32;
}

/// @relates format
constexpr bool is_bin(format x) {
  return x == bin8 || x == bin16 || x == bin32;
}

/// @relates format
constexpr bool is_fixext(format x) {
  return x == fixext1 || x == fixext2 || x == fixext4 || x == fixext8
         || x == fixext16;
}

/// @relates format
constexpr bool is_ext(format x) {
  return x == ext8 || x == ext16 || x == ext32;
}

/// @relates format
constexpr size_t fixstr_size(format x) {
  return static_cast<size_t>(x & 0b0001'1111);
}

/// @relates format
constexpr size_t fixarray_size(format x) {
  return static_cast<size_t>(x & 0b0000'1111);
}

/// @relates format
constexpr size_t fixmap_size(format x) {
  return static_cast<size_t>(x & 0b0000'1111);
}

/// @relates format
template <format Format>
constexpr size_t header_size() {
  if constexpr (is_int(Format) || is_uint(Format) || is_float(Format)
                || is_fix_sequence(Format))
    return 1;
  else if constexpr (Format == str8 || Format == bin8 || is_fixext(Format))
    return 2;
  else if constexpr (Format == str16 || Format == bin16 || Format == array16
                     || Format == map16 || Format == ext8)
    return 3;
  else if constexpr (Format == ext16)
    return 4;
  else if constexpr (Format == str32 || Format == bin32 || Format == array32
                     || Format == map32)
    return 5;
  else if constexpr (Format == ext32)
    return 6;
  return 0;
}

/// @relates format
template <format Format>
constexpr auto make_size(size_t x) {
  using vast::detail::narrow_cast;
  if constexpr (Format == str8 || Format == bin8 || Format == ext8)
    return narrow_cast<uint8_t>(x);
  if constexpr (Format == str16 || Format == bin16 || Format == ext16
                || Format == array16 || Format == map16)
    return narrow_cast<uint16_t>(x);
  if constexpr (Format == str32 || Format == bin32 || Format == ext32
                || Format == array32 || Format == map32)
    return narrow_cast<uint32_t>(x);
}

/// @relates format
template <format Format, class T>
constexpr auto native_cast(T x) {
  if constexpr (is_positive_fixint(Format) || Format == uint8)
    return static_cast<uint8_t>(x);
  if constexpr (Format == uint16)
    return static_cast<uint16_t>(x);
  if constexpr (Format == uint32)
    return static_cast<uint32_t>(x);
  if constexpr (Format == uint64)
    return static_cast<int64_t>(x);
  if constexpr (is_negative_fixint(Format) || Format == int8)
    return static_cast<int8_t>(x);
  if constexpr (Format == int16)
    return static_cast<int16_t>(x);
  if constexpr (Format == int32)
    return static_cast<int32_t>(x);
  if constexpr (Format == int64)
    return static_cast<int64_t>(x);
  if constexpr (Format == float32)
    return static_cast<float>(x);
  if constexpr (Format == float64)
    return static_cast<double>(x);
}

/// @relates format
template <format Format>
constexpr size_t capacity() {
  if constexpr (Format == fixext1)
    return 1;
  if constexpr (Format == fixext2)
    return 2;
  if constexpr (Format == fixext4)
    return 4;
  if constexpr (Format == fixext8)
    return 8;
  if constexpr (Format == fixext16)
    return 16;
  if constexpr (is_fixarray(Format) || is_fixmap(Format))
    return (1u << 4) - 1;
  if constexpr (is_fixstr(Format))
    return (1u << 5) - 1;
  if constexpr (Format == str8 || Format == bin8 || Format == ext8)
    return (1u << 8) - 1;
  if constexpr (Format == str16 || Format == bin16 || Format == array16
                || Format == map16 || Format == ext16)
    return (1u << 16) - 1;
  if constexpr (Format == str32 || Format == bin32 || Format == array32
                || Format == map32 || Format == ext32)
    return (1ull << 32) - 1;
  return 0;
}

/// Helper function to convert a numeric value (betweeen 16 and 64 bits)
/// from msgpack data (big-endian) to the host endianness.
/// @param ptr A pointer to the beginning of the numeric value.
/// @returns The converted value starting at *ptr*.
template <class To>
To to_num(const vast::byte* ptr) {
  if constexpr (std::is_unsigned_v<To>) {
    return vast::detail::to_host_order(*reinterpret_cast<const To*>(ptr));
  } else {
    using unsigned_type = std::make_unsigned_t<To>;
    auto x = to_num<unsigned_type>(ptr);
    return static_cast<To>(x);
  }
}

/// A variant structure to access encoded data.
class object {
public:
  explicit object(vast::span<const vast::byte> data)
    : format_{data.empty() ? nil : static_cast<msgpack::format>(data[0])},
      data_{data} {
    // nop
  }

  msgpack::format format() const {
    return format_;
  }

  auto data() const {
    return data_;
  }

private:
  msgpack::format format_;
  vast::span<const vast::byte> data_;
};

class overlay; // forward declaration

/// A view over values of the *array* family.
/// @relates object
class array_view {
public:
  array_view(msgpack::format fmt, size_t size,
             vast::span<const vast::byte> data)
    : format_{fmt}, size_{size}, data_{data} {
    VAST_ASSERT(is_fixarray(fmt) || fmt == array16 || fmt == array32
                || is_fixmap(fixmap) || fmt == map16 || fmt == map32);
  }

  /// @returns The container format.
  msgpack::format format() const {
    return format_;
  }

  /// @returns The number of elements in the array.
  size_t size() const {
    return size_;
  }

  /// @returns A pointer to the beginning of the array data.
  overlay data() const;

private:
  msgpack::format format_;
  size_t size_;
  vast::span<const vast::byte> data_;
};

/// A view over values of the *ext* family.
/// @relates object
class ext_view : vast::detail::equality_comparable<ext_view> {
public:
  ext_view(msgpack::format fmt, int8_t type, vast::span<const vast::byte> data)
    : format_{fmt}, type_{type}, data_{data} {
    VAST_ASSERT(fmt == fixext1 || fmt == fixext2 || fmt == fixext4
                || fmt == fixext8 || fmt == fixext16 || fmt == ext8
                || fmt == ext16 || fmt == ext32);
  }

  msgpack::format format() const {
    return format_;
  }

  int8_t type() const {
    return type_;
  }

  auto data() const {
    return data_;
  }

private:
  msgpack::format format_;
  int8_t type_;
  vast::span<const vast::byte> data_;
};

/// @relates ext_view
inline bool operator==(const ext_view& x, const ext_view y) {
  return x.format() == y.format() && x.type() == y.type()
         && x.data() == y.data();
}

/// @relates object
template <class Visitor>
decltype(auto) visit(Visitor&& f, const object& x) {
  using const_byte_span = vast::span<const vast::byte>;
  using namespace vast::detail;
  auto fmt = x.format();
  auto data = x.data();
  auto at = [&](auto offset) { return x.data().data() + offset; };
  if (is_positive_fixint(fmt)) {
    return f(static_cast<uint8_t>(data[0]));
  } else if (is_negative_fixint(fmt)) {
    return f(static_cast<int8_t>(data[0]));
  } else if (is_fixstr(fmt)) {
    auto size = fixstr_size(fmt);
    if (size == 0)
      return f(std::string_view{});
    auto str = reinterpret_cast<const char*>(at(1));
    return f(std::string_view{str, size});
  } else if (is_fixarray(fmt)) {
    return f(array_view{fmt, fixarray_size(fmt), data.subspan(1)});
  } else if (is_fixmap(fmt)) {
    return f(array_view{fmt, fixmap_size(fmt) * 2u, data.subspan(1)});
  }
  switch (fmt) {
    default:
      break;
    case nil:
      return f(caf::none);
    case false_:
      return f(false);
    case true_:
      return f(true);
    case uint8:
      return f(static_cast<uint8_t>(data[1]));
    case int8:
      return f(static_cast<int8_t>(data[1]));
    case uint16:
      return f(to_num<uint16_t>(at(1)));
    case int16:
      return f(to_num<int16_t>(at(1)));
    case uint32:
      return f(to_num<uint32_t>(at(1)));
    case int32:
      return f(to_num<int32_t>(at(1)));
    case uint64:
      return f(to_num<uint64_t>(at(1)));
    case int64:
      return f(to_num<int64_t>(at(1)));
    case float32:
      return f(*reinterpret_cast<const float*>(at(1)));
    case float64:
      return f(*reinterpret_cast<const double*>(at(1)));
    case str8: {
      auto size = static_cast<uint8_t>(data[1]);
      if (size == 0)
        return f(std::string_view{});
      auto str = reinterpret_cast<const char*>(at(2));
      return f(std::string_view{str, size});
    }
    case str16: {
      auto size = to_num<uint16_t>(at(1));
      if (size == 0)
        return f(std::string_view{});
      auto str = reinterpret_cast<const char*>(at(3));
      return f(std::string_view{str, size});
    }
    case str32: {
      auto size = to_num<uint32_t>(at(1));
      if (size == 0)
        return f(std::string_view{});
      auto str = reinterpret_cast<const char*>(at(5));
      return f(std::string_view{str, size});
    }
    case bin8: {
      auto size = static_cast<uint8_t>(data[1]);
      return f(size == 0 ? const_byte_span{} : const_byte_span{at(2), size});
    }
    case bin16: {
      auto size = to_num<uint16_t>(at(1));
      return f(size == 0 ? const_byte_span{} : const_byte_span{at(3), size});
    }
    case bin32: {
      auto size = to_num<uint32_t>(at(1));
      return f(size == 0 ? const_byte_span{} : const_byte_span{at(5), size});
    }
    case array16:
      return f(array_view{array16, to_num<uint16_t>(at(1)), data.subspan(3)});
    case array32:
      return f(array_view{array32, to_num<uint32_t>(at(1)), data.subspan(5)});
    case map16:
      return f(
        array_view{map16, to_num<uint16_t>(at(1)) * 2u, data.subspan(3)});
    case map32:
      return f(
        array_view{map32, to_num<uint32_t>(at(1)) * 2u, data.subspan(5)});
    case fixext1: {
      auto ext_type = static_cast<extension_type>(data[1]);
      auto ext_data = data.subspan(2, 1);
      return f(ext_view{fixext1, ext_type, ext_data});
    }
    case fixext2: {
      auto ext_type = static_cast<extension_type>(data[1]);
      auto ext_data = data.subspan(2, 2);
      return f(ext_view{fixext2, ext_type, ext_data});
    }
    case fixext4: {
      auto ext_type = static_cast<extension_type>(data[1]);
      auto ext_data = data.subspan(2, 4);
      if (ext_type == -1) {
        // timestamp32
        using namespace std::chrono;
        auto data32 = to_num<uint32_t>(ext_data.data());
        auto secs = seconds{narrow_cast<int32_t>(data32)};
        return f(vast::time{secs});
      }
      return f(ext_view{fixext4, ext_type, ext_data});
    }
    case fixext8: {
      auto ext_type = static_cast<extension_type>(data[1]);
      auto ext_data = data.subspan(2, 8);
      if (ext_type == -1) {
        // timestamp64
        using namespace std::chrono;
        auto data64 = to_num<uint64_t>(ext_data.data());
        auto ns = data64 >> 34;
        auto secs = data64 & 0x00000003ffffffffull;
        return f(vast::time{seconds{secs} + nanoseconds{ns}});
      }
      return f(ext_view{fixext8, ext_type, ext_data});
    }
    case fixext16: {
      auto ext_type = static_cast<extension_type>(data[1]);
      auto ext_data = data.subspan(2, 16);
      return f(ext_view{fixext16, ext_type, ext_data});
    }
    case ext8: {
      auto size = static_cast<uint8_t>(data[1]);
      auto ext_type = static_cast<extension_type>(data[2]);
      const_byte_span ext_data;
      if (size > 0) {
        ext_data = data.subspan(3, size);
        if (ext_type == -1) {
          // timestamp96
          if (size != 4 + 8)
            return f(fmt);
          using namespace std::chrono;
          auto ns = to_num<uint32_t>(ext_data.data());
          auto secs = to_num<int64_t>(ext_data.data() + 4);
          auto since_epoch = vast::duration{seconds{secs} + nanoseconds{ns}};
          return f(vast::time{since_epoch});
        }
      }
      return f(ext_view{ext8, ext_type, ext_data});
    }
    case ext16: {
      auto size = to_num<uint16_t>(at(1));
      auto ext_type = static_cast<extension_type>(data[3]);
      const_byte_span ext_data;
      if (size > 0)
        ext_data = data.subspan(4, size);
      return f(ext_view{ext16, ext_type, ext_data});
    }
    case ext32: {
      auto size = to_num<uint32_t>(at(1));
      auto ext_type = static_cast<extension_type>(data[5]);
      const_byte_span ext_data;
      if (size > 0)
        ext_data = data.subspan(6, size);
      return f(ext_view{ext32, ext_type, ext_data});
    }
  }
  // We dispatch the format itself if it is unknown.
  return f(fmt);
}

/// @relates object
template <class T>
caf::optional<T> get(const object& o) {
  return visit(
    vast::detail::overload([](auto&&) { return caf::optional<T>{}; },
                           [](T x) { return caf::optional<T>{std::move(x)}; }),
    o);
}

/// A view over a sequence of objects. It is a sequential, single-pass
/// abstraction enabling sequential decoding of objects.
class overlay {
public:
  /// Constructs an overlay from a byte sequence.
  /// @param buffer The sequence of bytes.
  explicit overlay(vast::span<const vast::byte> buffer);

  /// Access the object at the current position.
  /// @pre The underlying buffer must represent a sequence of at least one
  /// well-formed msgpack object.
  /// @returns The object at the current position.
  object get() const;

  /// Advances to the next object.
  /// @returns The number of bytes skipped or 0 on failure.
  size_t next();

  /// Skips multiple objects.
  /// @param n The number of objects to skip.
  /// @returns The number of bytes skipped or if no object got skipped.
  /// @pre The underlying buffer must contain at least *n* more well-formed
  ///      msgpack objects starting from the current position.
  size_t next(size_t n);

private:
  const vast::byte* at(size_t i) const {
    return buffer_.data() + position_ + i;
  }

  vast::span<const vast::byte> buffer_;
  vast::span<const vast::byte>::index_type position_;
};

} // namespace vast::msgpack
