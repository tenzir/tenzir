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

#include <cstdint>
#include <cstring>

#include "vast/detail/assert.hpp"
#include "vast/detail/byte.hpp"
#include "vast/detail/byte_swap.hpp"
#include "vast/detail/span.hpp"
#include "vast/detail/type_traits.hpp"

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
namespace vast::detail::msgpack {

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

/// Maps a MessagePack type to a C++ type.
template <format>
struct traits {
  using native_type = void;
};

/// @relates traits
template <format Format>
using native_type = typename traits<Format>::native_type;

template <>
struct traits<positive_fixint> {
  using native_type = uint8_t;
};

template <>
struct traits<negative_fixint> {
  using native_type = int8_t;
};

template <>
struct traits<int8> {
  using native_type = int8_t;
};

template <>
struct traits<int16> {
  using native_type = int16_t;
};

template <>
struct traits<int32> {
  using native_type = int32_t;
};

template <>
struct traits<int64> {
  using native_type = int64_t;
};

template <>
struct traits<uint8> {
  using native_type = uint8_t;
};

template <>
struct traits<uint16> {
  using native_type = uint16_t;
};

template <>
struct traits<uint32> {
  using native_type = uint32_t;
};

template <>
struct traits<uint64> {
  using native_type = uint64_t;
};

template <>
struct traits<float32> {
  using native_type = float;
};

template <>
struct traits<float64> {
  using native_type = double;
};

template <>
struct traits<str8> {
  using size_type = uint8_t;
};

template <>
struct traits<str16> {
  using size_type = uint16_t;
};

template <>
struct traits<str32> {
  using size_type = uint32_t;
};

template <>
struct traits<bin8> {
  using size_type = uint8_t;
};

template <>
struct traits<bin16> {
  using size_type = uint16_t;
};

template <>
struct traits<bin32> {
  using size_type = uint32_t;
};

template <>
struct traits<array16> {
  using size_type = uint16_t;
};

template <>
struct traits<array32> {
  using size_type = uint32_t;
};

template <>
struct traits<map16> {
  using size_type = uint16_t;
};

template <>
struct traits<map32> {
  using size_type = uint32_t;
};

/// @relates builder
struct bounds_check {};

/// @relates builder
struct no_bounds_check {};

// TODO: add policy for arithmetic range check as well?
/// Enables incremental construction of objects.
/// @tparam BoundsCheckPolicy A policy that controls whether the builder should
///         perform a bounds-check when writing into the underlying buffer.
template <class BoundsCheckPolicy = bounds_check>
class builder {
  struct empty {};

public:
  builder(span<byte> buffer) : buffer_{buffer}, i_{0} {
    // nop
  }

  /// A helper class to build containers incrementally. Zero or more calls of
  /// `add` must always follow a final call to `finish` to finalize the
  /// container construction.
  /// @tparam Format The format to build the helper for.
  template <format Format>
  class container_proxy {
    friend builder;

    // The number of bytes to skip initially and patch in later. One byte for
    // the format and the remaining bytes for the container size.
    size_t constexpr skip() {
      if constexpr (Format == fixarray || Format == fixmap)
        return 1;
      else
        return 1 + sizeof(typename traits<Format>::size_type);
    }

  public:
    /// Adds a value to an array.
    /// @tparam ElementFormat The format of the value to add.
    /// @param x The value to add.
    /// @returns The number of bytes written.
    template <format ElementFormat, class T = empty>
    size_t add(T&& x = {}) {
      static_assert(Format == fixarray || Format == array16
                    || Format == array32);
      auto bytes = builder_.add<ElementFormat>(std::forward<T>(x));
      if (bytes == 0)
        return 0;
      ++n_;
      return bytes;
    }

    /// Adds a key-value pair to the map.
    /// @tparam KeyFormat The format of the key to add.
    /// @tparam ValueFormat The format of the value to add.
    /// @param key The key of the key-value pair.
    /// @param value The value of the key-value pair.
    /// @returns The number of bytes written.
    template <
      format KeyFormat,
      format ValueFormat,
      class Key = empty,
      class Value = empty
    >
    size_t add(Key&& key = {}, Value&& value = {}) {
      static_assert(Format == fixmap || Format == map16 || Format == map32);
      auto key_bytes = builder_.add<KeyFormat>(std::forward<Key>(key));
      if (key_bytes == 0)
        return 0;
      auto value_bytes = builder_.add<ValueFormat>(std::forward<Value>(value));
      if (value_bytes == 0)
        return 0;
      ++n_;
      return key_bytes + value_bytes;
    }

    /// Finalizes the addition of values.
    /// @returns `true` on success.
    bool finish() {
      auto i = builder_.i_ - i_;
      if constexpr (Format == fixarray || Format == fixmap) {
        if (n_ >= 15)
          return false;
        *builder_.get(-i) = narrow_cast<uint8_t>(n_) & Format;
      } else if constexpr (Format == array16 || Format == map16) {
        *builder_.get(-i) = array16;
        auto bytes = to_network_order(narrow_cast<uint16_t>(i - skip()));
        *builder_.get<uint16_t>(-i + 1) = bytes;
      } else if constexpr (Format == array32 || Format == map32) {
        *builder_.get(-i) = array32;
        auto bytes = to_network_order(narrow_cast<uint32_t>(i - skip()));
        *builder_.get<uint32_t>(-i + 1) = bytes;
      } else {
        VAST_ASSERT(!"not a container format");
      }
      return true;
    }

  private:
    container_proxy(builder& b) : builder_{b}, i_{builder_.i_}, n_{0} {
      builder_.i_ += skip();
    }

    builder& builder_;
    span<byte>::index_type i_; // saved offset where container started
    span<byte>::size_type n_;  // container size
  };

  /// Creates a proxy builder to build container values.
  /// @tparam Format The format to create a proxy builder for.
  template <format Format>
  container_proxy<Format> build() {
    return container_proxy<Format>(*this);
  }

  /// Adds a value of a statically chosen format.
  /// @tparam fmt The format of *x*
  /// @param x The value to add.
  /// @returns The number of bytes written.
  template <format Format, class T = empty>
  size_t add(const T& x = {}) {
    if constexpr (Format == nil || Format == false_ || Format == true_)
      return add_byte(Format);
    if constexpr (Format == positive_fixint)
      return x >= 0 && x < 128 && add_byte(x & Format);
    if constexpr (Format == negative_fixint)
      return x >= -32 && x < 0 && add_byte(x & Format);
    if constexpr (Format == uint8 || Format == uint16 || Format == uint32
                  || Format == uint64 || Format == int8 || Format == int16
                  || Format == int32 || Format == int64)
      return add_int_family<Format>(x);
    if constexpr (Format == float32 || Format == float64)
      return add_float_family<Format>(x);
    if constexpr (Format == fixstr || Format == str8 || Format == str16
                  || Format == str32)
      return add_str_family<Format>(x);
    if constexpr (Format == bin8 || Format == bin16 || Format == bin32)
      return add_binary<Format>(x);
    if constexpr (Format == fixext1 || Format == fixext2 || Format == fixext4
                  || Format == fixext8 || Format == fixext16)
      return 0; // TODO
    if constexpr (Format == ext8 || Format == ext16 || Format == ext32)
      return 0; // TODO
    VAST_ASSERT(!"unsupported format");
    return 0;
  }

  /// Adds a value of a dynamically chosen format.
  /// @param fmt The format of *x*
  /// @param x The value to add.
  /// @returns The number of bytes written.
  template <class T = empty>
  size_t add(format fmt, const T& x = {}) {
    switch (fmt) {
      case nil:
        return add<nil>();
      case false_:
        return add<false_>();
      case true_:
        return add<true_>();
      case positive_fixint:
        if constexpr (std::is_integral_v<T>)
          return add<positive_fixint>(x);
        return 0;
      case negative_fixint:
        if constexpr (std::is_integral_v<T>)
          return add<negative_fixint>(x);
        return 0;
      case uint8:
        if constexpr (std::is_integral_v<T>)
          return add<uint8>(x);
        return 0;
      case uint16:
        if constexpr (std::is_integral_v<T>)
          return add<uint16>(x);
        return 0;
      case uint32:
        if constexpr (std::is_integral_v<T>)
          return add<uint32>(x);
        return 0;
      case uint64:
        if constexpr (std::is_integral_v<T>)
          return add<uint64>(x);
        return 0;
      case int8:
        if constexpr (std::is_integral_v<T>)
          return add<int8>(x);
        return 0;
      case int16:
        if constexpr (std::is_integral_v<T>)
          return add<int16>(x);
        return 0;
      case int32:
        if constexpr (std::is_integral_v<T>)
          return add<int32>(x);
        return 0;
      case int64:
        if constexpr (std::is_integral_v<T>)
          return add<int64>(x);
        return 0;
      case float32:
        if constexpr (std::is_floating_point_v<T>)
          return add<float32>(x);
        return 0;
      case float64:
        if constexpr (std::is_floating_point_v<T>)
          return add<float64>(x);
        return 0;
      case fixstr:
        if constexpr (std::is_convertible_v<T, std::string_view>)
          return add<fixstr>(x);
      case str8:
        if constexpr (std::is_convertible_v<T, std::string_view>)
          return add<str8>(x);
      case str16:
        if constexpr (std::is_convertible_v<T, std::string_view>)
          return add<str16>(x);
      case str32:
        if constexpr (std::is_convertible_v<T, std::string_view>)
          return add<str32>(x);
      case bin8:
        if constexpr (std::is_convertible_v<T, span<const byte>>)
          return add<bin8>(x);
      case bin16:
        if constexpr (std::is_convertible_v<T, span<const byte>>)
          return add<bin16>(x);
      case bin32:
        if constexpr (std::is_convertible_v<T, span<const byte>>)
          return add<bin32>(x);
      case fixarray:
      case array16:
      case array32:
      case fixmap:
      case map16:
      case map32:
        // TODO: To add containers, the user must go through a proxy. Is
        // returning 0 the right way to signal this to the user?
        return 0;
      case fixext1:
      case fixext2:
      case fixext4:
      case fixext8:
      case fixext16:
      case ext8:
      case ext16:
      case ext32:
        return 0; // TODO
    }
  }

  /// @returns The number of bytes the builder has written into its buffer.
  size_t size() const {
    return i_;
  }

private:
  template <class T>
  size_t write_byte(T x) {
    VAST_ASSERT(i_ < buffer_.size());
    *get(0) = static_cast<uint8_t>(x);
    ++i_;
    return 1;
  }

  template <class T>
  size_t write_count(T x) {
    static_assert(std::is_integral_v<T> && std::is_unsigned_v<T>);
    VAST_ASSERT(i_ + narrow_cast<long>(sizeof(T)) < buffer_.size());
    *get<T>(0) = to_network_order(x);
    i_ += sizeof(T);
    return sizeof(T);
  }

  size_t write_data(const void* x, size_t n) {
    VAST_ASSERT(i_ + narrow_cast<long>(n) < buffer_.size());
    std::memcpy(get(0), x, n);
    i_ += n;
    return n;
  }

  template <class T>
  size_t write_data(span<const T> xs) {
    return write_data(xs.data(), xs.size() * sizeof(T));
  }

  size_t add_byte(uint8_t fmt) {
    return within_bounds(1) ? write_byte(fmt) : 0;
  }

  template <format Format, class T>
  size_t add_int_family(T x) {
    static_assert(std::is_integral_v<T>);
    auto y = narrow_cast<native_type<Format>>(x);
    auto n = 1 + sizeof(y);
    if (!within_bounds(n))
      return 0;
    auto u = static_cast<std::make_unsigned_t<native_type<Format>>>(y);
    auto z = to_network_order(u);
    return write_byte(Format) + write_data(&z, sizeof(z));
  }

  template <format Format, class T>
  size_t add_float_family(T x) {
    static_assert(std::is_floating_point_v<T>);
    auto n = size_t{1};
    if constexpr (Format == float32)
      n += 4;
    if constexpr (Format == float64)
      n += 8;
    if (!within_bounds(n))
      return 0;
    // TODO: is it okay to write the value as is?
    return write_byte(Format) + write_data(&x, sizeof(x));
  }

  template <format Format>
  size_t add_str_family(std::string_view x) {
    static_assert(Format == fixstr || Format == str8 || Format == str16 ||
                  Format == str32);
    auto n = size_t{0};
    if constexpr (Format == fixstr) {
      if (x.size() >= 32 || !within_bounds(1 + x.size()))
        return 0;
      n += write_byte(fixstr & x.size());
    } else if constexpr (Format == str8 || Format == str16 || Format == str32) {
      using size_type = typename traits<Format>::size_type;
      auto max = static_cast<size_t>(std::numeric_limits<size_type>::max());
      if (x.size() >= max || !within_bounds(1 + sizeof(size_type) + x.size()))
        return 0;
      n += write_byte(Format);
      n += write_count(narrow_cast<size_type>(x.size()));
    }
    if (n == 0 || x.empty())
      return n;
    auto ptr = reinterpret_cast<const byte*>(x.data());
    auto xs = span<const byte>(ptr, x.size());
    return n + write_data(xs);
  }

  template <format Format>
  size_t add_binary(span<const byte> xs) {
    using size_type = typename traits<Format>::size_type;
    auto max = static_cast<size_t>(std::numeric_limits<size_type>::max());
    if (xs.size() >= max || !within_bounds(1 + sizeof(size_type) + xs.size()))
      return 0;
    auto n = narrow_cast<size_type>(xs.size());
    return write_byte(Format) + write_count(n) + write_data(xs);
  }

  bool within_bounds(int bytes_needed) {
    if constexpr (std::is_same_v<BoundsCheckPolicy, bounds_check>)
      return i_ + bytes_needed <= buffer_.size();
    else if constexpr (std::is_same_v<BoundsCheckPolicy, no_bounds_check>)
      return true;
    else
      static_assert(always_false_v<BoundsCheckPolicy>, "invalid bounds policy");
  }

  template <class T = uint8_t>
  T* get(span<byte>::index_type offset = 0) {
    VAST_ASSERT(i_ + offset < buffer_.size());
    return reinterpret_cast<T*>(&buffer_[i_ + offset]);
  }

  span<byte> buffer_;
  span<byte>::index_type i_;
};

/// Encodes a value into a sequence of bytes.
/// @param x The value to encode.
/// @param sink The buffer to write to.
/// @returns The number of bytes *x* occupies in *buf*. (0 on failure)
template <format Format, class T>
size_t encode(const T& x, span<byte> sink) {
  return builder{sink}.add(x);
}

/// Decodes an encoded sequence of bytes.
/// @param sink The buffer to read from.
/// @return The next element.
format next(span<const byte> source) {
  VAST_ASSERT(source.size() > 0);
  return *reinterpret_cast<const format*>(source.data());
}

} // namespace vast::detail::msgpack
