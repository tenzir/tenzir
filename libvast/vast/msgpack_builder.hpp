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

#include "vast/msgpack.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <type_traits>
#include <vector>

#include <vast/byte.hpp>
#include <vast/detail/byte_swap.hpp>
#include <vast/detail/narrow.hpp>
#include <vast/detail/type_traits.hpp>
#include <vast/span.hpp>

namespace vast::msgpack {

// -- policies ----------------------------------------------------------------

/// A type tag for the builder input validation policy. With this tag, the
/// builder makes sure that the input conforms with the given format. For
/// example, the builder would ensure that the input for a `fixstr` does not
/// exceed 31 bytes.
/// @relates builder no_input_validation
struct input_validation {};

/// A type tag for the builder input validation policy. With this tag, the
/// builder does not check whether input conforms with the given format.
/// @relates builder input_validation
struct no_input_validation {};

// -- builder -----------------------------------------------------------------

/// Enables incremental construction of objects.
/// @tparam InputValidationPolicy A policy that controls whether the input
///         should be validated.
template <class InputValidationPolicy = input_validation>
class builder {
  struct empty {};

public:
  using value_type = vast::byte;

  /// A helper class to build formats incrementally. Zero or more calls of
  /// `add` must always follow a final call to `finish` to finalize the
  /// format construction.
  /// @tparam Format The format to build the helper for.
  template <format Format>
  class proxy {
    friend builder;
    static_assert(Format == bin8 || Format == bin16 || Format == bin32
                  || Format == fixarray || Format == array16
                  || Format == array32 || Format == fixmap || Format == map16
                  || Format == map32 || Format == ext8 || Format == ext16
                  || Format == ext32);

  public:
    /// Adds an object to an array.
    /// @tparam ElementFormat The format of the object to add.
    /// @param x The object to add.
    /// @returns The number of bytes written or 0 on failure.
    template <format ElementFormat, class T = empty, class U = empty>
    size_t add(const T& x = {}, const U& y = {}) {
      if constexpr (std::is_same_v<InputValidationPolicy, input_validation>)
        if (size_ >= capacity<Format>())
          return 0;
      auto result = builder_.add<ElementFormat>(x, y);
      if (result > 0) {
        if constexpr (Format == fixarray || Format == fixmap
                      || Format == array16 || Format == array32
                      || Format == map16 || Format == map32)
          ++size_;
        else
          size_ += result;
      }
      return result;
    }

    /// Finalizes the addition of values to a container.
    /// @returns The number of total bytes the proxy has written or 0 on
    ///          failure. When the result is 0, the proxy is in the state as if
    ///          after a call to reset().
    size_t finish() {
      using namespace vast::detail;
      VAST_ASSERT(size_ <= capacity<Format>());
      if constexpr (Format == fixmap || Format == map16 || Format == map32) {
        if constexpr (std::is_same_v<InputValidationPolicy, input_validation>)
          if (size_ % 2 != 0) { // Maps have an even number of elements.
            reset();
            return 0;
          }
        size_ /= 2;
      }
      // Always write the format first.
      auto ptr = builder_.buffer_.data() + offset_;
      *ptr = static_cast<value_type>(Format);
      // Then write the number of elements or size in bytes.
      if constexpr (Format == fixarray || Format == fixmap) {
        *ptr &= value_type{0b1111'0000};
        *ptr |= narrow_cast<value_type>(size_);
      } else {
        auto size = make_size<Format>(size_);
        auto size_ptr = reinterpret_cast<decltype(&size)>(ptr + 1);
        *size_ptr = to_network_order(size);
      }
      return builder_.buffer_.size() - offset_;
    }

    /// Finalizes the addition of data to an extension format.
    /// @param type The value of the extension type integer.
    /// @returns The number of total bytes the proxy has written or 0 on
    ///          failure.
    size_t finish(extension_type type) {
      static_assert(is_fixext(Format) || is_ext(Format));
      auto num_bytes = finish();
      if (num_bytes == 0)
        return 0;
      auto data = builder_.buffer_.data();
      auto offset = offset_ + header_size<Format>() - 1;
      auto ptr = reinterpret_cast<extension_type*>(data + offset);
      *ptr = type;
      return num_bytes;
    }

    /// Creates a nested proxy builder to build container values.
    /// @tparam NestedFormat The format to create a proxy builder for.
    template <format NestedFormat>
    proxy<NestedFormat> build() {
      return builder_.build<NestedFormat>();
    }

    /// Resets the proxy to its state immediately after construction.
    void reset() {
      size_ = 0;
      // Skip directly to data offset. We patch in the header data later in
      // finish().
      builder_.buffer_.resize(offset_ + header_size<Format>());
    }

  private:
    explicit proxy(builder& b)
      : builder_{b}, offset_{builder_.buffer_.size()}, size_{0} {
      reset();
    }

    builder& builder_;
    size_t offset_; // where we started in the builder buffer
    size_t size_;   // number of elements or size in bytes
  };

  /// Constructs a builder from a byte span.
  /// @param buffer The buffer to write into.
  explicit builder(std::vector<value_type>& buffer)
    : buffer_{buffer}, offset_{buffer_.size()} {
    // nop
  }

  /// Creates a proxy builder to build container values.
  /// @tparam Format The format to create a proxy builder for.
  template <format Format>
  proxy<Format> build() {
    return proxy<Format>(*this);
  }

  /// Adds an object of a statically chosen format.
  /// @tparam fmt The format of *x*
  /// @param x The object to add.
  /// @returns The number of bytes written or 0 on failure
  template <format Format, class T = empty, class U = empty>
  size_t add(const T& x = {}, const U& y = {}) {
    if (!validate<Format>(x, y))
      return false;
    if constexpr (Format == nil || Format == false_ || Format == true_)
      return add_format(Format);
    if constexpr (Format == positive_fixint || Format == negative_fixint)
      return add_format(x & Format);
    if constexpr (Format == uint8 || Format == uint16 || Format == uint32
                  || Format == uint64 || Format == int8 || Format == int16
                  || Format == int32 || Format == int64)
      return add_int<Format>(x);
    if constexpr (Format == float32 || Format == float64)
      return add_float<Format>(x);
    if constexpr (Format == fixstr)
      return add_fixstr(x);
    if constexpr (Format == str8 || Format == str16 || Format == str32)
      return add_str<Format>(x);
    if constexpr (Format == bin8 || Format == bin16 || Format == bin32)
      return add_binary<Format>(x);
    if constexpr (Format == fixext1 || Format == fixext2 || Format == fixext4
                  || Format == fixext8 || Format == fixext16)
      return add_fix_ext<Format>(x, y);
    if constexpr (Format == ext8 || Format == ext16 || Format == ext32)
      return add_ext<Format>(x, y);
    VAST_ASSERT(!"unsupported format");
    return 0;
  }

  /// Adds a timestmap. Internally, the builder creates an extension object
  /// with the type set to -1.
  /// @param x The number of seconds since the UNIX epoch.
  /// @param ns The number of nanoseconds since the UNIX epoch.
  /// @returns The number of bytes written or 0 on failure.
  size_t add(std::chrono::seconds secs, std::chrono::nanoseconds ns) {
    using namespace std::chrono;
    using namespace vast;
    using namespace vast::detail;
    // The dispatching logic stems directly from the spec, as illustrated in
    // the section "Timestamp extension type".
    if ((secs.count() >> 34) == 0) {
      uint64_t data64 = (ns.count() << 34) | secs.count();
      if ((data64 & 0xffffffff00000000ull) == 0) {
        // Use timestamp32 if we don't have nanoseconds.
        auto data32 = to_network_order(narrow_cast<uint32_t>(data64));
        return add<fixext4>(-1, as_bytes(span{&data32, 1}));
      }
      // Use timestamp64 if we have nanoseconds.
      data64 = to_network_order(data64);
      return add<fixext8>(-1, as_bytes(span{&data64, 1}));
    }
    // Use timestamp96 if seconds are larger than 2^34.
    std::array<uint32_t, 3> data96;
    auto ptr_ns = data96.data();
    auto ptr_secs = reinterpret_cast<uint64_t*>(ptr_ns + 1);
    *ptr_ns = to_network_order(narrow_cast<uint32_t>(ns.count()));
    *ptr_secs = to_network_order(narrow_cast<uint64_t>(secs.count()));
    return add<ext8>(-1, as_bytes(span{data96.data(), data96.size()}));
  }

  /// Adds a timestmap.
  /// @param x The time.
  /// @returns The number of bytes written or 0 on failure.
  size_t add(vast::time x) {
    using namespace std::chrono;
    auto since_epoch = x.time_since_epoch();
    auto secs = duration_cast<seconds>(since_epoch);
    auto ns = duration_cast<nanoseconds>(since_epoch) - secs;
    return add(secs, ns);
  }

  /// Resets the builder (and buffer) to the state immediately after
  /// construction.
  void reset() {
    return buffer_.resize(offset_);
  }

private:
  // -- policy functions -------------------------------------------------------

  template <format Format, class T, class U>
  bool validate(const T& x, const U& y) {
    using namespace vast::detail;
    using std::size; // enable ADL
    if constexpr (std::is_same_v<InputValidationPolicy, input_validation>) {
      if constexpr (std::is_same_v<T, empty>) {
        static_assert(std::is_same_v<U, empty>);
        return true;
      } else if constexpr (Format == positive_fixint) {
        return x >= 0 && x < 128;
      } else if constexpr (Format == negative_fixint) {
        return x >= -32 && x < 0;
      } else if constexpr (is_fixstr(Format) || is_str(Format)
                           || is_bin(Format)) {
        return size(x) <= capacity<Format>();
      } else if constexpr (is_fixext(Format) || is_ext(Format)) {
        return narrow_cast<size_t>(size(y)) <= capacity<Format>();
      }
      return true;
    } else if constexpr (std::is_same_v<InputValidationPolicy,
                                        no_input_validation>) {
      return true;
    } else {
      static_assert(vast::detail::always_false_v<InputValidationPolicy>,
                    "invalid input validation policy");
    }
  }

  // -- low-level buffer manipulation ------------------------------------------

  size_t write_byte(uint8_t x) {
    buffer_.emplace_back(value_type{x});
    return 1;
  }

  size_t write_data(const void* x, size_t n) {
    auto ptr = reinterpret_cast<const value_type*>(x);
    buffer_.insert(buffer_.end(), ptr, ptr + n);
    return n;
  }

  template <class T>
  size_t write_data(vast::span<const T> xs) {
    return write_data(xs.data(), xs.size() * sizeof(T));
  }

  template <class T>
  size_t write_count(T x) {
    static_assert(std::is_integral_v<T> && std::is_unsigned_v<T>);
    auto y = vast::detail::to_network_order(x);
    return write_data(&y, sizeof(y));
  }

  // -- format-specific utilities ----------------------------------------------

  size_t add_format(uint8_t x) {
    return write_byte(x);
  }

  template <format Format, class T>
  size_t add_int(T x) {
    static_assert(std::is_integral_v<T>);
    auto y = native_cast<Format>(x);
    auto u = static_cast<std::make_unsigned_t<decltype(y)>>(y);
    auto z = vast::detail::to_network_order(u);
    return write_byte(Format) + write_data(&z, sizeof(z));
  }

  template <format Format, class T>
  size_t add_float(T x) {
    static_assert(std::is_floating_point_v<T>);
    // TODO: is it okay to write the floating-point value as is?
    return write_byte(Format) + write_data(&x, sizeof(x));
  }

  size_t add_fixstr(std::string_view x) {
    using namespace vast::detail;
    auto fmt = uint8_t{0b1010'0000} | narrow_cast<uint8_t>(x.size());
    return write_byte(fmt) + write_data(x.data(), x.size());
  }

  template <format Format>
  size_t add_str(std::string_view x) {
    auto xs = as_bytes(vast::span{x.data(), x.size()});
    if (!xs.empty())
      return add_binary<Format>(xs);
    return write_byte(Format) + write_count(make_size<Format>(0));
  }

  template <format Format>
  size_t add_binary(vast::span<const vast::byte> xs) {
    using namespace vast::detail;
    auto n = make_size<Format>(xs.size());
    return write_byte(Format) + write_count(n) + write_data(xs);
  }

  template <format Format>
  size_t add_fix_ext(extension_type type, vast::span<const vast::byte> xs) {
    return write_byte(Format) + write_byte(type)
           + write_data(xs.data(), xs.size());
  }

  template <format Format>
  size_t add_ext(extension_type type, vast::span<const vast::byte> xs) {
    auto n = make_size<Format>(xs.size());
    return write_byte(Format) + write_count(n) + write_byte(type)
           + write_data(xs.data(), xs.size());
  }

  std::vector<value_type>& buffer_;
  size_t offset_;
};

// -- helper functions to encode common types ---------------------------------

/// Encodes a value into a builder.
/// @param builder The builder to add *x* to.
/// @param x The value to encode in *builder*.
/// @returns The number of bytes written or 0 on failure.
template <class Builder, class T, class = std::enable_if_t<std::is_empty_v<T>>>
size_t put(Builder& builder, T) {
  return builder.template add<nil>();
}

template <class Builder>
size_t put(Builder& builder, bool x) {
  return x ? builder.template add<true_>() : builder.template add<false_>();
}

// -- int ---------------------------------------------------------------------

template <class Builder, class T>
auto put(Builder& builder, T x)
  -> std::enable_if_t<std::is_integral_v<T> && std::is_signed_v<T>, size_t> {
  using std::numeric_limits;
  if constexpr (sizeof(T) == 8)
    if (x < numeric_limits<int32_t>::min())
      return builder.template add<int64>(x);
  if constexpr (sizeof(T) >= 4)
    if (x < numeric_limits<int16_t>::min())
      return builder.template add<int32>(x);
  if constexpr (sizeof(T) >= 2)
    if (x < numeric_limits<int8_t>::min())
      return builder.template add<int16>(x);
  if (x < -32)
    return builder.template add<int8>(x);
  if (x < 0)
    return builder.template add<negative_fixint>(x);
  if (x < 32)
    return builder.template add<positive_fixint>(x);
  if constexpr (sizeof(T) == 1)
    return builder.template add<int8>(x);
  if constexpr (sizeof(T) >= 2)
    if (x <= numeric_limits<int8_t>::max())
      return builder.template add<int8>(x);
  if constexpr (sizeof(T) == 2)
    return builder.template add<int16>(x);
  if constexpr (sizeof(T) >= 4)
    if (x <= numeric_limits<int16_t>::max())
      return builder.template add<int16>(x);
  if constexpr (sizeof(T) == 4)
    return builder.template add<int32>(x);
  if constexpr (sizeof(T) == 8) {
    if (x <= numeric_limits<int32_t>::max())
      return builder.template add<int32>(x);
    return builder.template add<int64>(x);
  }
}

template <class Builder, class T>
auto put(Builder& builder, T x)
  -> std::enable_if_t<std::is_integral_v<T> && std::is_unsigned_v<T>, size_t> {
  using std::numeric_limits;
  if (x < 32)
    return builder.template add<positive_fixint>(x);
  if constexpr (sizeof(T) == 1)
    return builder.template add<uint8>(x);
  if constexpr (sizeof(T) >= 2)
    if (x <= numeric_limits<uint8_t>::max())
      return builder.template add<uint8>(x);
  if constexpr (sizeof(T) == 2)
    return builder.template add<uint16>(x);
  if constexpr (sizeof(T) >= 4)
    if (x <= numeric_limits<uint16_t>::max())
      return builder.template add<uint16>(x);
  if constexpr (sizeof(T) == 4)
    return builder.template add<uint32>(x);
  if constexpr (sizeof(T) == 8) {
    if (x <= numeric_limits<uint32_t>::max())
      return builder.template add<uint32>(x);
    return builder.template add<uint64>(x);
  }
}

// -- float -------------------------------------------------------------------

template <class Builder>
size_t put(Builder& builder, float x) {
  return builder.template add<float32>(x);
}

template <class Builder>
size_t put(Builder& builder, double x) {
  return builder.template add<float64>(x);
}

// -- string ------------------------------------------------------------------

template <class Builder>
size_t put(Builder& builder, std::string_view x) {
  if (x.size() <= capacity<fixstr>())
    return builder.template add<fixstr>(x);
  if (x.size() <= capacity<str16>())
    return builder.template add<str16>(x);
  if (x.size() <= capacity<str32>())
    return builder.template add<str32>(x);
  return 0;
}

// -- bin ---------------------------------------------------------------------

template <class Builder>
size_t put(Builder& builder, vast::span<const vast::byte> xs) {
  auto size = static_cast<size_t>(xs.size());
  if (size <= capacity<bin8>())
    return builder.template add<bin8>(xs);
  if (size <= capacity<bin16>())
    return builder.template add<bin16>(xs);
  if (size <= capacity<bin32>())
    return builder.template add<bin32>(xs);
  return 0;
}

// -- array -------------------------------------------------------------------

template <class Builder, class T, class F>
size_t put_array(Builder& builder, const T& xs, F f) {
  auto add = [&](auto&& proxy) -> size_t {
    for (auto&& x : xs)
      if (f(proxy, x) == 0) {
        builder.reset();
        return 0;
      }
    return proxy.finish();
  };
  auto size = vast::detail::narrow_cast<size_t>(xs.size());
  if (size <= capacity<fixarray>())
    return add(builder.template build<fixarray>());
  if (size <= capacity<array16>())
    return add(builder.template build<array16>());
  if (size <= capacity<array32>())
    return add(builder.template build<array32>());
  return 0;
}

template <class Builder, class T>
size_t put_array(Builder& builder, const T& xs) {
  auto f = [](auto& proxy, auto&& x) { return put(proxy, x); };
  return put_array(builder, xs, f);
}

template <class Builder, class T>
size_t put(Builder& builder, std::vector<T>& xs) {
  return put_array(builder, xs);
}

// -- map ---------------------------------------------------------------------

template <class Builder, class T, class F>
size_t put_map(Builder& builder, const T& xs, F f) {
  auto add = [&](auto proxy) -> size_t {
    for (auto&& [k, v] : xs) {
      if (f(proxy, k) == 0 || f(proxy, v) == 0) {
        builder.reset();
        return 0;
      }
    }
    return proxy.finish();
  };
  auto size = vast::detail::narrow_cast<size_t>(xs.size());
  if (size <= capacity<fixmap>())
    return add(builder.template build<fixmap>());
  if (size <= capacity<map16>())
    return add(builder.template build<map16>());
  if (size <= capacity<map32>())
    return add(builder.template build<map32>());
  return 0;
}

template <class Builder, class T>
size_t put_map(Builder& builder, const T& xs) {
  auto f = [](auto& proxy, auto&& x) { return put(proxy, x); };
  return put_map(builder, xs, f);
}

template <class Builder, class K, class V>
size_t put(Builder& builder, const std::map<K, V>& xs) {
  return put_map(builder, xs);
}

// -- variadic ----------------------------------------------------------------

template <class Builder, class T, class... Ts>
size_t put(Builder& builder, const T& x, const Ts&... xs) {
  auto n = put(builder, x);
  if (n == 0) {
    builder.reset();
    return 0;
  }
  return n + put(builder, xs...);
}

} // namespace vast::msgpack
