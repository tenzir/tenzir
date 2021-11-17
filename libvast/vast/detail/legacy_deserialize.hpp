//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/as_bytes.hpp"
#include "vast/detail/byte_swap.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"

#include <caf/detail/ieee_754.hpp>
#include <caf/detail/network_order.hpp>
#include <caf/detail/type_traits.hpp>
#include <caf/detail/uri_impl.hpp>
#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <caf/meta/load_callback.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <optional>
#include <span>
#include <type_traits>
#include <vector>

namespace vast::detail {

/// An inspector for CAF inspect
class legacy_deserializer {
public:
  static constexpr bool reads_state = false;
  static constexpr bool writes_state = true;
  using result_type = bool;

  explicit legacy_deserializer(std::span<const std::byte> bytes)
    : bytes_(bytes) {
  }

  template <class... Ts>
  result_type operator()(Ts&&... xs) noexcept {
    return (apply(xs) && ...);
  }

  result_type apply_raw(size_t num_bytes, void* storage) {
    if (num_bytes > bytes_.size())
      return false;
    memcpy(storage, bytes_.data(), num_bytes);
    bytes_ = bytes_.subspan(num_bytes);
    return true;
  }

private:
  template <class T>
    requires(caf::detail::is_inspectable<legacy_deserializer, T>::value)
  result_type apply(T& x) {
    return inspect(*this, x);
  }

  template <class T>
    requires(caf::meta::is_annotation<T>::value
             && !caf::meta::is_load_callback<T>::value)
  result_type apply(T&) {
    // annotations are skipped
    return true;
  }

  template <class T>
    requires(caf::meta::is_load_callback<T>::value)
  result_type apply(T& x) {
    auto err = x.fun();
    return static_cast<bool>(err == caf::none);
  }

  template <class T>
    requires(std::is_enum<T>::value)
  result_type apply(T& x) {
    using underlying = typename std::underlying_type<T>::type;
    underlying tmp;
    if (!apply(tmp))
      return false;
    x = static_cast<T>(tmp);
    return true;
  }

  template <class F, class S>
    requires(
      caf::detail::is_serializable<typename std::remove_const<F>::type>::value&&
        caf::detail::is_serializable<S>::value)
  result_type apply(std::pair<F, S>& xs) {
    using t0 = typename std::remove_const<F>::type;
    if (!apply(const_cast<t0&>(xs.first)))
      return false;
    return apply(xs.second);
  }

  result_type apply(caf::uri& x) {
    auto impl = caf::make_counted<caf::detail::uri_impl>();
    if (!apply(*impl))
      return false;
    x = caf::uri{std::move(impl)};
    return true;
  }

  result_type apply(bool& x) {
    uint8_t tmp = 0;
    if (!apply(tmp))
      return false;
    x = tmp != 0;
    return true;
  }

  result_type apply(int8_t& x) {
    return apply_raw(sizeof(x), &x);
  }

  result_type apply(uint8_t& x) {
    return apply_raw(sizeof(x), &x);
  }

  result_type apply(int16_t& x) {
    return apply_int(x);
  }

  result_type apply(uint16_t& x) {
    return apply_int(x);
  }

  result_type apply(int32_t& x) {
    return apply_int(x);
  }

  result_type apply(uint32_t& x) {
    return apply_int(x);
  }

  result_type apply(int64_t& x) {
    return apply_int(x);
  }

  result_type apply(uint64_t& x) {
    return apply_int(x);
  }

  template <class T>
    requires(std::is_integral<T>::value && !std::is_same<bool, T>::value)
  result_type apply(T& x) {
    using type
      = caf::detail::select_integer_type_t<sizeof(T), std::is_signed<T>::value>;
    return apply(reinterpret_cast<type&>(x));
  }

  result_type apply(float& x) {
    return apply_float(x);
  }

  result_type apply(double& x) {
    return apply_float(x);
  }

  result_type apply(long double& x) {
    // The IEEE-754 conversion does not work for long double
    // => fall back to string serialization (even though it sucks).
    std::string tmp;
    if (!apply(tmp))
      return false;
    std::istringstream iss{std::move(tmp)};
    iss >> x;
    return true;
  }

  result_type apply(std::string& x) {
    size_t str_size = 0;
    if (!begin_sequence(str_size))
      return false;
    if (str_size > bytes_.size())
      return false;
    x.assign(reinterpret_cast<const char*>(bytes_.data()), str_size);
    bytes_ = bytes_.subspan(str_size);
    return true;
  }

  result_type apply(caf::none_t& x) {
    x = caf::none;
    return true;
  }

  template <class Rep, class Period>
    requires(std::is_integral<Rep>::value)
  result_type apply(std::chrono::duration<Rep, Period>& x) {
    using duration_type = std::chrono::duration<Rep, Period>;
    Rep tmp;
    if (!apply(tmp))
      return false;
    x = duration_type{tmp};
    return true;
  }

  template <class Rep, class Period>
    requires(std::is_floating_point<Rep>::value)
  result_type apply(std::chrono::duration<Rep, Period>& x) {
    using duration_type = std::chrono::duration<Rep, Period>;
    // always save/store floating point durations as doubles
    double tmp = NAN;
    if (!apply(tmp))
      return false;
    x = duration_type{tmp};
    return true;
  }

  template <class T>
    requires(caf::detail::is_iterable<T>::value
             && !caf::detail::is_inspectable<legacy_deserializer, T>::value)
  result_type apply(T& xs) {
    return apply_sequence(xs);
  }

  template <class Clock, class Duration>
  result_type apply(std::chrono::time_point<Clock, Duration>& t) {
    Duration dur{};
    if (!apply(dur))
      return false;
    t = std::chrono::time_point<Clock, Duration>{dur};
    return true;
  }

  template <class T>
    requires(!caf::detail::is_byte_sequence<T>::value)
  result_type apply_sequence(T& xs) {
    size_t size = 0;
    if (!begin_sequence(size))
      return false;
    xs.clear();
    auto it = std::inserter(xs, xs.end());
    for (size_t i = 0; i < size; ++i) {
      typename T::value_type tmp;
      if (!apply(tmp))
        return false;
      *it++ = std::move(tmp);
    }
    return true;
  }

  template <class T, std::size_t N>
  result_type apply(std::array<T, N>& x) {
    for (T& v : x)
      if (!apply(v))
        return false;
    return true;
  }

  result_type begin_sequence(size_t& list_size) {
    // Use varbyte encoding to compress sequence size on the wire.
    uint32_t x = 0;
    int n = 0;
    uint8_t low7 = 0;
    do {
      if (!apply(low7))
        return false;
      x |= static_cast<uint32_t>((low7 & 0x7F)) << (7 * n);
      ++n;
    } while ((low7 & 0x80) != 0);
    list_size = x;
    return true;
  }

  template <class T>
  result_type apply_float(T& x) {
    typename caf::detail::ieee_754_trait<T>::packed_type tmp;
    if (!apply_int(tmp))
      return false;
    x = caf::detail::unpack754(tmp);
    return true;
  }

  template <class T>
  result_type apply_int(T& x) {
    std::make_unsigned_t<T> tmp;
    if (!apply_raw(sizeof(x), &tmp))
      return false;
    x = static_cast<T>(to_host_order(tmp));
    return true;
  }

  std::span<const std::byte> bytes_ = {};
};

/// Deserializes a sequence of objects from a byte buffer.
/// @param buffer The vector of bytes to read from.
/// @param xs The object to deserialize.
/// @returns The status of the operation.
/// @relates detail::serialize
template <concepts::byte_container Buffer, class... Ts>
  requires(!std::is_rvalue_reference_v<Ts> && ...)
bool legacy_deserialize(const Buffer& buffer, Ts&... xs) {
  legacy_deserializer f{as_bytes(buffer)};
  return f(xs...);
}

} // namespace vast::detail
