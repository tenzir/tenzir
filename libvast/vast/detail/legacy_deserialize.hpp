//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "caf/deserializer.hpp"
#include "caf/detail/ieee_754.hpp"
#include "caf/detail/network_order.hpp"
#include "caf/detail/type_traits.hpp"
#include "caf/error.hpp"
#include "caf/expected.hpp"
#include "vast/detail/byte_swap.hpp"
#include "vast/error.hpp"

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

class legacy_deserializer { // an inspector for CAF inspect
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

private:
  template <class T>
    requires(caf::detail::is_inspectable<legacy_deserializer, T>::value)
  result_type apply(T& x) {
    return inspect(*this, x);
  }

  template <class T>
    requires(caf::meta::is_annotation<T>::value)
  result_type apply(T&) {
    return true;
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

  template <class T>
    requires(caf::detail::is_iterable<T>::value
             && !caf::detail::is_inspectable<legacy_deserializer, T>::value)
  result_type apply(T& xs) {
    return apply_sequence(xs);
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

  result_type apply_raw(size_t num_bytes, void* storage) {
    if (num_bytes > bytes_.size())
      return false;
    memcpy(storage, bytes_.data(), num_bytes);
    bytes_ = bytes_.subspan(num_bytes);
    return true;
  }

  std::span<const std::byte> bytes_ = {};
};

template <class T>
std::optional<T> legacy_deserialize(std::span<const std::byte> bytes) {
  legacy_deserializer f{bytes};
  T result{};
  if (f(result))
    return result;
  return std::nullopt;
}

} // namespace vast::detail
