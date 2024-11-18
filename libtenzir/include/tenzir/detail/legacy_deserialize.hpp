//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/as_bytes.hpp"
#include "tenzir/detail/byteswap.hpp"
#include "tenzir/detail/inspection_common.hpp"
#include "tenzir/error.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/variant_traits.hpp"

#include <caf/config_value.hpp>
#include <caf/detail/ieee_754.hpp>
#include <caf/detail/network_order.hpp>
#include <caf/detail/select_integer_type.hpp>
#include <caf/detail/type_traits.hpp>
#include <caf/expected.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <optional>
#include <span>
#include <type_traits>
#include <vector>
namespace tenzir::detail {

/// An inspector for CAF inspect
class legacy_deserializer {
public:
  static constexpr bool is_loading = true;
  using result_type = bool;

  explicit legacy_deserializer(std::span<const std::byte> bytes)
    : bytes_(bytes) {
  }

  template <class... Ts>
  result_type operator()(Ts&&... xs) noexcept {
    return (apply(xs) && ...);
  }

  template <class T>
  auto object(const T&) {
    return detail::inspection_object(*this);
  }

  template <class T>
  auto field(std::string_view, T& value) {
    return detail::inspection_field{value};
  }

  inline result_type apply_raw(size_t num_bytes, void* storage) {
    if (num_bytes > bytes_.size())
      return false;
    memcpy(storage, bytes_.data(), num_bytes);
    bytes_ = bytes_.subspan(num_bytes);
    return true;
  }

  template <class T>
    requires requires(legacy_deserializer& f, T& x) {
               requires !std::is_enum_v<T>;
               {
                 inspect(f, x)
                 } -> std::same_as<legacy_deserializer::result_type>;
             }
  result_type apply(T& x) {
    return inspect(*this, x);
  }

  template <class T>
    requires(std::is_enum_v<T>)
  result_type apply(T& x) {
    using underlying = typename std::underlying_type_t<T>;
    underlying tmp;
    if (!apply(tmp))
      return false;
    x = static_cast<T>(tmp);
    return true;
  }

  template <class F, class S>
  result_type apply(std::pair<F, S>& xs) {
    using t0 = typename std::remove_const_t<F>;
    if (!apply(const_cast<t0&>(xs.first)))
      return false;
    return apply(xs.second);
  }

  result_type apply(caf::uri& x) {
    auto impl = caf::make_counted<caf::uri::impl_type>();
    if (!apply(*impl))
      return false;
    x = caf::uri{std::move(impl)};
    return true;
  }

  inline result_type apply(bool& x) {
    uint8_t tmp = 0;
    if (!apply(tmp))
      return false;
    x = tmp != 0;
    return true;
  }

  inline result_type apply(int8_t& x) {
    return apply_raw(sizeof(x), &x);
  }

  inline result_type apply(uint8_t& x) {
    return apply_raw(sizeof(x), &x);
  }

  inline result_type apply(int16_t& x) {
    return apply_int(x);
  }

  inline result_type apply(uint16_t& x) {
    return apply_int(x);
  }

  inline result_type apply(int32_t& x) {
    return apply_int(x);
  }

  inline result_type apply(uint32_t& x) {
    return apply_int(x);
  }

  inline result_type apply(int64_t& x) {
    return apply_int(x);
  }

  inline result_type apply(uint64_t& x) {
    return apply_int(x);
  }

  template <class T>
    requires(std::is_integral_v<T>)
  result_type apply(T& x) {
    using type
      = caf::detail::select_integer_type_t<sizeof(T), std::is_signed_v<T>>;
    return apply(reinterpret_cast<type&>(x));
  }

  inline result_type apply(float& x) {
    return apply_float(x);
  }

  inline result_type apply(double& x) {
    return apply_float(x);
  }

  inline result_type apply(long double& x) {
    // The IEEE-754 conversion does not work for long double
    // => fall back to string serialization (even though it sucks).
    std::string tmp;
    if (!apply(tmp))
      return false;
    std::istringstream iss{std::move(tmp)};
    iss >> x;
    return true;
  }

  inline result_type apply(std::string& x) {
    size_t str_size = 0;
    if (!begin_sequence(str_size))
      return false;
    if (str_size > bytes_.size())
      return false;
    x.assign(reinterpret_cast<const char*>(bytes_.data()), str_size);
    bytes_ = bytes_.subspan(str_size);
    return true;
  }

  inline result_type apply(caf::none_t& x) {
    x = caf::none;
    return true;
  }

  template <class Rep, class Period>
    requires(std::is_integral_v<Rep>)
  result_type apply(std::chrono::duration<Rep, Period>& x) {
    using duration_type = std::chrono::duration<Rep, Period>;
    Rep tmp;
    if (!apply(tmp))
      return false;
    x = duration_type{tmp};
    return true;
  }

  template <class Rep, class Period>
    requires(std::is_floating_point_v<Rep>)
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
    requires(
      caf::detail::is_iterable<T>::value
      && !caf::detail::has_inspect_overload<legacy_deserializer, T&>::value)
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

  template <class T, std::size_t N>
  result_type apply(std::array<T, N>& x) {
    for (T& v : x)
      if (!apply(v))
        return false;
    return true;
  }

  template <class... Ts>
  result_type apply(caf::variant<Ts...>& x) {
    uint8_t type_tag = 0;
    if (!apply(type_tag))
      return false;
    return apply(type_tag, x);
  }

  template <class... Ts>
  result_type apply(uint8_t type_tag, caf::variant<Ts...>& x) {
    using namespace caf;
    using caf::detail::tl_at;
    auto& f = *this;
    switch (type_tag) {
      default:
        CAF_RAISE_ERROR("invalid type found");
#define CAF_VARIANT_ASSIGN_CASE_IMPL(n)                                        \
  case n: {                                                                    \
    using tmp_t =                                                              \
      typename caf::detail::tl_at<caf::detail::type_list<Ts...>,               \
                                  ((n) < sizeof...(Ts) ? (n) : 0)>::type;      \
    x = tmp_t{};                                                               \
    return f(as<tmp_t>(x));                                                    \
  }
        CAF_VARIANT_ASSIGN_CASE_IMPL(0);
        CAF_VARIANT_ASSIGN_CASE_IMPL(1);
        CAF_VARIANT_ASSIGN_CASE_IMPL(2);
        CAF_VARIANT_ASSIGN_CASE_IMPL(3);
        CAF_VARIANT_ASSIGN_CASE_IMPL(4);
        CAF_VARIANT_ASSIGN_CASE_IMPL(5);
        CAF_VARIANT_ASSIGN_CASE_IMPL(6);
        CAF_VARIANT_ASSIGN_CASE_IMPL(7);
        CAF_VARIANT_ASSIGN_CASE_IMPL(8);
        CAF_VARIANT_ASSIGN_CASE_IMPL(9);
        CAF_VARIANT_ASSIGN_CASE_IMPL(10);
        CAF_VARIANT_ASSIGN_CASE_IMPL(11);
        CAF_VARIANT_ASSIGN_CASE_IMPL(12);
        CAF_VARIANT_ASSIGN_CASE_IMPL(13);
        CAF_VARIANT_ASSIGN_CASE_IMPL(14);
        CAF_VARIANT_ASSIGN_CASE_IMPL(15);
        CAF_VARIANT_ASSIGN_CASE_IMPL(16);
        CAF_VARIANT_ASSIGN_CASE_IMPL(17);
        CAF_VARIANT_ASSIGN_CASE_IMPL(18);
        CAF_VARIANT_ASSIGN_CASE_IMPL(19);
        CAF_VARIANT_ASSIGN_CASE_IMPL(20);
        CAF_VARIANT_ASSIGN_CASE_IMPL(21);
        CAF_VARIANT_ASSIGN_CASE_IMPL(22);
        CAF_VARIANT_ASSIGN_CASE_IMPL(23);
        CAF_VARIANT_ASSIGN_CASE_IMPL(24);
        CAF_VARIANT_ASSIGN_CASE_IMPL(25);
        CAF_VARIANT_ASSIGN_CASE_IMPL(26);
        CAF_VARIANT_ASSIGN_CASE_IMPL(27);
        CAF_VARIANT_ASSIGN_CASE_IMPL(28);
        CAF_VARIANT_ASSIGN_CASE_IMPL(29);
#undef CAF_VARIANT_ASSIGN_CASE_IMPL
    }
    return false;
  }

  template <class T>
  result_type apply(caf::optional<T>& x) {
    bool is_set = false;
    if (!apply(is_set)) {
      x = {};
      return false;
    }
    if (!is_set) {
      x = {};
      return true;
    }
    T v;
    if (!apply(v))
      return false;
    x = v;
    return true;
  }

  template <class T>
  result_type apply(std::optional<T>& x) {
    bool is_set = false;
    if (!apply(is_set)) {
      x = {};
      return false;
    }
    if (!is_set) {
      x = {};
      return true;
    }
    T v;
    if (!apply(v))
      return false;
    x = v;
    return true;
  }

  result_type apply(caf::config_value& x) {
    uint8_t type_tag = 0;
    if (!apply(type_tag))
      return false;
    auto& variant = x.get_data();
    //  CAF 0.17 caf::config_value has different types in the underlying
    //  caf::variant: type_list<integer, boolean, real, atom, timespan, uri,
    //  string, list, dictionary>
    // Since for current variant the integer is at index 1, boolean at index 2,
    // real at index 3 we need to map these accordingly
    switch (type_tag) {
      case 0: {
        caf::config_value::integer integer;
        if (!apply(integer))
          return false;
        x = integer;
        return true;
      }
      case 1: {
        caf::config_value::boolean boolean;
        if (!apply(boolean))
          return false;
        x = boolean;
        return true;
      }
      case 2: {
        caf::config_value::real real;
        if (!apply(real))
          return false;
        x = real;
        return true;
      }
    }
    return apply(type_tag, variant);
  }

private:
  template <class T>
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

  inline result_type begin_sequence(size_t& list_size) {
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
  requires(!std::is_rvalue_reference_v<Ts> && ...) bool
legacy_deserialize(const Buffer& buffer, Ts&... xs) {
  legacy_deserializer f{as_bytes(buffer)};
  return f(xs...);
}

} // namespace tenzir::detail
