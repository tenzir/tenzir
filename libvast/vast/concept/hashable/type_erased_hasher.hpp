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

#include <cstddef>
#include <type_traits>

namespace vast {

/// A type-erased hasher that encapsulates an hash function. For details, see
/// http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2014/n3980.html.
/// @relates hash_append
template <class Result, detail::endianness Endian = detail::host_endian>
class type_erased_hasher {
  using function = std::function<void(void const*, size_t)>;

public:
  static constexpr detail::endianness endian = Endian;

  using result_type = Result;

  template <
    class Hasher,
    class = std::enable_if_t<
      std::is_constructible_v<function, Hasher>
        && std::is_same_v<
             typename std::decay_t<Hasher>::result_type,
             result_type
           >
      >
    >
  explicit type_erased_hasher(Hasher&& h)
    : hasher_(std::forward<Hasher>(h)),
      convert_(convert<std::decay_t<Hasher>>) {
    static_assert(endian == std::decay_t<Hasher>::endian);
  }

  void operator()(void const* key, size_t len) {
    hasher_(key, len);
  }

  explicit operator result_type() noexcept {
    return convert_(hasher_);
  }

  template <class T>
  T* target() noexcept {
    return hasher_.target<T>();
  }

private:
  template <class Hasher>
  static result_type convert(function& f) noexcept {
    return static_cast<result_type>(*f.target<Hasher>());
  }

  function hasher_;
  result_type (*convert_)(function&);
};

} // namespace vast
