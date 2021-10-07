//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/bit.hpp"

#include <cstddef>
#include <type_traits>

namespace vast {

struct xxhash_base {
  using result_type = size_t;

  // If XXH_FORCE_NATIVE_FORMAT == 1 in xxhash.c, then use endian::native.
  static constexpr detail::endian endian = detail::endian::little;
};

/// The 32-bit version of xxHash.
class xxhash32 : public xxhash_base {
public:
  explicit xxhash32(result_type seed = 0) noexcept;

  void operator()(const void* x, size_t n) noexcept;

  explicit operator result_type() noexcept;

  template <class Inspector>
  friend auto inspect(Inspector& f, xxhash32& xxh) {
    return f(xxh.state_);
  }

private:
  // Must be kept in sync with xxhash.h.
  struct state_type {
    unsigned total_len_32;
    unsigned large_len;
    unsigned v1;
    unsigned v2;
    unsigned v3;
    unsigned v4;
    unsigned mem32[4];
    unsigned memsize;
    unsigned reserved;
  };

  state_type state_;
};

/// The 64-bit version of xxHash.
class xxhash64 : public xxhash_base {
public:
  explicit xxhash64(result_type seed = 0) noexcept;

  void operator()(const void* x, size_t n) noexcept;

  explicit operator result_type() noexcept;

  template <class Inspector>
  friend auto inspect(Inspector& f, xxhash64& xxh) {
    return f(xxh.state_);
  }

private:
  // Must be kept in sync with xxhash.h.
  struct state_type {
    unsigned long long total_len;
    unsigned long long v1;
    unsigned long long v2;
    unsigned long long v3;
    unsigned long long v4;
    unsigned long long mem64[4];
    unsigned memsize;
    unsigned reserved[2];
  };

  state_type state_;
};

/// The [xxhash](https://github.com/Cyan4973/xxHash) algorithm.
using xxhash = std::conditional_t<sizeof(void*) == 4, xxhash32, xxhash64>;

} // namespace vast
