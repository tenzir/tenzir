//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/bit.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>

namespace vast {

/// The [SHA-1](https://en.wikipedia.org/wiki/SHA-1) hash algorithm.
/// This implementation comes from https://github.com/kerukuro/digestpp.
class sha1 {
public:
  using result_type = std::array<uint32_t, 5>;

  static constexpr detail::endian endian = detail::endian::native;

  sha1() noexcept;

  void add(std::span<const std::byte> bytes) noexcept;

  result_type finish() noexcept;

  template <class Inspector>
  friend auto inspect(Inspector& f, sha1& x) {
    return f(x.H_, x.m_, x.pos_, x.total_);
  }

private:
  void finalize();

  void transform(const unsigned char* data, size_t num_blks);

  std::array<uint32_t, 5> H_;
  std::array<unsigned char, 64> m_;
  size_t pos_ = 0;
  uint64_t total_ = 0;
};

} // namespace vast
