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
#include <cstdint>
#include <span>

namespace vast {

/// The [CRC32](http://en.wikipedia.org/wiki/Cyclic_redundancy_check) algorithm.
class crc32 {
public:
  static constexpr detail::endian endian = detail::endian::native;

  using result_type = uint32_t;
  using seed_type = result_type;

  explicit crc32(seed_type seed = 0) noexcept;

  void add(std::span<const std::byte> bytes) noexcept;

  result_type finish() const noexcept;

  template <class Inspector>
  friend auto inspect(Inspector& f, crc32& crc) {
    return f(crc.digest_);
  }

private:
  result_type digest_;
};

} // namespace vast
