//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <bit>
#include <cstddef>
#include <cstdint>
#include <span>

namespace tenzir {

/// The [CRC32](http://en.wikipedia.org/wiki/Cyclic_redundancy_check) algorithm.
class crc32 {
public:
  static constexpr std::endian endian = std::endian::native;

  using result_type = uint32_t;
  using seed_type = result_type;

  explicit crc32(seed_type seed = 0) noexcept;

  void reset() noexcept;

  void add(std::span<const std::byte> bytes) noexcept;

  result_type finish() const noexcept;

  template <class Inspector>
  friend auto inspect(Inspector& f, crc32& crc) {
    return f(crc.seed_, crc.digest_);
  }

private:
  result_type digest_;
  seed_type seed_;
};

} // namespace tenzir
