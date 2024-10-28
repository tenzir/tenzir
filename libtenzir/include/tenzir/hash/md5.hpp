
//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <span>

namespace tenzir {

/// The [MD5](https://en.wikipedia.org/wiki/MD5) hash algorithm.
/// This implementation comes from https://github.com/kerukuro/digestpp.
class md5 {
public:
  using result_type = std::array<const std::byte, 128 / 8>;

  static constexpr std::endian endian = std::endian::native;

  md5() noexcept;

  void add(std::span<const std::byte> bytes) noexcept;

  auto finish() noexcept -> result_type;

  friend auto inspect(auto& f, md5& x) {
    return f.object(x).fields(f.field("H", x.H_), f.field("m", x.m_),
                              f.field("os", x.pos_),
                              f.field("total", x.total_));
  }

private:
  void finalize();

  void transform(const unsigned char* data, size_t num_blks);

  std::array<uint32_t, 4> H_;
  std::array<unsigned char, 64> m_;
  size_t pos_ = 0;
  uint64_t total_ = 0;
};

} // namespace tenzir
