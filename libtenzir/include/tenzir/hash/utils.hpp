//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// Most of the actual implementation in this file comes from a 3rd party and
// has been adapted to fit into the Tenzir code base. Details about the original
// file:
//
// - Repository: https://github.com/kerukuro/digestpp
// - Commit:     ebb699402c244e22c3aff61d2239bcb2e87b8ef8
// - Path:       detail/functions.hpp, detail/absorb_data.hpp
// - Author:     kerukuro
// - License:    The Unlicense

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>

namespace tenzir::detail {

// Utility functions for hash function computation. See
// https://github.com/kerukuro/digestpp for details.

// Rotate 32-bit unsigned integer to the left.
inline auto rotate_left(uint32_t x, unsigned n) -> uint32_t {
  return (x << n) | (x >> (32 - n));
}

inline auto rotate_right(uint32_t x, unsigned n) -> uint32_t {
  return (x >> n) | (x << (32 - n));
}

inline auto rotate_right(uint64_t x, unsigned n) -> uint64_t {
  return (x >> n) | (x << (64 - n));
}

template <class T, class F>
void absorb_bytes(const unsigned char* data, size_t len, size_t bs,
                  size_t bschk, unsigned char* m, size_t& pos, T& total,
                  F transform) {
  if (pos && pos + len >= bschk) {
    std::memcpy(m + pos, data, bs - pos);
    transform(m, 1);
    len -= bs - pos;
    data += bs - pos;
    total += bs * 8;
    pos = 0;
  }
  if (len >= bschk) {
    size_t blocks = (len + bs - bschk) / bs;
    size_t bytes = blocks * bs;
    transform(data, blocks);
    len -= bytes;
    data += bytes;
    total += (bytes) * 8;
  }
  std::memcpy(m + pos, data, len);
  pos += len;
}

} // namespace tenzir::detail
