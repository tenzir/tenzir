//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

// Most of the actual implementation in this file comes from a 3rd party and
// has been adapted to fit into the VAST code base. Details about the original
// file:
//
// - Repository: https://github.com/kerukuro/digestpp
// - Commit:     92dc9b33a63da76b088ac2471fbb350177d04c2f
// - Path:       algorithm/detail/sha1_provider.hpp
// - Author:     kerukuro
// - License:    The Unlicense

#include "vast/hash/sha1.hpp"

#include "vast/detail/byte_swap.hpp"

#include <cstring>

namespace vast {
namespace {

// Rotate 32-bit unsigned integer to the left.
uint32_t rotate_left(uint32_t x, unsigned n) {
  return (x << n) | (x >> (32 - n));
}

// Accumulate data and call the transformation function for full blocks.
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
    total += (bytes) *8;
  }
  std::memcpy(m + pos, data, len);
  pos += len;
}

// -- SHA1 constants and functions --------------------------------------------

std::array<uint32_t, 4> K = {0x5a827999, 0x6ed9eba1, 0x8f1bbcdc, 0xca62c1d6};

uint32_t choice(uint32_t x, uint32_t y, uint32_t z) {
  return (x & y) ^ (~x & z);
}

uint32_t parity(uint32_t x, uint32_t y, uint32_t z) {
  return x ^ y ^ z;
}

uint32_t majority(uint32_t x, uint32_t y, uint32_t z) {
  return (x & y) ^ (x & z) ^ (y & z);
}

} // namespace

sha1::sha1() noexcept {
  H_[0] = 0x67452301;
  H_[1] = 0xefcdab89;
  H_[2] = 0x98badcfe;
  H_[3] = 0x10325476;
  H_[4] = 0xc3d2e1f0;
}

void sha1::add(std::span<const std::byte> bytes) noexcept {
  auto f = [this](const unsigned char* data, size_t len) {
    transform(data, len);
  };
  auto ptr = reinterpret_cast<const unsigned char*>(bytes.data());
  absorb_bytes(ptr, bytes.size(), 64, 64, m_.data(), pos_, total_, f);
}

sha1::result_type sha1::finish() noexcept {
  finalize();
  return H_;
}

void sha1::finalize() {
  total_ += pos_ * 8;
  m_[pos_++] = 0x80;
  if (pos_ > 56) {
    if (pos_ != 64)
      std::memset(&m_[pos_], 0, 64 - pos_);
    transform(&m_[0], 1);
    pos_ = 0;
  }
  std::memset(&m_[0] + pos_, 0, 56 - pos_);
  uint64_t mlen = detail::byte_swap(total_);
  std::memcpy(&m_[0] + (64 - 8), &mlen, 64 / 8);
  transform(&m_[0], 1);
  for (int i = 0; i < 5; i++)
    H_[i] = detail::byte_swap(H_[i]);
}

void sha1::transform(const unsigned char* data, size_t num_blks) {
  for (uint64_t blk = 0; blk < num_blks; blk++) {
    uint32_t m[16];
    auto xs = reinterpret_cast<const uint32_t*>(data);
    for (uint32_t i = 0; i < 64 / 4; i++)
      m[i] = detail::byte_swap(xs[blk * 16 + i]);
    uint32_t w[80];
    for (int t = 0; t <= 15; t++)
      w[t] = m[t];
    for (int t = 16; t <= 79; t++)
      w[t] = rotate_left(w[t - 3] ^ w[t - 8] ^ w[t - 14] ^ w[t - 16], 1);
    auto a = H_[0];
    auto b = H_[1];
    auto c = H_[2];
    auto d = H_[3];
    auto e = H_[4];
    auto k = K[0];
    auto f = choice;
    for (int t = 0; t <= 79; t++) {
      auto T = rotate_left(a, 5) + f(b, c, d) + e + k + w[t];
      e = d;
      d = c;
      c = rotate_left(b, 30);
      b = a;
      a = T;
      if (t == 19) {
        f = parity;
        k = K[1];
      } else if (t == 39) {
        f = majority;
        k = K[2];
      } else if (t == 59) {
        f = parity;
        k = K[3];
      }
    }
    H_[0] += a;
    H_[1] += b;
    H_[2] += c;
    H_[3] += d;
    H_[4] += e;
  }
}

} // namespace vast
