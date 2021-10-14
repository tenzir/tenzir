//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/address.hpp"

#include "vast/as_bytes.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/address.hpp"
#include "vast/data.hpp"
#include "vast/word.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <cstdlib>
#include <utility>

namespace vast {

bool address::is_v4() const {
  return std::memcmp(&bytes_, &v4_mapped_prefix, 12) == 0;
}

bool address::is_v6() const {
  return !is_v4();
}

bool address::is_loopback() const {
  if (is_v4())
    return bytes_[12] == 127;
  else
    return ((bytes_[0] == 0) && (bytes_[1] == 0) && (bytes_[2] == 0)
            && (bytes_[3] == 0) && (bytes_[4] == 0) && (bytes_[5] == 0)
            && (bytes_[6] == 0) && (bytes_[7] == 0) && (bytes_[8] == 0)
            && (bytes_[9] == 0) && (bytes_[10] == 0) && (bytes_[11] == 0)
            && (bytes_[12] == 0) && (bytes_[13] == 0) && (bytes_[14] == 0)
            && (bytes_[15] == 1));
}

bool address::is_broadcast() const {
  return is_v4() && bytes_[12] == 0xff && bytes_[13] == 0xff
         && bytes_[14] == 0xff && bytes_[15] == 0xff;
}

bool address::is_multicast() const {
  return is_v4() ? bytes_[12] == 224 : bytes_[0] == 0xff;
}

namespace {

inline uint32_t bitmask32(size_t bottom_bits) {
  return bottom_bits >= 32 ? 0xffffffff : ((uint32_t{1} << bottom_bits) - 1);
}

} // namespace

bool address::mask(unsigned top_bits_to_keep) {
  if (top_bits_to_keep > 128)
    return false;
  uint32_t m[4] = {0xffffffff, 0xffffffff, 0xffffffff, 0xffffffff};
  auto r = std::ldiv(top_bits_to_keep, 32);
  if (r.quot < 4)
    m[r.quot] = detail::to_network_order(m[r.quot] & ~bitmask32(32 - r.rem));
  for (size_t i = r.quot + 1; i < 4; ++i)
    m[i] = 0;
  auto p = reinterpret_cast<uint32_t*>(std::launder(&bytes_[0]));
  for (size_t i = 0; i < 4; ++i)
    p[i] &= m[i];
  return true;
}

address& address::operator&=(const address& other) {
  for (auto i = 0u; i < 16u; ++i)
    bytes_[i] &= other.bytes_[i];
  return *this;
}

address& address::operator|=(const address& other) {
  for (auto i = 0u; i < 16u; ++i)
    bytes_[i] |= other.bytes_[i];
  return *this;
}

address& address::operator^=(const address& other) {
  if (is_v4() || other.is_v4())
    for (auto i = 12u; i < 16u; ++i)
      bytes_[i] ^= other.bytes_[i];
  else
    for (auto i = 0u; i < 16u; ++i)
      bytes_[i] ^= other.bytes_[i];
  return *this;
}

bool address::compare(const address& other, size_t k) const {
  VAST_ASSERT(k > 0 && k <= 128);
  auto x = bytes_.data();
  auto y = other.bytes_.data();
  for (; k > 8; k -= 8)
    if (*x++ != *y++)
      return false;
  auto mask = word<byte_type>::msb_fill(k);
  return (*x & mask) == (*y & mask);
}

bool operator==(const address& x, const address& y) {
  return x.bytes_ == y.bytes_;
}

bool operator<(const address& x, const address& y) {
  return x.bytes_ < y.bytes_;
}

} // namespace vast
