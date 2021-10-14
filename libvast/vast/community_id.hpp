//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/address.hpp"
#include "vast/concept/hashable/hash_append.hpp"
#include "vast/concept/hashable/sha1.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/base64.hpp"
#include "vast/detail/byte_swap.hpp"
#include "vast/detail/coding.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/flow.hpp"
#include "vast/icmp.hpp"
#include "vast/port.hpp"

#include <caf/optional.hpp>

#include <cstddef>
#include <span>
#include <string>
#include <type_traits>

namespace vast {
namespace policy {

/// Tag type to select Base64 encoding.
struct base64 {};

/// Tag type to select plain hex ASCII encoding.
struct ascii {};

} // namespace policy

/// The [Community ID](https://github.com/corelight/community-id-spec) flow
/// hashing algorithm.
namespace community_id {

/// The Community ID version.
constexpr char version = '1';

template <incremental_hash HashAlgorithm>
void community_id_hash_append(HashAlgorithm& h, const address& x) {
  if (x.is_v4())
    hash_append(h, as_bytes(x).subspan<12, 4>());
  else
    hash_append(h, as_bytes(x).subspan<0, 16>());
}

/// Computes a hash of a flow according to the community ID specification.
/// @param h The hash algorithm to use.
/// @param x The flow to hash.
/// @relates flow
template <incremental_hash HashAlgorithm>
void community_id_hash_append(HashAlgorithm& h, const flow& x) {
  VAST_ASSERT(x.src_port.type() == x.dst_port.type());
  auto src_port_num = x.src_port.number();
  auto dst_port_num = x.dst_port.number();
  auto is_one_way = false;
  // Normalize ICMP and ICMP6. Source and destination port map to ICMP
  // message type and message code.
  if (protocol(x) == port_type::icmp) {
    if (auto p = dual(detail::narrow_cast<icmp_type>(src_port_num)))
      dst_port_num = static_cast<uint16_t>(*p);
    else
      is_one_way = true;
  } else if (protocol(x) == port_type::icmp6) {
    if (auto p = dual(detail::narrow_cast<icmp6_type>(src_port_num)))
      dst_port_num = static_cast<uint16_t>(*p);
    else
      is_one_way = true;
  }
  auto is_ordered = is_one_way || x.src_addr < x.dst_addr
                    || (x.src_addr == x.dst_addr
                        && src_port_num < dst_port_num);
  // Adjust byte order - if needed.
  src_port_num = detail::to_network_order(src_port_num);
  dst_port_num = detail::to_network_order(dst_port_num);
  static constexpr auto padding = uint8_t{0};
  if (is_ordered) {
    community_id_hash_append(h, x.src_addr);
    community_id_hash_append(h, x.dst_addr);
    hash_append(h, protocol(x));
    hash_append(h, padding);
    hash_append(h, src_port_num);
    hash_append(h, dst_port_num);
  } else {
    community_id_hash_append(h, x.dst_addr);
    community_id_hash_append(h, x.src_addr);
    hash_append(h, protocol(x));
    hash_append(h, padding);
    hash_append(h, dst_port_num);
    hash_append(h, src_port_num);
  }
}

/// Computes the length of the version prefix.
/// @see max_length
constexpr size_t version_prefix_length() {
  constexpr size_t version_number = 1;
  constexpr size_t version_separator = 1;
  return version_number + version_separator;
}

/// Computes the maximum length of the Community ID string.
/// @returns An upper bound in bytes.
/// @see version_prefix_length
template <class Policy>
constexpr size_t max_length() {
  constexpr auto hex_size = (160 / 8) * 2; // 160-bit SHA-1 digest as hex.
  auto prefix = version_prefix_length();
  if constexpr (std::is_same_v<Policy, policy::base64>)
    return prefix + detail::base64::encoded_size(hex_size);
  else if constexpr (std::is_same_v<Policy, policy::ascii>)
    return prefix + hex_size;
  else
    static_assert(detail::always_false_v<Policy>, "unsupported policy");
}

/// Calculates the Community ID for a given flow.
/// @tparam Policy The rendering policy to select Base64 or ASCII.
/// @param x The flow tuple.
/// @param seed An optional seed to the SHA-1 hash.
/// @returns A string representation of the Community ID for *x*.
template <class Policy>
std::string compute(const flow& x, uint16_t seed = 0) {
  std::string result;
  // Perform exactly one allocator round-trip.
  result.reserve(max_length<Policy>());
  // The version prefix is always present.
  result += version;
  result += ':';
  // Compute a SHA-1 hash over the flow tuple.
  sha1 hasher;
  hash_append(hasher, detail::to_network_order(seed));
  community_id_hash_append(hasher, x);
  auto digest = static_cast<sha1::result_type>(hasher);
  // Convert the binary digest to plain hex ASCII or to Base64.
  if constexpr (std::is_same_v<Policy, policy::base64>) {
    constexpr auto element_size = sizeof(sha1::result_type::value_type);
    constexpr auto num_bytes = element_size * digest.size();
    auto ptr = reinterpret_cast<const uint8_t*>(digest.data());
    result.resize(max_length<Policy>());
    auto offset = version_prefix_length();
    auto n = detail::base64::encode(result.data() + offset, ptr, num_bytes);
    result.resize(offset + n);
  } else if constexpr (std::is_same_v<Policy, policy::ascii>) {
    auto bytes = as_bytes(std::span{digest.data(), digest.size()});
    detail::hexify<policy::lowercase>(bytes, result);
  } else {
    static_assert(detail::always_false_v<Policy>, "unsupported policy");
  }
  return result;
}

} // namespace community_id
} // namespace vast
