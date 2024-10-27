//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/base64.hpp"
#include "tenzir/detail/byteswap.hpp"
#include "tenzir/detail/coding.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/type_traits.hpp"
#include "tenzir/flow.hpp"
#include "tenzir/hash/hash_append.hpp"
#include "tenzir/hash/sha1.hpp"
#include "tenzir/icmp.hpp"
#include "tenzir/ip.hpp"
#include "tenzir/port.hpp"

#include <caf/optional.hpp>

#include <cstddef>
#include <span>
#include <string>
#include <type_traits>

namespace tenzir {
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

/// The default seed according to the spec.
constexpr auto default_seed = uint16_t{0};

template <incremental_hash HashAlgorithm>
auto community_id_hash_append(HashAlgorithm& h, const ip& x) -> void{
  if (x.is_v4())
    hash_append(h, as_bytes(x).subspan<12, 4>());
  else
    hash_append(h, as_bytes(x).subspan<0, 16>());
}

/// Computes a hash of a host pair according to the Community ID specification.
/// @param h The hash algorithm to use.
/// @param src_addr The IP source address.
/// @param dst_addr The IP destination address.
/// @param proto The transport-layer protocol.
template <incremental_hash HashAlgorithm>
auto community_id_hash_append(HashAlgorithm& h, const ip& src_addr,
                              const ip& dst_addr, port_type proto) -> void {
  static constexpr auto padding = uint8_t{0};
  if (src_addr < dst_addr) {
    community_id_hash_append(h, src_addr);
    community_id_hash_append(h, dst_addr);
    hash_append(h, proto);
    hash_append(h, padding);
  } else {
    community_id_hash_append(h, dst_addr);
    community_id_hash_append(h, src_addr);
    hash_append(h, proto);
    hash_append(h, padding);
  }
}

/// Computes a hash of a flow according to the community ID specification.
/// @param h The hash algorithm to use.
/// @param x The flow to hash.
/// @relates flow
template <incremental_hash HashAlgorithm>
auto community_id_hash_append(HashAlgorithm& h, const flow& x) -> void{
  TENZIR_ASSERT(x.src_port.type() == x.dst_port.type());
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
constexpr auto version_prefix_length() -> size_t {
  constexpr size_t version_number = 1;
  constexpr size_t version_separator = 1;
  return version_number + version_separator;
}

/// Computes the maximum length of the Community ID string.
/// @returns An upper bound in bytes.
/// @see version_prefix_length
template <class Policy>
constexpr auto max_length() -> size_t {
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
/// @param seed An optional seed to the SHA-1 hash.
/// @param xs The host pair or flow.
/// @returns A string representation of the Community ID for *x*.
template <class Policy, class... Ts>
auto compute(uint16_t seed, const Ts&... xs) -> std::string {
  std::string result;
  // Perform exactly one allocator round-trip.
  result.reserve(max_length<Policy>());
  // The version prefix is always present.
  result += version;
  result += ':';
  // Compute a SHA-1 hash over the flow tuple.
  sha1 hasher;
  hash_append(hasher, detail::to_network_order(seed));
  community_id_hash_append(hasher, xs...);
  auto digest = hasher.finish();
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
    detail::hexify<policy::lowercase>(as_bytes(digest), result);
  } else {
    static_assert(detail::always_false_v<Policy>, "unsupported policy");
  }
  return result;
}

// Primary functions to construct a Community ID.

template <class Policy = policy::base64>
auto make(const flow& x, uint16_t seed = default_seed) -> std::string {
  return compute<Policy>(seed, x);
}

template <class Policy = policy::base64>
auto make(const ip& src_addr, const ip& dst_addr, port_type proto,
          uint16_t seed = default_seed) -> std::string {
  return compute<Policy>(seed, src_addr, dst_addr, proto);
}

} // namespace community_id
} // namespace tenzir
