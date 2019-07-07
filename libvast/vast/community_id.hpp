/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include <cstddef>
#include <type_traits>

#include "vast/address.hpp"
#include "vast/concept/hashable/hash_append.hpp"
#include "vast/concept/hashable/sha1.hpp"
#include "vast/detail/base64.hpp"
#include "vast/detail/byte_swap.hpp"
#include "vast/detail/coding.hpp"
#include "vast/port.hpp"

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

/// The byte values for transport-layer protocols.
/// @relates flow_tuple
enum class protocol : uint8_t {
  icmp = 1,
  tcp = 6,
  udp = 17,
  icmp6 = 58,
  sctp = 132,
};

/// A connection 5-tuple.
struct flow_tuple {
  protocol proto;
  address src_addr;
  port src_port;
  address dst_addr;
  port dst_port;
};

/// Determines whether the flow is ordered, i.e., has a directionality-agnostic
/// component-wise total ordering.
/// @param x The tuple to inspect.
/// @returns `true` if the flow is ordered.
/// @relates flow_tuple
inline bool is_ordered(const flow_tuple& x) noexcept {
  return x.src_addr < x.dst_addr
         || (x.src_addr == x.dst_addr && x.src_port < x.dst_port);
}

/// @relates flow_tuple
template <class Hasher>
void hash_append(Hasher& hasher, const flow_tuple& x) {
  constexpr auto padding = uint8_t{0};
  auto src_port = detail::to_network_order(x.src_port.number());
  auto dst_port = detail::to_network_order(x.dst_port.number());
  if (is_ordered(x)) {
    hash_append(hasher, x.src_addr);
    hash_append(hasher, x.dst_addr);
    hash_append(hasher, x.proto);
    hash_append(hasher, padding);
    hash_append(hasher, src_port);
    hash_append(hasher, dst_port);
  } else {
    hash_append(hasher, x.dst_addr);
    hash_append(hasher, x.src_addr);
    hash_append(hasher, x.proto);
    hash_append(hasher, padding);
    hash_append(hasher, dst_port);
    hash_append(hasher, src_port);
  }
}

/// Computes the length of the version prefix.
/// @see max_length
inline constexpr size_t version_prefix_length() {
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
  return 0;
}

/// Calculates the Community ID for a given flow.
/// @tparam Policy The rendering policy to select Base64 or ASCII.
/// @param flow The flow tuple.
/// @param seed An optional seed to the SHA-1 hash.
/// @returns A string representation of the Community ID for *flow*.
template <class Policy>
std::string compute(const flow_tuple& flow, uint16_t seed = 0) {
  std::string result;
  // Perform exactly one allocator round-trip.
  result.reserve(max_length<Policy>());
  // The version prefix is always present.
  result += version;
  result += ':';
  // Compute a SHA-1 hash over the flow tuple.
  sha1 hasher;
  hash_append(hasher, detail::to_network_order(seed));
  hash_append(hasher, flow);
  auto digest = static_cast<sha1::result_type>(hasher);
  // Convert the binary digest to plain hex ASCII or to Base64.
  constexpr auto element_size = sizeof(sha1::result_type::value_type);
  constexpr auto num_bytes = element_size * digest.size();
  auto ptr = reinterpret_cast<const uint8_t*>(digest.data());
  if constexpr (std::is_same_v<Policy, policy::base64>) {
    result.resize(max_length<Policy>());
    auto offset = version_prefix_length();
    auto n = detail::base64::encode(result.data() + offset, ptr, num_bytes);
    result.resize(offset + n);
  } else {
    for (size_t i = 0; i < num_bytes; ++i) {
      auto [hi, lo] = detail::byte_to_hex<policy::lowercase>(ptr[i]);
      result += hi;
      result += lo;
    }
  }
  return result;
}

} // namespace community_id
} // namespace vast
