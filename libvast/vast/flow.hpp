//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/address.hpp"
#include "vast/concept/hashable/hash.hpp"
#include "vast/concept/hashable/uniquely_represented.hpp"
#include "vast/port.hpp"

#include <functional>
#include <optional>
#include <string_view>

namespace vast {

/// A connection 5-tuple, consisting of IP addresses and transport-layer ports
/// for originator and resopnder. The protocol type is encoded in the ports.
struct flow {
  address src_addr;
  address dst_addr;
  port src_port;
  port dst_port;
};

template <>
struct is_uniquely_represented<flow>
  : std::bool_constant<is_uniquely_represented<address>::value
                       && is_uniquely_represented<port>::value
                       && sizeof(flow)
                            == ((2 * sizeof(address)) + (2 * sizeof(port)))> {};

/// Factory function to construct a flow.
/// @param src_addr The IP address of the flow source.
/// @param dst_addr The IP address of the flow destination.
/// @param src_port The transport-layer port of the flow source.
/// @param dst_port The transport-layer port of the flow destination.
/// @param protocol The transport-layer protocol in use.
/// @return An instance of a flow.
/// @relates flow
inline flow make_flow(address src_addr, address dst_addr, uint16_t src_port,
                      uint16_t dst_port, port_type protocol) {
  return {src_addr, dst_addr, port{src_port, protocol},
          port{dst_port, protocol}};
}

/// Factory function to construct a flow.
/// @param src_addr The IP address of the flow source.
/// @param dst_addr The IP address of the flow destination.
/// @param src_port The transport-layer port of the flow source.
/// @param dst_port The transport-layer port of the flow destination.
/// @return An instance of a flow.
/// @relates flow
template <port_type Protocol>
flow make_flow(address src_addr, address dst_addr, uint16_t src_port,
               uint16_t dst_port) {
  return make_flow(src_addr, dst_addr, src_port, dst_port, Protocol);
}

/// Factory function to construct a flow.
/// @param src_addr The IP address of the flow source as string.
/// @param dst_addr The IP address of the flow destination as string.
/// @param src_port The transport-layer port of the flow source.
/// @param dst_port The transport-layer port of the flow destination.
/// @param protocol The transport-layer protocol in use.
/// @return An instance of a flow.
/// @relates flow
std::optional<flow>
make_flow(std::string_view src_addr, std::string_view dst_addr,
          uint16_t src_port, uint16_t dst_port, port_type protocol);

/// Factory function to construct a flow.
/// @param src_addr The IP address of the flow source as string.
/// @param dst_addr The IP address of the flow destination as string.
/// @param src_port The transport-layer port of the flow source.
/// @param dst_port The transport-layer port of the flow destination.
/// @return An instance of a flow.
/// @relates flow
template <port_type Protocol>
auto make_flow(std::string_view src_addr, std::string_view dst_addr,
               uint16_t src_port, uint16_t dst_port) {
  return make_flow(src_addr, dst_addr, src_port, dst_port, Protocol);
}

/// @returns the protocol of a flow tuple.
/// @param x The flow to extract the protocol from.
/// @relates flow
port_type protocol(const flow& x);

/// @relates flow
bool operator==(const flow& x, const flow& y);

/// @relates flow
inline bool operator!=(const flow& x, const flow& y) {
  return !(x == y);
}

/// @relates flow
template <class Inspector>
auto inspect(Inspector& f, flow& x) {
  return f(x.src_addr, x.dst_addr, x.src_port, x.dst_port);
}

} // namespace vast

namespace std {

template <>
struct hash<vast::flow> {
  size_t operator()(const vast::flow& x) const {
    return vast::hash(x);
  }
};

} // namespace std
