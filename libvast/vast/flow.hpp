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

#include <functional>
#include <string_view>

#include <caf/optional.hpp>

#include "vast/address.hpp"
#include "vast/port.hpp"

namespace vast {

/// A connection 5-tuple, consisting of IP addresses and transport-layer ports
/// for originator and resopnder. The protocol type is encoded in the ports.
struct flow {
  address src_addr;
  address dst_addr;
  port src_port;
  port dst_port;
};

/// Factory function to construct a flow.
/// @param src_addr The IP address of the flow source.
/// @param dst_addr The IP address of the flow destination.
/// @param src_port The transport-layer port of the flow source.
/// @param dst_port The transport-layer port of the flow destination.
/// @param protocol The transport-layer protocol in use.
/// @return An instance of a flow.
/// @relates flow
inline flow make_flow(address src_addr, address dst_addr, uint16_t src_port,
                      uint16_t dst_port, port::port_type protocol) {
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
template <port::port_type Protocol>
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
caf::optional<flow> make_flow(std::string_view src_addr,
                              std::string_view dst_addr, uint16_t src_port,
                              uint16_t dst_port, port::port_type protocol);

/// Factory function to construct a flow.
/// @param src_addr The IP address of the flow source as string.
/// @param dst_addr The IP address of the flow destination as string.
/// @param src_port The transport-layer port of the flow source.
/// @param dst_port The transport-layer port of the flow destination.
/// @return An instance of a flow.
/// @relates flow
template <port::port_type Protocol>
auto make_flow(std::string_view src_addr, std::string_view dst_addr,
               uint16_t src_port, uint16_t dst_port) {
  return make_flow(src_addr, dst_addr, src_port, dst_port, Protocol);
}

/// @returns the protocol of a flow tuple.
/// @param x The flow to extract the protocol from.
/// @relates flow
port::port_type protocol(const flow& x);

/// @returns a hash value for a flow tuple.
/// @param x The flow to compute the hash value from.
/// @relates flow
size_t hash(const flow& x);

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

/// @relates flow
template <class Hasher>
void hash_append(Hasher& h, const flow& x) {
  hash_append(h, x.src_addr, x.dst_addr, x.src_port, x.dst_port);
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
