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

#include <cstdint>

#include <caf/fwd.hpp>

namespace vast {

/// ICMP message type.
/// @see icmp6_type
enum class icmp_type : uint8_t {
  echo_reply = 0,
  echo = 8,
  rtr_advert = 9,
  rtr_solicit = 10,
  tstamp = 13,
  tstamp_reply = 14,
  info = 15,
  info_reply = 16,
  mask = 17,
  mask_reply = 18,
};

/// ICMP6 message type.
/// @see icmp_type
enum class icmp6_type : uint8_t {
  echo_request = 128,
  echo_reply = 129,
  mld_listener_query = 130,
  mld_listener_report = 131,
  nd_router_solicit = 133,
  nd_router_advert = 134,
  nd_neighbor_solicit = 135,
  nd_neighbor_advert = 136,
  wru_request = 139,
  wru_reply = 140,
  haad_request = 144,
  haad_reply = 145,
};

/// Computes the dual to a given ICMP type.
/// @param x The ICMP type.
/// @returns For a request type, the response type - and vice versa.
/// @relates icmp_type
caf::optional<icmp_type> dual(icmp_type x);

/// Computes the dual to a given ICMP6 type.
/// @param x The ICMP6 type.
/// @returns For a request type, the response type - and vice versa.
/// @relates icmp6_type
caf::optional<icmp6_type> dual(icmp6_type x);

} // namespace vast
