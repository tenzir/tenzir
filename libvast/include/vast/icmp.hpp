//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/fwd.hpp>

#include <cstdint>
#include <optional>

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
std::optional<icmp_type> dual(icmp_type x);

/// Computes the dual to a given ICMP6 type.
/// @param x The ICMP6 type.
/// @returns For a request type, the response type - and vice versa.
/// @relates icmp6_type
std::optional<icmp6_type> dual(icmp6_type x);

} // namespace vast
