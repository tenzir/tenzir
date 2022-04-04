//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <fmt/core.h>

#include <string>

namespace vast {

/// The type of a layer-2 frame.
enum class frame_type : char {
  invalid = '\x00',
  ethernet = '\x01',
  vlan = '\x02',
  mpls = '\x03',
  pppoe = '\x04',
  ppp = '\x05',
  chap = '\x06',
  ipv4 = '\x07',
  udp = '\x08',
  radius = '\x09',
  radavp = '\x0a',
  l2tp = '\x0b',
  l2avp = '\x0c',
  ospfv2 = '\x0d',
  ospf_md5 = '\x0e',
  tcp = '\x0f',
  ip_md5 = '\x10',
  unknown = '\x11',
  gre = '\x12',
  gtp = '\x13',
  vxlan = '\x14'
};

} // namespace vast

namespace fmt {

template <>
struct formatter<vast::frame_type> : formatter<std::string_view> {
  template <class FormatContext>
  auto format(vast::frame_type x, FormatContext& ctx) const {
    std::string_view type = "unknown";
    switch (x) {
      case vast::frame_type::invalid:
        type = "invalid";
        break;
      case vast::frame_type::ethernet:
        type = "ethernet";
        break;
      case vast::frame_type::vlan:
        type = "vlan";
        break;
      case vast::frame_type::mpls:
        type = "mpls";
        break;
      case vast::frame_type::pppoe:
        type = "pppoe";
        break;
      case vast::frame_type::ppp:
        type = "ppp";
        break;
      case vast::frame_type::chap:
        type = "chap";
        break;
      case vast::frame_type::ipv4:
        type = "ipv4";
        break;
      case vast::frame_type::udp:
        type = "udp";
        break;
      case vast::frame_type::radius:
        type = "radius";
        break;
      case vast::frame_type::radavp:
        type = "radavp";
        break;
      case vast::frame_type::l2tp:
        type = "l2tp";
        break;
      case vast::frame_type::l2avp:
        type = "l2avp";
        break;
      case vast::frame_type::ospfv2:
        type = "ospfv2";
        break;
      case vast::frame_type::ospf_md5:
        type = "ospf_md5";
        break;
      case vast::frame_type::tcp:
        type = "tcp";
        break;
      case vast::frame_type::ip_md5:
        type = "ip_md5";
        break;
      case vast::frame_type::unknown:
        type = "unknown";
        break;
      case vast::frame_type::gre:
        type = "gre";
        break;
      case vast::frame_type::gtp:
        type = "gtp";
        break;
      case vast::frame_type::vxlan:
        type = "vxlan";
        break;
    }
    return formatter<std::string_view>::format(type, ctx);
  }
};

} // namespace fmt
