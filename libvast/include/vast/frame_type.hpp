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
      using enum vast::frame_type;
      case invalid:
        type = "invalid";
        break;
      case ethernet:
        type = "ethernet";
        break;
      case vlan:
        type = "vlan";
        break;
      case mpls:
        type = "mpls";
        break;
      case pppoe:
        type = "pppoe";
        break;
      case ppp:
        type = "ppp";
        break;
      case chap:
        type = "chap";
        break;
      case ipv4:
        type = "ipv4";
        break;
      case udp:
        type = "udp";
        break;
      case radius:
        type = "radius";
        break;
      case radavp:
        type = "radavp";
        break;
      case l2tp:
        type = "l2tp";
        break;
      case l2avp:
        type = "l2avp";
        break;
      case ospfv2:
        type = "ospfv2";
        break;
      case ospf_md5:
        type = "ospf_md5";
        break;
      case tcp:
        type = "tcp";
        break;
      case ip_md5:
        type = "ip_md5";
        break;
      case unknown:
        type = "unknown";
        break;
      case gre:
        type = "gre";
        break;
      case gtp:
        type = "gtp";
        break;
      case vxlan:
        type = "vxlan";
        break;
    }
    return formatter<std::string_view>::format(type, ctx);
  }
};

} // namespace fmt
