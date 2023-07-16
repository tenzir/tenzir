//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <fmt/format.h>

namespace tenzir {

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

} // namespace tenzir

namespace fmt {

template <>
struct formatter<tenzir::frame_type> : formatter<string_view> {
  using super = formatter<string_view>;

  template <class FormatContext>
  constexpr auto format(const tenzir::frame_type& x, FormatContext& ctx)
    -> decltype(ctx.out()) {
    switch (x) {
      case tenzir::frame_type::invalid:
        return super::format("invalid", ctx);
      case tenzir::frame_type::ethernet:
        return super::format("ethernet", ctx);
      case tenzir::frame_type::vlan:
        return super::format("vlan", ctx);
      case tenzir::frame_type::mpls:
        return super::format("mpls", ctx);
      case tenzir::frame_type::pppoe:
        return super::format("pppoe", ctx);
      case tenzir::frame_type::ppp:
        return super::format("ppp", ctx);
      case tenzir::frame_type::chap:
        return super::format("chap", ctx);
      case tenzir::frame_type::ipv4:
        return super::format("ipv4", ctx);
      case tenzir::frame_type::udp:
        return super::format("udp", ctx);
      case tenzir::frame_type::radius:
        return super::format("radius", ctx);
      case tenzir::frame_type::radavp:
        return super::format("radavp", ctx);
      case tenzir::frame_type::l2tp:
        return super::format("l2tp", ctx);
      case tenzir::frame_type::l2avp:
        return super::format("l2avp", ctx);
      case tenzir::frame_type::ospfv2:
        return super::format("ospfv2", ctx);
      case tenzir::frame_type::ospf_md5:
        return super::format("ospf_md5", ctx);
      case tenzir::frame_type::tcp:
        return super::format("tcp", ctx);
      case tenzir::frame_type::ip_md5:
        return super::format("ip_md5", ctx);
      case tenzir::frame_type::unknown:
        return super::format("unknown", ctx);
      case tenzir::frame_type::gre:
        return super::format("gre", ctx);
      case tenzir::frame_type::gtp:
        return super::format("gtp", ctx);
      case tenzir::frame_type::vxlan:
        return super::format("vxlan", ctx);
    }
    return super::format("unknown", ctx);
  }
};

} // namespace fmt
