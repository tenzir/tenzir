//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <fmt/format.h>

#include <cstddef>
#include <cstdint>
#include <span>

namespace tenzir {

/// The two-octet field of an Ethernet frame that identifies the protocol of
/// its payload. The values are a subset of the assignments in the IEEE
/// EtherType registry at https://standards-oui.ieee.org/ethertype/eth.txt.
enum class ether_type : uint16_t {
  invalid = 0x0000,
  ipv4 = 0x0800,
  arp = 0x0806,
  wol = 0x0842,
  avtp = 0x22F0,
  trill = 0x22F3,
  srp = 0x22EA,
  dec_mop_rc = 0x6002,
  decnet = 0x6003,
  dec_lat = 0x6004,
  rarp = 0x8035,
  apple_talk = 0x809B,
  aarp = 0x80F3,
  ieee_802_1aq = 0x8100,
  slpp = 0x8102,
  ipx = 0x8137,
  qnx = 0x8204,
  ipv6 = 0x86DD,
  flow_control = 0x8808,
  slow_protocols = 0x8809,
  cobra_net = 0x8819,
  mpls_uni = 0x8847,
  mpls_multi = 0x8848,
  pppoe_discover = 0x8863,
  pppoe_session = 0x8864,
  intel = 0x886D,
  jumbo_frames = 0x8870,
  homeplug_1 = 0x887B,
  ieee_802_1x = 0x888E,
  profinet = 0x8892,
  hyperscsi = 0x889A,
  ata = 0x88A2,
  ethercat = 0x88A4,
  ieee_802_1ad = 0x88A8,
  powerlink = 0x88AB,
  goose = 0x88B8,
  gse = 0x88B9,
  sv = 0x88BA,
  lldp = 0x88CC,
  sercos_iii = 0x88CD,
  wsmp = 0x88DC,
  homeplug_av = 0x88E1,
  mrp = 0x88E3,
  ieee_802_1ae = 0x88E5,
  pbb = 0x88E7,
  ptp = 0x88F7,
  nc_si = 0x88F8,
  prp = 0x88FB,
  ieee_802_1ag = 0x8902,
  fcoe = 0x8906,
  fcoe_init = 0x8914,
  roce = 0x8915,
  tte = 0x891D,
  hsr = 0x892F,
  conf_testing = 0x9000,
  ieee_802_1q_db = 0x9100,
  llt = 0xCAFE,
};

/// Interprets two bytes into an EtherType.
/// @param octets The two octets representing the EtherType.
/// @returns The `ether_type` instance for *octets*.
/// @relates ether_type
auto as_ether_type(std::span<const std::byte, 2> octets) -> ether_type;

} // namespace tenzir

namespace fmt {

template <>
struct formatter<tenzir::ether_type> : formatter<string_view> {
  template <class FormatContext>
  constexpr auto format(const tenzir::ether_type& x, FormatContext& ctx)
    -> decltype(ctx.out()) {
    auto name = string_view{"unknown"};
    switch (x) {
      case tenzir::ether_type::invalid:
        name = "invalid";
        break;
      case tenzir::ether_type::ipv4:
        name = "ipv4";
        break;
      case tenzir::ether_type::arp:
        name = "arp";
        break;
      case tenzir::ether_type::wol:
        name = "wol";
        break;
      case tenzir::ether_type::avtp:
        name = "avtp";
        break;
      case tenzir::ether_type::trill:
        name = "trill";
        break;
      case tenzir::ether_type::srp:
        name = "srp";
        break;
      case tenzir::ether_type::dec_mop_rc:
        name = "dec_mop_rc";
        break;
      case tenzir::ether_type::decnet:
        name = "decnet";
        break;
      case tenzir::ether_type::dec_lat:
        name = "dec_lat";
        break;
      case tenzir::ether_type::rarp:
        name = "rarp";
        break;
      case tenzir::ether_type::apple_talk:
        name = "apple_talk";
        break;
      case tenzir::ether_type::aarp:
        name = "aarp";
        break;
      case tenzir::ether_type::ieee_802_1aq:
        name = "ieee_802_1aq";
        break;
      case tenzir::ether_type::slpp:
        name = "slpp";
        break;
      case tenzir::ether_type::ipx:
        name = "ipx";
        break;
      case tenzir::ether_type::qnx:
        name = "qnx";
        break;
      case tenzir::ether_type::ipv6:
        name = "ipv6";
        break;
      case tenzir::ether_type::flow_control:
        name = "flow_control";
        break;
      case tenzir::ether_type::slow_protocols:
        name = "slow_protocols";
        break;
      case tenzir::ether_type::cobra_net:
        name = "cobra_net";
        break;
      case tenzir::ether_type::mpls_uni:
        name = "mpls_uni";
        break;
      case tenzir::ether_type::mpls_multi:
        name = "mpls_multi";
        break;
      case tenzir::ether_type::pppoe_discover:
        name = "pppoe_discover";
        break;
      case tenzir::ether_type::pppoe_session:
        name = "pppoe_session";
        break;
      case tenzir::ether_type::intel:
        name = "intel";
        break;
      case tenzir::ether_type::jumbo_frames:
        name = "jumbo_frames";
        break;
      case tenzir::ether_type::homeplug_1:
        name = "homeplug_1";
        break;
      case tenzir::ether_type::ieee_802_1x:
        name = "ieee_802_1x";
        break;
      case tenzir::ether_type::profinet:
        name = "profinet";
        break;
      case tenzir::ether_type::hyperscsi:
        name = "hyperscsi";
        break;
      case tenzir::ether_type::ata:
        name = "ata";
        break;
      case tenzir::ether_type::ethercat:
        name = "ethercat";
        break;
      case tenzir::ether_type::ieee_802_1ad:
        name = "ieee_802_1ad";
        break;
      case tenzir::ether_type::powerlink:
        name = "powerlink";
        break;
      case tenzir::ether_type::goose:
        name = "goose";
        break;
      case tenzir::ether_type::gse:
        name = "gse";
        break;
      case tenzir::ether_type::sv:
        name = "sv";
        break;
      case tenzir::ether_type::lldp:
        name = "lldp";
        break;
      case tenzir::ether_type::sercos_iii:
        name = "sercos_iii";
        break;
      case tenzir::ether_type::wsmp:
        name = "wsmp";
        break;
      case tenzir::ether_type::homeplug_av:
        name = "homeplug_av";
        break;
      case tenzir::ether_type::mrp:
        name = "mrp";
        break;
      case tenzir::ether_type::ieee_802_1ae:
        name = "ieee_802_1ae";
        break;
      case tenzir::ether_type::pbb:
        name = "pbb";
        break;
      case tenzir::ether_type::ptp:
        name = "ptp";
        break;
      case tenzir::ether_type::nc_si:
        name = "nc_si";
        break;
      case tenzir::ether_type::prp:
        name = "prp";
        break;
      case tenzir::ether_type::ieee_802_1ag:
        name = "ieee_802_1ag";
        break;
      case tenzir::ether_type::fcoe:
        name = "fcoe";
        break;
      case tenzir::ether_type::fcoe_init:
        name = "fcoe_init";
        break;
      case tenzir::ether_type::roce:
        name = "roce";
        break;
      case tenzir::ether_type::tte:
        name = "tte";
        break;
      case tenzir::ether_type::hsr:
        name = "hsr";
        break;
      case tenzir::ether_type::conf_testing:
        name = "conf_testing";
        break;
      case tenzir::ether_type::ieee_802_1q_db:
        name = "ieee_802_1q_db";
        break;
      case tenzir::ether_type::llt:
        name = "llt";
        break;
    }
    return formatter<string_view>::format(name, ctx);
  }
};

} // namespace fmt
