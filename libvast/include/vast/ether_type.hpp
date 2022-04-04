//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <fmt/format.h>

#include <cstddef>
#include <cstdint>
#include <span>

namespace vast {

/// The two-octet field in the an Ethernet frame indicating the payload type.
/// Directly copied from https://en.wikipedia.org/wiki/EtherType on 2019/11/16.
enum class ether_type : uint16_t {
  ipv4 = 0x0800,           ///< Internet Protocol version 4
  arp = 0x0806,            ///< Address Resolution Protocol (ARP)
  wol = 0x0842,            ///< Wake-on-LAN
  avtp = 0x22F0,           ///< Audio Video Transport Protocol (AVTP)
  trill = 0x22F3,          ///< IETF TRILL Protocol
  srp = 0x22EA,            ///< Stream Reservation Protocol
  dec_mop_rc = 0x6002,     ///< DEC MOP RC
  decnet = 0x6003,         ///< DECnet Phase IV, DNA Routing
  dec_lat = 0x6004,        ///< DEC LAT
  rarp = 0x8035,           ///< Reverse Address Resolution Protocol (RARP)
  apple_talk = 0x809B,     ///< AppleTalk (Ethertalk)
  aarp = 0x80F3,           ///< AppleTalk Address Resolution Protocol (AARP)
  ieee_802_1aq = 0x8100,   ///< VLAN-tagged frame (IEEE 802.1Q) and IEEE 802.1aq
  slpp = 0x8102,           ///< Simple Loop Prevention Protocol (SLPP)
  ipx = 0x8137,            ///< IPX
  qnx = 0x8204,            ///< QNX Qnet
  ipv6 = 0x86DD,           ///< Internet Protocol Version 6 (IPv6)
  flow_control = 0x8808,   ///< Ethernet flow control
  slow_protocols = 0x8809, ///< Ethernet Slow Protocols
  cobra_net = 0x8819,      ///< CobraNet
  mpls_uni = 0x8847,       ///< MPLS unicast
  mpls_multi = 0x8848,     ///< MPLS multicast
  pppoe_discover = 0x8863, ///< PPPoE Discovery Stage
  pppoe_session = 0x8864,  ///< PPPoE Session Stage
  intel = 0x886D,          ///< Intel Advanced Networking Services
  jumbo_frames = 0x8870,   ///< Jumbo Frames
  homeplug_1 = 0x887B,     ///< HomePlug 1.0 MME
  ieee_802_1x = 0x888E,    ///< EAP over LAN (IEEE 802.1X)
  profinet = 0x8892,       ///< PROFINET Protocol
  hyperscsi = 0x889A,      ///< HyperSCSI (SCSI over Ethernet)
  ata = 0x88A2,            ///< ATA over Ethernet
  ethercat = 0x88A4,       ///< EtherCAT Protocol
  ieee_802_1ad = 0x88A8,   ///< Provider Bridging (IEEE 802.1ad) & IEEE 802.1aq
  powerlink = 0x88AB,      ///< Ethernet Powerlink
  goose = 0x88B8,          ///< GOOSE (Generic Object Oriented Substation event)
  gse = 0x88B9,            ///< Generic Substation Events Management Services
  sv = 0x88BA,             ///< SV (Sampled Value Transmission)
  lldp = 0x88CC,           ///< Link Layer Discovery Protocol (LLDP)
  sercos_iii = 0x88CD,     ///< SERCOS III
  wsmp = 0x88DC,           ///< WSMP, WAVE Short Message Protocol
  homeplug_av = 0x88E1,    ///< HomePlug AV MME
  mrp = 0x88E3,            ///< Media Redundancy Protocol (IEC62439-2)
  ieee_802_1ae = 0x88E5,   ///< MAC security (IEEE 802.1AE)
  pbb = 0x88E7,            ///< Provider Backbone Bridges (PBB) (IEEE 802.1ah)
  ptp = 0x88F7,            ///< Precision Time Protocol over Ethernet
  nc_si = 0x88F8,          ///< NC-SI
  prp = 0x88FB,            ///< Parallel Redundancy Protocol (PRP)
  ieee_802_1ag = 0x8902,   ///< IEEE 802.1ag Connectivity Fault Management (CFM)
  fcoe = 0x8906,           ///< Fibre Channel over Ethernet (FCoE)
  fcoe_init = 0x8914,      ///< FCoE Initialization Protocol
  roce = 0x8915,           ///< RDMA over Converged Ethernet (RoCE)
  tte = 0x891D,            ///< TTEthernet Protocol Control Frame (TTE)
  hsr = 0x892F,            ///< High-availability Seamless Redundancy (HSR)
  conf_testing = 0x9000,   ///< Ethernet Configuration Testing Protocol[13]
  ieee_802_1q_db = 0x9100, ///< VLAN (IEEE 802.1Q) frame w/ double tagging
  llt = 0xCAFE,            ///< Veritas Technologies Low Latency Transport (LLT)
};

/// Interprets two bytes into an EtherType.
/// @param octets The two octets representing the EtherType.
/// @returns The `ether_type` instance for *octects*.
/// @relates ether_type
ether_type as_ether_type(std::span<const std::byte, 2> octets);

} // namespace vast

namespace fmt {

template <>
struct formatter<vast::ether_type> : formatter<string_view> {
  using super = formatter<string_view>;

  template <class FormatContext>
  constexpr auto format(const vast::ether_type& x, FormatContext& ctx) const
    -> decltype(ctx.out()) {
    switch (x) {
      case vast::ether_type::ipv4:
        return super::format("Internet Protocol version 4", ctx);
      case vast::ether_type::arp:
        return super::format("Address Resolution Protocol (ARP)", ctx);
      case vast::ether_type::wol:
        return super::format("Wake-on-LAN", ctx);
      case vast::ether_type::avtp:
        return super::format("Audio Video Transport Protocol (AVTP)", ctx);
      case vast::ether_type::trill:
        return super::format("IETF TRILL Protocol", ctx);
      case vast::ether_type::srp:
        return super::format("Stream Reservation Protocol", ctx);
      case vast::ether_type::dec_mop_rc:
        return super::format("DEC MOP RC", ctx);
      case vast::ether_type::decnet:
        return super::format("DECnet Phase IV, DNA Routing", ctx);
      case vast::ether_type::dec_lat:
        return super::format("DEC LAT", ctx);
      case vast::ether_type::rarp:
        return super::format("Reverse Address Resolution Protocol (RARP)", ctx);
      case vast::ether_type::apple_talk:
        return super::format("AppleTalk (Ethertalk)", ctx);
      case vast::ether_type::aarp:
        return super::format("AppleTalk Address Resolution Protocol (AARP)",
                             ctx);
      case vast::ether_type::ieee_802_1aq:
        return super::format("VLAN-tagged frame (IEEE 802.1Q) and IEEE 802.1aq",
                             ctx);
      case vast::ether_type::slpp:
        return super::format("Simple Loop Prevention Protocol (SLPP)", ctx);
      case vast::ether_type::ipx:
        return super::format("IPX", ctx);
      case vast::ether_type::qnx:
        return super::format("QNX Qnet", ctx);
      case vast::ether_type::ipv6:
        return super::format("Internet Protocol Version 6 (IPv6)", ctx);
      case vast::ether_type::flow_control:
        return super::format("Ethernet flow control", ctx);
      case vast::ether_type::slow_protocols:
        return super::format("Ethernet Slow Protocols", ctx);
      case vast::ether_type::cobra_net:
        return super::format("CobraNet", ctx);
      case vast::ether_type::mpls_uni:
        return super::format("MPLS unicast", ctx);
      case vast::ether_type::mpls_multi:
        return super::format("MPLS multicast", ctx);
      case vast::ether_type::pppoe_discover:
        return super::format("PPPoE Discovery Stage", ctx);
      case vast::ether_type::pppoe_session:
        return super::format("PPPoE Session Stage", ctx);
      case vast::ether_type::intel:
        return super::format("Intel Advanced Networking Services", ctx);
      case vast::ether_type::jumbo_frames:
        return super::format("Jumbo Frames", ctx);
      case vast::ether_type::homeplug_1:
        return super::format("HomePlug 1.0 MME", ctx);
      case vast::ether_type::ieee_802_1x:
        return super::format("EAP over LAN (IEEE 802.1X)", ctx);
      case vast::ether_type::profinet:
        return super::format("PROFINET Protocol", ctx);
      case vast::ether_type::hyperscsi:
        return super::format("HyperSCSI (SCSI over Ethernet)", ctx);
      case vast::ether_type::ata:
        return super::format("ATA over Ethernet", ctx);
      case vast::ether_type::ethercat:
        return super::format("EtherCAT Protocol", ctx);
      case vast::ether_type::ieee_802_1ad:
        return super::format("Provider Bridging (IEEE 802.1ad) & IEEE 802.1aq",
                             ctx);
      case vast::ether_type::powerlink:
        return super::format("Ethernet Powerlink", ctx);
      case vast::ether_type::goose:
        return super::format("GOOSE (Generic Object Oriented Substation event)",
                             ctx);
      case vast::ether_type::gse:
        return super::format("Generic Substation Events Management Services",
                             ctx);
      case vast::ether_type::sv:
        return super::format("SV (Sampled Value Transmission)", ctx);
      case vast::ether_type::lldp:
        return super::format("Link Layer Discovery Protocol (LLDP)", ctx);
      case vast::ether_type::sercos_iii:
        return super::format("SERCOS III", ctx);
      case vast::ether_type::wsmp:
        return super::format("WSMP, WAVE Short Message Protocol", ctx);
      case vast::ether_type::homeplug_av:
        return super::format("HomePlug AV MME", ctx);
      case vast::ether_type::mrp:
        return super::format("Media Redundancy Protocol (IEC62439-2)", ctx);
      case vast::ether_type::ieee_802_1ae:
        return super::format("MAC security (IEEE 802.1AE)", ctx);
      case vast::ether_type::pbb:
        return super::format("Provider Backbone Bridges (PBB) (IEEE 802.1ah)",
                             ctx);
      case vast::ether_type::ptp:
        return super::format("Precision Time Protocol over Ethernet", ctx);
      case vast::ether_type::nc_si:
        return super::format("NC-SI", ctx);
      case vast::ether_type::prp:
        return super::format("Parallel Redundancy Protocol (PRP)", ctx);
      case vast::ether_type::ieee_802_1ag:
        return super::format("IEEE 802.1ag Connectivity Fault Management (CFM)",
                             ctx);
      case vast::ether_type::fcoe:
        return super::format("Fibre Channel over Ethernet (FCoE)", ctx);
      case vast::ether_type::fcoe_init:
        return super::format("FCoE Initialization Protocol", ctx);
      case vast::ether_type::roce:
        return super::format("RDMA over Converged Ethernet (RoCE)", ctx);
      case vast::ether_type::tte:
        return super::format("TTEthernet Protocol Control Frame (TTE)", ctx);
      case vast::ether_type::hsr:
        return super::format("High-availability Seamless Redundancy (HSR)",
                             ctx);
      case vast::ether_type::conf_testing:
        return super::format("Ethernet Configuration Testing Protocol[13]",
                             ctx);
      case vast::ether_type::ieee_802_1q_db:
        return super::format("VLAN (IEEE 802.1Q) frame w/ double tagging", ctx);
      case vast::ether_type::llt:
        return super::format("Veritas Technologies Low Latency Transport (LLT)",
                             ctx);
    }
    return super::format("unknown", ctx);
  }
};

} // namespace fmt
