//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

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
