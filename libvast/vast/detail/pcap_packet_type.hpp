#ifndef VAST_DETAIL_PCAP_PACKET_TYPE_HPP
#define VAST_DETAIL_PCAP_PACKET_TYPE_HPP

#include "vast/type.hpp"

namespace vast {
namespace detail {

/// Creates the type for packets in VAST.
inline type make_packet_type() {
  auto packet = record_type{
    {"meta", record_type{
      {"src", address_type{}},
      {"dst", address_type{}},
      {"sport", port_type{}},
      {"dport", port_type{}}}},
    {"data", string_type{}.attributes({{"skip"}})}
  };
  packet.name() = "pcap::packet";
  return packet;
}

static auto const pcap_packet_type = make_packet_type();

} // namespace detail
} // namespace vast

#endif
