#ifndef VAST_DETAIL_PACKET_TYPE_H
#define VAST_DETAIL_PACKET_TYPE_H

#include "vast/type.h"

namespace vast {
namespace detail {

/// Creates the type for packets in VAST.
inline type make_packet_type()
{
  auto packet = type::record{
    {"meta", type::record{
      {"network", type::record{
        {"ttl", type::count{}},
        {"src", type::address{}},
        {"dst", type::address{}}}},
      {"transport", type::record{
        {"tcp", type::record{
          {"sport", type::port{}},
          {"dport", type::port{}}}},
        {"udp", type::record{
          {"sport", type::port{}},
          {"dport", type::port{}}}},
        {"icmp", type::record{
          {"type", type::count{}},
          {"code", type::count{}}}}}}}},
    {"data", type::string{}}};

  packet.name("vast::packet");

  return packet;
};

} // namespace detail
} // namespace vast

#endif
