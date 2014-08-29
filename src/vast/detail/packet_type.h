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
      {"src", type::address{}},
      {"dst", type::address{}},
      {"sport", type::port{}},
      {"dport", type::port{}}}},
    {"data", type::string{}}};

  packet.name("vast::packet");

  return packet;
};

} // namespace detail
} // namespace vast

#endif
