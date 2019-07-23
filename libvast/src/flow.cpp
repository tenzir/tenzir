/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/flow.hpp"

#include "vast/concept/hashable/uhash.hpp"
#include "vast/concept/hashable/xxhash.hpp"
#include "vast/concept/parseable/vast/address.hpp"
#include "vast/detail/assert.hpp"

namespace vast {

caf::optional<flow> make_flow(std::string_view src_addr,
                              std::string_view dst_addr, uint16_t src_port,
                              uint16_t dst_port, port::port_type protocol) {
  using parsers::addr;
  flow result;
  if (!addr(src_addr, result.src_addr) || !addr(dst_addr, result.dst_addr))
    return caf::none;
  result.src_port = port{src_port, protocol};
  result.dst_port = port{dst_port, protocol};
  return result;
}

bool operator==(const flow& x, const flow& y) {
  return x.src_addr == y.src_addr && x.dst_addr == y.dst_addr
         && x.src_port == y.src_port && x.dst_port == y.dst_port;
}

port::port_type protocol(const flow& x) {
  VAST_ASSERT(x.src_port.type() == x.dst_port.type());
  return x.src_port.type();
}

size_t hash(const flow& x) {
  vast::uhash<vast::xxhash> f;
  return f(x);
}

} // namespace vast
