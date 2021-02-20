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

#include "vast/ether_type.hpp"

#include "vast/detail/byte_swap.hpp"

#include <cstddef>
#include <utility>

namespace vast {

ether_type as_ether_type(span<const std::byte, 2> octets) {
  auto ptr = reinterpret_cast<const uint16_t*>(std::launder(octets.data()));
  return static_cast<ether_type>(detail::to_host_order(*ptr));
}

} // namespace vast
