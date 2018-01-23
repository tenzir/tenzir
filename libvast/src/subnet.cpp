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

#include <tuple>

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/subnet.hpp"
#include "vast/json.hpp"
#include "vast/subnet.hpp"

namespace vast {

subnet::subnet() : length_{0u} {
}

subnet::subnet(address addr, uint8_t length)
  : network_{std::move(addr)}, length_{length} {
  if (!initialize()) {
    network_ = address{};
    length_ = 0;
  }
}

bool subnet::contains(const address& addr) const {
  return addr.compare(network_, length_);
}

bool subnet::contains(const subnet& other) const {
  return length_ <= other.length_ && contains(other.network_);
}

const address& subnet::network() const {
  return network_;
}

uint8_t subnet::length() const {
  return network_.is_v4() ? length_ - 96 : length_;
}

bool subnet::initialize() {
  if (network_.is_v4()) {
    if (length_ > 32)
      return false;
    length_ += 96;
  } else if (length_ > 128) {
    return false;
  }
  network_.mask(length_);
  return true;
}

bool operator==(const subnet& x, const subnet& y) {
  return x.network_ == y.network_ && x.length_ == y.length_;
}

bool operator<(const subnet& x, const subnet& y) {
  return std::tie(x.network_, x.length_) < std::tie(y.network_, y.length_);
}

bool convert(const subnet& sn, json& j) {
  j = to_string(sn);
  return true;
}

} // namespace vast
