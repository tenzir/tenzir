//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/concept/printable/vast/subnet.hpp"

#include "vast/concept/printable/to_string.hpp"
#include "vast/data.hpp"
#include "vast/subnet.hpp"

#include <tuple>

namespace vast {

subnet::subnet() : length_{0u} {
}

subnet::subnet(ip addr, uint8_t length) : network_{addr}, length_{length} {
  if (!initialize()) {
    network_ = ip{};
    length_ = 0;
  }
}

bool subnet::contains(const ip& addr) const {
  return addr.compare(network_, length_);
}

bool subnet::contains(const subnet& other) const {
  return length_ <= other.length_ && contains(other.network_);
}

const ip& subnet::network() const {
  return network_;
}

uint8_t subnet::length() const {
  return length_;
}

bool subnet::initialize() {
  if (length_ > 128) {
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

bool convert(const subnet& sn, data& d) {
  d = to_string(sn);
  return true;
}

} // namespace vast
