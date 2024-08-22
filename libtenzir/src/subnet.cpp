//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/subnet.hpp"

#include "tenzir/concept/printable/tenzir/subnet.hpp"
#include "tenzir/concept/printable/to_string.hpp"

#include <tuple>

namespace tenzir {

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

bool subnet::debug(debug_writer& f) {
  return f.fmt_value("{}", to_string(*this));
}

bool operator==(const subnet& x, const subnet& y) {
  return x.network_ == y.network_ && x.length_ == y.length_;
}

bool operator<(const subnet& x, const subnet& y) {
  return std::tie(x.network_, x.length_) < std::tie(y.network_, y.length_);
}

} // namespace tenzir
