//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/inspection_common.hpp"
#include "vast/detail/operators.hpp"
#include "vast/ip.hpp"

namespace vast {

class data;

/// Stores IPv4 and IPv6 prefixes, e.g., `192.168.1.1/16` and `FD00::/8`.
class subnet : detail::totally_ordered<subnet> {
public:
  /// Constructs the empty prefix, i.e., `::/0`.
  subnet();

  /// Constructs a prefix from an address.
  /// @param addr The address.
  /// @param length The prefix length, as specified for IPv6 addresses.
  subnet(ip addr, uint8_t length);

  /// Checks whether this subnet includes a given address.
  /// @param addr The address to test for containment.
  /// @returns `true` if *addr* is an element of this subnet.
  [[nodiscard]] bool contains(const ip& addr) const;

  /// Checks whether this subnet includes another subnet.
  /// For two subnets *A* and *B*, the subset relationship *A ⊆ B* holds true
  /// if all hosts of A are also part of B. This is true if (1) *A*'s prefix
  /// length is less than or equal to *B*'s, and (2) if the host address of *A*
  /// and *B* are equal in the first *k* bits, where *k* is the prefix length
  /// of *A*.
  /// @param other The subnet to test for containment.
  /// @returns `true` if *other* is contained within this subnet.
  [[nodiscard]] bool contains(const subnet& other) const;

  /// Retrieves the network address of the prefix.
  /// @returns The prefix address.
  [[nodiscard]] const ip& network() const;

  /// Retrieves the prefix length.
  /// @returns The prefix length.
  [[nodiscard]] uint8_t length() const;

  friend bool operator==(const subnet& x, const subnet& y);
  friend bool operator<(const subnet& x, const subnet& y);

  template <class Inspector>
  friend auto inspect(Inspector& f, subnet& sn) {
    return detail::apply_all(f, sn.network_, sn.length_);
  }

  friend bool convert(const subnet& sn, data& d);

private:
  bool initialize();

  ip network_;
  uint8_t length_;
};

} // namespace vast
