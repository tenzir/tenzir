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

#pragma once

#include "vast/address.hpp"
#include "vast/detail/operators.hpp"

namespace vast {

class json;

/// Stores IPv4 and IPv6 prefixes, e.g., `192.168.1.1/16` and `FD00::/8`.
class subnet : detail::totally_ordered<subnet> {
  friend access;

public:
  /// Constructs the empty prefix, i.e., `::/0`.
  subnet();

  /// Constructs a prefix from an address.
  /// @param addr The address.
  /// @param length The prefix length.
  subnet(address addr, uint8_t length);

  /// Checks whether this subnet includes a given address.
  /// @param addr The address to test for containment.
  /// @returns `true` if *addr* is an element of this subnet.
  bool contains(const address& addr) const;

  /// Checks whether this subnet includes another subnet.
  /// For two subnets *A* and *B*, the subset relationship *A ⊆ B* holds true
  /// if all hosts of A are also part of B. This is true if (1) *A*'s prefix
  /// length is less than or equal to *B*'s, and (2) if the host address of *A*
  /// and *B* are equal in the first *k* bits, where *k* is the prefix length
  /// of *A*.
  /// @param other The subnet to test for containment.
  /// @returns `true` if *other* is contained within this subnet.
  bool contains(const subnet& other) const;

  /// Retrieves the network address of the prefix.
  /// @returns The prefix address.
  const address& network() const;

  /// Retrieves the prefix length.
  /// @returns The prefix length.
  uint8_t length() const;

  friend bool operator==(const subnet& x, const subnet& y);
  friend bool operator<(const subnet& x, const subnet& y);

  template <class Inspector>
  friend auto inspect(Inspector& f, subnet& sn) {
    return f(sn.network_, sn.length_);
  }

  friend bool convert(const subnet& sn, json& j);

private:
  bool initialize();

  address network_;
  uint8_t length_;
};

} // namespace vast

