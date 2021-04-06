//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/address.hpp"
#include "vast/detail/operators.hpp"

#include <fmt/format.h>

namespace vast {

class data;

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

  friend bool convert(const subnet& sn, data& d);

private:
  bool initialize();

  address network_;
  uint8_t length_;
};

} // namespace vast

namespace fmt {

template <>
struct formatter<vast::subnet> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return std::end(ctx);
  }

  template <typename FormatContext>
  auto format(const vast::subnet& sn, FormatContext& ctx) {
    // This is a workaround for bug in fmt/6.2.1 which fails with
    // an idiomatic approach that would be something like this:
    // ```
    //   return format_t(ctx.out(), {}/{}, sn.network(), sn.length());
    // ```
    // See details with a reproduction here
    // https://github.com/fmtlib/fmt/issues/2214
    auto out = ctx.out();
    out = format_to(out, "{}", sn.network());
    out = format_to(out, "/{}", sn.length());
    return out;
  }
};

} // namespace fmt
