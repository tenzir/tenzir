#ifndef VAST_SUBNET_HPP
#define VAST_SUBNET_HPP

#include "vast/address.hpp"
#include "vast/util/operators.hpp"

namespace vast {

/// Stores IPv4 and IPv6 prefixes, e.g., `192.168.1.1/16` and `FD00::/8`.
class subnet : util::totally_ordered<subnet> {
  friend access;

public:
  /// Constructs the empty prefix, i.e., `::/0`.
  subnet();

  /// Constructs a prefix from an address.
  /// @param addr The address.
  /// @param length The prefix length.
  subnet(address addr, uint8_t length);

  /// Checks whether this prefix includes a given address.
  /// @param addr The address to test for .
  bool contains(address const& addr) const;

  /// Retrieves the network address of the prefix.
  /// @returns The prefix address.
  address const& network() const;

  /// Retrieves the prefix length.
  /// @returns The prefix length.
  uint8_t length() const;

  friend bool operator==(subnet const& x, subnet const& y);
  friend bool operator<(subnet const& x, subnet const& y);

  template <class Inspector>
  friend auto inspect(Inspector& f, subnet& sn) {
    return f(sn.network_, sn.length_);
  }

private:
  bool initialize();

  address network_;
  uint8_t length_;
};

} // namespace vast

#endif
