#ifndef VAST_PREFIX_H
#define VAST_PREFIX_H

#include "vast/address.h"
#include "vast/util/operators.h"

namespace vast {

/// Stores IPv4 and IPv6 prefixes, e.g., @c 192.168.1.1/16 and @c FD00::/8.
class prefix : util::totally_ordered<prefix>
{
public:
  /// Constructs the empty prefix, i.e., @c ::/0.
  prefix();

  /// Constructs a prefix from an address.
  /// @param addr The address.
  /// @param length The prefix length.
  prefix(address addr, uint8_t length);

  /// Constructs a prefix from another one.
  /// @param other The prefix to copy.
  prefix(prefix const& other);

  /// Moves a prefix.
  /// @param other The prefix to move.
  prefix(prefix&& other);

  /// Assigns another prefix to this instance.
  /// @param other The right-hand side of the assignment.
  prefix& operator=(prefix other);

  /// Checks whether this prefix includes a given address.
  /// @param addr The address to test for .
  bool contains(address const& addr) const;

  /// Retrieves the network address of the prefix.
  /// @return The prefix address.
  address const& network() const;

  /// Retrieves the prefix length.
  /// @return The prefix length.
  uint8_t length() const;

private:
  void initialize();

  friend io::access;
  void serialize(io::serializer& sink);
  void deserialize(io::deserializer& source);

  address network_;
  uint8_t length_;
};

bool operator==(prefix const& x, prefix const& y);
bool operator<(prefix const& x, prefix const& y);

std::string to_string(prefix const& p);
std::ostream& operator<<(std::ostream& out, prefix const& pfx);

} // namespace vast

#endif
