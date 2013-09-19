#ifndef VAST_PREFIX_H
#define VAST_PREFIX_H

#include "vast/address.h"
#include "vast/util/operators.h"

namespace vast {

/// Stores IPv4 and IPv6 prefixes, e.g., @c 192.168.1.1/16 and @c FD00::/8.
class prefix : util::totally_ordered<prefix>,
               util::parsable<prefix>,
               util::printable<prefix>
{
public:

  /// Constructs the empty prefix, i.e., @c ::/0.
  prefix();

  /// Constructs a prefix from an address.
  /// @param addr The address.
  /// @param length The prefix length.
  prefix(address addr, uint8_t length);

  prefix(prefix const& other) = default;
  prefix(prefix&& other);
  prefix& operator=(prefix&&) = default;
  prefix& operator=(prefix const&) = default;

  /// Checks whether this prefix includes a given address.
  /// @param addr The address to test for .
  bool contains(address const& addr) const;

  /// Retrieves the network address of the prefix.
  /// @returns The prefix address.
  address const& network() const;

  /// Retrieves the prefix length.
  /// @returns The prefix length.
  uint8_t length() const;

private:
  void initialize();

  address network_;
  uint8_t length_;

private:
  friend access;

  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  template <typename Iterator>
  bool parse(Iterator& start, Iterator end)
  {
    char buf[64];
    auto p = buf;
    while (*start != '/' && start != end && p < &buf[63])
      *p++ = *start++;
    *p = '\0';

    address addr{buf};

    if (*start++ != '/')
      return false;

    p = buf;
    while (start != end && p < &buf[3])
      *p++ = *start++;
    *p = '\0';

    uint8_t length;
    auto s = buf;
    if (! util::parse_positive_decimal(s, p, length))
      return false;

    *this = {std::move(addr), length};

    return true;
  }

  template <typename Iterator>
  bool print(Iterator& out) const
  {
    if (! render(out, network_))
      return false;
    *out++ = '/';
    return render(out, length());
  }

  friend bool operator==(prefix const& x, prefix const& y);
  friend bool operator<(prefix const& x, prefix const& y);
};

} // namespace vast

#endif
