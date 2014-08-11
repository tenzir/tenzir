#ifndef VAST_SUBNET_H
#define VAST_SUBNET_H

#include "vast/address.h"
#include "vast/parse.h"
#include "vast/util/operators.h"

namespace vast {

/// Stores IPv4 and IPv6 prefixes, e.g., `192.168.1.1/16` and `FD00::/8`.
class subnet : util::totally_ordered<subnet>
{
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

private:
  bool initialize();

  address network_;
  uint8_t length_;

private:
  friend access;

  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  template <typename Iterator>
  friend trial<void> print(subnet const& s, Iterator&& out)
  {
    auto t = print(s.network_, out);
    if (! t)
      return t.error();

    *out++ = '/';

    return print(s.length(), out);
  }

  template <typename Iterator>
  friend trial<void> parse(subnet& s, Iterator& begin, Iterator end)
  {
    char buf[64];
    auto p = buf;
    while (*begin != '/' && begin != end && p < &buf[63])
      *p++ = *begin++;
    *p = '\0';

    auto lval = buf;
    auto t = parse(s.network_, lval, p);
    if (! t)
      return t.error();

    if (*begin++ != '/')
      return error{"missing / in:", buf};

    p = buf;
    while (begin != end && p < &buf[3])
      *p++ = *begin++;
    *p = '\0';

    lval = buf;
    t = parse(s.length_, lval, p);
    if (! t)
      return t.error();

    if (! s.initialize())
      return error{"invalid parameters"};

    return nothing;
  }

  friend bool operator==(subnet const& x, subnet const& y);
  friend bool operator<(subnet const& x, subnet const& y);
};

trial<void> convert(subnet const& p, util::json& j);

} // namespace vast

#endif
