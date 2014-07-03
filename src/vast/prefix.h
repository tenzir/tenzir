#ifndef VAST_PREFIX_H
#define VAST_PREFIX_H

#include "vast/address.h"
#include "vast/parse.h"
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
  bool initialize();

  address network_;
  uint8_t length_;

private:
  friend access;

  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  template <typename Iterator>
  friend trial<void> print(prefix const& p, Iterator&& out)
  {
    auto t = print(p.network_, out);
    if (! t)
      return t.error();

    *out++ = '/';

    return print(p.length(), out);
  }

  template <typename Iterator>
  friend trial<void> parse(prefix& pfx, Iterator& begin, Iterator end)
  {
    char buf[64];
    auto p = buf;
    while (*begin != '/' && begin != end && p < &buf[63])
      *p++ = *begin++;
    *p = '\0';

    auto lval = buf;
    auto t = parse(pfx.network_, lval, p);
    if (! t)
      return t.error();

    if (*begin++ != '/')
      return error{"missing / in:", buf};

    p = buf;
    while (begin != end && p < &buf[3])
      *p++ = *begin++;
    *p = '\0';

    lval = buf;
    t = parse(pfx.length_, lval, p);
    if (! t)
      return t.error();

    if (! pfx.initialize())
      return error{"invalid parameters"};

    return nothing;
  }

  friend bool operator==(prefix const& x, prefix const& y);
  friend bool operator<(prefix const& x, prefix const& y);
};

trial<void> convert(prefix const& p, util::json& j);

} // namespace vast

#endif
