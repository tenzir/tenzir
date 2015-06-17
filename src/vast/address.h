#ifndef VAST_ADDRESS_H
#define VAST_ADDRESS_H

#include <array>
#include <string>
#include "vast/fwd.h"
#include "vast/print.h"
#include "vast/util/operators.h"

namespace vast {

struct access;

/// An IP address.
class address : util::totally_ordered<address>,
                util::bitwise<address>
{
  friend access;

  /// Top 96 bits of v4-mapped-addr.
  static std::array<uint8_t, 12> const v4_mapped_prefix;

public:
  /// Address family.
  enum family
  {
    ipv4,
    ipv6
  };

  /// Address byte order.
  enum byte_order
  {
    host,
    network
  };

  /// Default-constructs an (invalid) address.
  address();

  /// Constructs an address from raw bytes.
  /// @param bytes A pointer to the raw byte representation. This must point
  ///              to 4 bytes if *fam* is `ipv4`, and to 16 bytes if *fam* is
  ///              `ipv6`.
  /// @param fam The address family.
  /// @param order The byte order in which the address pointed to by *bytes*
  ///              is stored in.
  address(uint32_t const* bytes, family fam, byte_order order);

  friend bool operator==(address const& x, address const& y);
  friend bool operator<(address const& x, address const& y);

  /// Determines whether the address is IPv4.
  /// @returns @c true iff the address is an IPv4 address.
  bool is_v4() const;

  /// Determines whether the address is IPv4.
  /// @returns `true` iff the address is an IPv4 address.
  bool is_v6() const;

  /// Determines whether the address is an IPv4 loopback address.
  /// @returns `true` if the address is v4 and its first byte has the
  /// value 127.
  bool is_loopback() const;

  /// Determines whether the address is an IPv4 broadcast address.
  /// @returns `true` if the address is v4 and has the value 255.255.255.255.
  bool is_broadcast() const;

  /// Determines whether the address is a multicast address. For v4
  /// addresses, this means the first byte equals to 224. For v6 addresses,
  /// this means the first bytes equals 255.
  /// @returns `true` if the address is a multicast address.
  bool is_multicast() const;

  /// Masks out lower bits of the address.
  /// @param top_bits_to_keep The number of bits *not* to mask out,
  ///                         counting from the highest order bit. The value is
  ///                         always interpreted relative to the IPv6 bit
  ///                         width, even if the address is IPv4. That means if
  ///                         we compute 192.168.1.2/16, we need to pass in
  ///                         112 (i.e., 96 + 16). The value must be in the
  ///                         range from 0 to 128.
  /// @returns `true` on success.
  bool mask(unsigned top_bits_to_keep);

  /// AND's another address to this instance.
  /// @param other The other address.
  /// @returns A reference to `*this`.
  address& operator&=(address const& other);

  /// OR's another address to this instance.
  /// @param other The other address.
  /// @returns A reference to `*this`.
  address& operator|=(address const& other);

  /// XOR's another address to this instance.
  /// @param other The other address.
  /// @returns A reference to `*this`.
  address& operator^=(address const& other);

  /// Retrieves the underlying byte array.
  /// @returns A reference to an array of 16 bytes.
  std::array<uint8_t, 16> const& data() const;

  template <typename Iterator>
  friend trial<void> print(address const& a, Iterator&& out)
  {
    return print(to_string(a), out);
  }

  friend std::string to_string(address const& a);

private:
  std::array<uint8_t, 16> bytes_;
};

trial<void> convert(address const& a, util::json& j);

} // namespace vast

#include "vast/concept/parseable/core/parse.h"
#include "vast/concept/parseable/vast/address.h"

namespace vast {

// TODO: remove after conversion to new parseable concept.
template <typename Iterator>
trial<void> parse(address& a, Iterator& begin, Iterator end)
{
  using vast::parse;
  if (parse(begin, end, a))
    return nothing;
  else
    return error{"failed to parse address", std::string{begin, end}};
}

} // namespace vast

#endif
