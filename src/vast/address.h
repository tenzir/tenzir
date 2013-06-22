#ifndef VAST_ADDRESS_H
#define VAST_ADDRESS_H

#include <array>
#include <string>
#include "vast/fwd.h"
#include "vast/util/operators.h"

namespace vast {

// Forward declaration
class string;

/// An IP address.
class address : util::totally_ordered<address>, util::bitwise<address>
{
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

  /// Constructs an empty address.
  address();

  /// Copies another address into this instance.
  /// @param other The address to copy.
  address(address const& other);

  /// Moves another address.
  /// @param other The address to move.
  address(address&& other);

  /// Assigns another address to this instance.
  /// @param other The right-hand side of the assignment.
  address& operator=(address other);

  /// Constructs an address from a raw bytes.
  ///
  /// @param bytes A pointer to the raw byte representation. This must point
  /// to 4 bytes if *fam* is `ipv4`, and to 16 bytes if *fam* is `ipv6`.
  ///
  /// @param fam The address family.
  ///
  /// @param order The byte order in which the address pointed to by *bytes*
  /// is stored in.
  address(uint32_t const* bytes, family fam, byte_order order);

  /// Constructs an address from a C string.
  ///
  /// @param start The string holding and IPv4 or IPv6 address.
  address(char const* str);

  /// Constructs an address from a C++ string.
  ///
  /// @param str The string holding and IPv4 or IPv6 address.
  address(std::string const& str);

  /// Constructs an address from a VAST++ string.
  ///
  /// @param str The string holding and IPv4 or IPv6 address.
  address(string const& str);

  /// Determines whether the address is IPv4.
  ///
  /// @return @c true iff the address is an IPv4 address.
  bool is_v4() const;

  /// Determines whether the address is IPv4.
  ///
  /// @return `true` iff the address is an IPv4 address.
  bool is_v6() const;

  /// Determines whether the address is an IPv4 loopback address.
  ///
  /// @return `true` if the address is v4 and its first byte has the
  /// value 127.
  bool is_loopback() const;

  /// Determines whether the address is an IPv4 broadcast address.
  ///
  /// @return `true` if the address is v4 and has the value 255.255.255.255.
  bool is_broadcast() const;

  /// Determines whether the address is a multicast address. For v4
  /// addresses, this means the first byte equals to 224. For v6 addresses,
  /// this means the first bytes equals 255.
  ///
  /// @return `true` if the address is a multicast address.
  bool is_multicast() const;

  /// Masks out lower bits of the address.
  ///
  /// @param top_bits_to_keep The number of bits *not* to mask out,
  /// counting from the highest order bit. The value is always
  /// interpreted relative to the IPv6 bit width, even if the address
  /// is IPv4. That means if we compute 192.168.1.2/16, we need to pass in
  /// 112 (i.e., 96 + 16). The value must be in the range from 0 to 128.
  void mask(unsigned top_bits_to_keep);

  /// AND's another address to this instance.
  /// @param other The other address.
  /// @return A reference to `*this`.
  address& operator&=(address const& other);

  /// OR's another address to this instance.
  /// @param other The other address.
  /// @return A reference to `*this`.
  address& operator|=(address const& other);

  /// XOR's another address to this instance.
  /// @param other The other address.
  /// @return A reference to `*this`.
  address& operator^=(address const& other);

  /// Retrieves the underlying byte array.
  /// @return A reference to an array of 16 bytes.
  std::array<uint8_t, 16> const& data() const;

private:
  void from_v4(char const* str);
  void from_v6(char const* str);

  friend access;
  void serialize(serializer& sink);
  void deserialize(deserializer& source);

  friend bool operator==(address const& x, address const& y);
  friend bool operator<(address const& x, address const& y);

  friend std::string to_string(address const& a);

  std::array<uint8_t, 16> bytes_;
};

std::ostream& operator<<(std::ostream& out, address const& addr);

} // namespace vast

#endif
