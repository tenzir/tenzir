#ifndef VAST_ADDRESS_H
#define VAST_ADDRESS_H

#include <array>
#include <string>
#include "vast/fwd.h"
#include "vast/optional.h"
#include "vast/string.h"
#include "vast/util/operators.h"
#include "vast/util/parse.h"
#include "vast/util/print.h"

namespace vast {

/// An IP address.
class address : util::totally_ordered<address>,
                util::bitwise<address>,
                util::parsable<address>,
                util::printable<address>
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

  /// Constructs an address from a C string.
  /// @param start The string holding and IPv4 or IPv6 address.
  /// @returns An engaged option iff parsing succeeded.
  static optional<address> from_string(char const* str);

  /// Constructs an address from a C++ string.
  /// @param start The string holding and IPv4 or IPv6 address.
  /// @returns An engaged option iff parsing succeeded.
  static optional<address> from_string(std::string const& str);

  /// Constructs an address from a VAST string.
  /// @param start The string holding and IPv4 or IPv6 address.
  /// @returns An engaged option iff parsing succeeded.
  static optional<address> from_string(string const& str);

  /// Constructs an IPv4 address from a string.
  /// @param str The string holding the IPv4 address.
  static optional<address> from_v4(char const* str);

  /// Constructs an IPv6 address from a string.
  /// @param str The string holding the IPv6 address.
  static optional<address> from_v6(char const* str);

  address();

  /// Constructs an address from a C string.
  /// @param start The string holding and IPv4 or IPv6 address.
  explicit address(char const* str);

  /// Constructs an address from a C++ string.
  /// @param start The string holding and IPv4 or IPv6 address.
  /// @returns An engaged option iff parsing succeeded.
  explicit address(std::string const& str);

  /// Constructs an address from a VAST string.
  /// @param start The string holding and IPv4 or IPv6 address.
  /// @returns An engaged option iff parsing succeeded.
  explicit address(string const& str);

  address(address const&) = default;
  address(address&& other);
  address& operator=(address const&) = default;
  address& operator=(address&&) = default;

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

  /// Determines whether the address is IPv4.
  ///
  /// @returns @c true iff the address is an IPv4 address.
  bool is_v4() const;

  /// Determines whether the address is IPv4.
  ///
  /// @returns `true` iff the address is an IPv4 address.
  bool is_v6() const;

  /// Determines whether the address is an IPv4 loopback address.
  ///
  /// @returns `true` if the address is v4 and its first byte has the
  /// value 127.
  bool is_loopback() const;

  /// Determines whether the address is an IPv4 broadcast address.
  ///
  /// @returns `true` if the address is v4 and has the value 255.255.255.255.
  bool is_broadcast() const;

  /// Determines whether the address is a multicast address. For v4
  /// addresses, this means the first byte equals to 224. For v6 addresses,
  /// this means the first bytes equals 255.
  ///
  /// @returns `true` if the address is a multicast address.
  bool is_multicast() const;

  /// Masks out lower bits of the address.
  ///
  /// @param top_bits_to_keep The number of bits *not* to mask out,
  /// counting from the highest order bit. The value is always
  /// interpreted relative to the IPv6 bit width, even if the address
  /// is IPv4. That means if we compute 192.168.1.2/16, we need to pass in
  /// 112 (i.e., 96 + 16). The value must be in the range from 0 to 128.
  ///
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

private:
  std::array<uint8_t, 16> bytes_;

private:
  friend access;

  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  template <typename Iterator>
  bool parse(Iterator& start, Iterator end)
  {
    string str;
    if (! extract(start, end, str))
      return false;

    if (auto a = from_string(str))
    {
      *this = std::move(*a);
      return true;
    }

    return false;
  }

  template <typename Iterator>
  bool print(Iterator& out) const
  {
    string str;
    if (! convert(str))
      return false;

    out = std::copy(str.begin(), str.end(), out);
    return true;
  }

  bool convert(string& str) const;

  friend bool operator==(address const& x, address const& y);
  friend bool operator<(address const& x, address const& y);
};

} // namespace vast

#endif
