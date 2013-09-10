#ifndef VAST_PORT_H
#define VAST_PORT_H

#include <iosfwd>
#include <cstring>
#include <string>
#include "vast/fwd.h"
#include "vast/util/operators.h"
#include "vast/util/parse.h"
#include "vast/util/print.h"

namespace vast {

/// A transport-layer port.
class port : util::totally_ordered<port>,
             util::parsable<port>,
             util::printable<port>
{
public:
  /// The transport layer type.
  enum port_type : uint8_t
  {
    unknown,
    tcp,
    udp,
    icmp
  };

  /// Constructs the empty port, i.e., @c 0/unknown.
  port() = default;

  /// Constructs a port.
  /// @param number The port number.
  /// @param type The port type.
  port(uint16_t number, port_type type = unknown);

  /// Constructs a port from another one.
  /// @param other The port to copy.
  port(port const& other);

  /// Moves a port.
  /// @param other The port to move.
  port(port&& other);

  /// Assigns another port to this instance.
  /// @param other The right-hand side of the assignment.
  port& operator=(port other);

  /// Retrieves the port number.
  /// @return The port number.
  uint16_t number() const;

  /// Retrieves the transport protocol type.
  /// @return The port type.
  port_type type() const;

  /// Sets the port type.
  /// @param t The new port type.
  void type(port_type t);

private:
  uint16_t number_ = 0;
  port_type type_ = unknown;

private:
  friend access;

  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  template <typename Iterator>
  bool parse(Iterator& start, Iterator end)
  {
    // Longest port: 42000/unknown = 5 + 1 + 7 = 13 bytes plus NUL.
    char buf[16];
    auto p = buf;
    while (*start != '/' && std::isdigit(*start) &&
           start != end && p < &buf[15])
      *p++ = *start++;

    uint16_t number;
    auto s = buf;
    if (! util::parse_positive_decimal(s, p, number))
      return false;

    if (start == end || *start++ != '/')
    {
      *this = {number, port::unknown};
      return true;
    }

    p = buf;
    while (start != end && p < &buf[7])
      *p++ = *start++;
    *p = '\0';

    if (! std::strncmp(buf, "tcp", 3))
      *this = {number, port::tcp};
    else if (! std::strncmp(buf, "udp", 3))
      *this = {number, port::udp};
    else if (! std::strncmp(buf, "icmp", 4))
      *this = {number, port::icmp};
    else
      *this = {number, port::unknown};

    return true;
  }

  template <typename Iterator>
  bool print(Iterator& out) const
  {
    static constexpr auto tcp = "tcp";
    static constexpr auto udp = "udp";
    static constexpr auto icmp = "icmp";
    if (! render(out, number_))
      return false;
    *out++ = '/';
    switch (type())
    {
      default:
        *out++ = '?';
        break;
      case port::tcp:
        out = std::copy(tcp, tcp + 3, out);
        break;
      case port::udp:
        out = std::copy(udp, udp + 3, out);
        break;
      case port::icmp:
        out = std::copy(icmp, icmp + 4, out);
        break;
    }
    return true;
  }

  friend bool operator==(port const& x, port const& y);
  friend bool operator<(port const& x, port const& y);
};

} // namespace vast

#endif
