#ifndef VAST_PORT_H
#define VAST_PORT_H

#include <iosfwd>
#include <cstring>
#include <string>
#include "vast/fwd.h"
#include "vast/parse.h"
#include "vast/print.h"
#include "vast/util/operators.h"

namespace vast {

/// A transport-layer port.
class port : util::totally_ordered<port>
{
public:
  using number_type = uint16_t;

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
  port(number_type number, port_type type = unknown);

  /// Retrieves the port number.
  /// @returns The port number.
  number_type number() const;

  /// Retrieves the transport protocol type.
  /// @returns The port type.
  port_type type() const;

  /// Sets the port type.
  /// @param t The new port type.
  void type(port_type t);

private:
  number_type number_ = 0;
  port_type type_ = unknown;

private:
  friend access;

  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  template <typename Iterator>
  friend trial<void> parse(port& prt, Iterator& begin, Iterator end)
  {
    // Longest port: 42000/unknown = 5 + 1 + 7 = 13 bytes plus NUL.
    char buf[16];
    auto p = buf;
    while (*begin != '/' && std::isdigit(*begin) && begin != end
           && p - buf < 16)
      *p++ = *begin++;

    auto lval = buf;
    auto t = parse_positive_decimal(prt.number_, lval, p);
    if (! t)
      return t.error();

    if (begin == end || *begin++ != '/')
      return nothing;

    p = buf;
    while (begin != end && p < &buf[7])
      *p++ = *begin++;
    *p = '\0';

    if (! std::strncmp(buf, "tcp", 3))
      prt.type_ = port::tcp;
    else if (! std::strncmp(buf, "udp", 3))
      prt.type_ = port::udp;
    else if (! std::strncmp(buf, "icmp", 4))
      prt.type_ = port::icmp;

    return nothing;
  }

  template <typename Iterator>
  friend trial<void> print(port const& p, Iterator&& out)
  {
    auto t = print(p.number_, out);
    if (! t)
      return t.error();

    *out++ = '/';

    switch (p.type())
    {
      default:
        return print('?', out);
      case port::tcp:
        return print("tcp", out);
      case port::udp:
        return print("udp", out);
      case port::icmp:
        return print("icmp", out);
    }
  }

  friend bool operator==(port const& x, port const& y);
  friend bool operator<(port const& x, port const& y);
};

trial<void> convert(port const& p, util::json& j);

} // namespace vast

#endif
