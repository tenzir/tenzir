#ifndef VAST_PORT_H
#define VAST_PORT_H

#include <iosfwd>
#include <cstring>
#include <string>
#include "vast/fwd.h"
#include "vast/print.h"
#include "vast/util/operators.h"

namespace vast {

/// A transport-layer port.
class port : util::totally_ordered<port>
{
  friend access;

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

  friend bool operator==(port const& x, port const& y);
  friend bool operator<(port const& x, port const& y);

  /// Retrieves the port number.
  /// @returns The port number.
  number_type number() const;

  /// Retrieves the transport protocol type.
  /// @returns The port type.
  port_type type() const;

  /// Sets the port number.
  /// @param n The new port number.
  void number(number_type n);

  /// Sets the port type.
  /// @param t The new port type.
  void type(port_type t);

  // TODO: Migrate to concepts location.
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

private:
  number_type number_ = 0;
  port_type type_ = unknown;
};

// TODO: Migrate to concepts location.
trial<void> convert(port const& p, util::json& j);

} // namespace vast

#endif
