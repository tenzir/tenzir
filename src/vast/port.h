#ifndef VAST_PORT_H
#define VAST_PORT_H

#include <cstdint>

#include "vast/util/operators.h"

namespace vast {

struct access;

/// A transport-layer port.
class port : util::totally_ordered<port> {
  friend access;

public:
  using number_type = uint16_t;

  /// The transport layer type.
  enum port_type : uint8_t { unknown, tcp, udp, icmp };

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

private:
  number_type number_ = 0;
  port_type type_ = unknown;
};

} // namespace vast

#endif
