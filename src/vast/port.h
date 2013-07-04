#ifndef VAST_PORT_H
#define VAST_PORT_H

#include <iosfwd>
#include <string>
#include "vast/fwd.h"
#include "vast/util/operators.h"

namespace vast {

/// A transport-layer port.
class port : util::totally_ordered<port>
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
  friend access;
  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  friend bool operator==(port const& x, port const& y);
  friend bool operator<(port const& x, port const& y);

  uint16_t number_ = 0;
  port_type type_ = unknown;
};

std::string to_string(port const& p);
std::ostream& operator<<(std::ostream& out, port const& p);

} // namespace vast

#endif
