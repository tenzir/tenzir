/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include <cstdint>

#include "vast/detail/operators.hpp"

namespace vast {

struct access;
class json;

/// A transport-layer port.
class port : detail::totally_ordered<port> {
  friend access;

public:
  using number_type = uint16_t;

  /// The transport layer type.
  enum port_type : uint8_t {
    unknown = 0,
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

  /// Sets the port number.
  /// @param n The new port number.
  void number(number_type n);

  /// Sets the port type.
  /// @param t The new port type.
  void type(port_type t);

  friend bool operator==(const port& x, const port& y);
  friend bool operator<(const port& x, const port& y);

  template <class Inspector>
  friend auto inspect(Inspector& f, port& p) {
    return f(p.number_, p.type_);
  }

private:
  number_type number_ = 0;
  port_type type_ = unknown;
};

bool convert(const port& p, json& j);

} // namespace vast

