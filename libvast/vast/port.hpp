//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/hashable/uniquely_represented.hpp"
#include "vast/detail/operators.hpp"

#include <cstdint>

namespace vast {

class data;

/// The transport layer type.
enum class port_type : uint8_t {
  icmp = 1,
  tcp = 6,
  udp = 17,
  icmp6 = 58,
  sctp = 132,
  unknown = 255,
};

/// A transport-layer port.
class port : detail::totally_ordered<port> {
public:
  using number_type = uint16_t;

  /// Constructs the empty port, i.e., `0/unknown`.
  port();

  /// Constructs a port.
  /// @param n The port number.
  /// @param t The port type.
  port(number_type n, port_type t = port_type::unknown);

  /// Retrieves the port number.
  /// @returns The port number.
  [[nodiscard]] number_type number() const;

  /// Retrieves the transport protocol type.
  /// @returns The port type.
  [[nodiscard]] port_type type() const;

  /// Sets the port number.
  /// @param n The new port number.
  void number(number_type n);

  /// Sets the port type.
  /// @param t The new port type.
  void type(port_type t);

  friend bool operator==(const port& x, const port& y);
  friend bool operator<(const port& x, const port& y);

  template <class Inspector>
  friend auto inspect(Inspector& f, port& x) {
    return f(x.data_);
  }

private:
  uint32_t data_ = 0;
};

template <>
struct is_uniquely_represented<port>
  : std::bool_constant<sizeof(port) == sizeof(uint32_t)> {};

bool convert(const port& p, data& d);

} // namespace vast
