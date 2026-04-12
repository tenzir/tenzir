//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/concept/printable/tenzir/port.hpp"

#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/data.hpp"
#include "tenzir/port.hpp"

#include <tuple>

namespace tenzir {

port::port() {
  type(port_type::unknown);
}

port::port(number_type n, port_type t) {
  number(n);
  type(t);
}

port::number_type port::number() const {
  return data_ >> 16;
}

port_type port::type() const {
  return static_cast<port_type>(data_ & 0xFF);
}

void port::number(number_type n) {
  data_ |= uint32_t{n} << 16;
}

void port::type(port_type t) {
  data_ &= 0xFFFFFF00;
  data_ |= static_cast<std::underlying_type_t<port_type>>(t);
}

bool operator==(const port& x, const port& y) {
  return x.number() == y.number()
         and (x.type() == y.type() or x.type() == port_type::unknown
             or y.type() == port_type::unknown);
}

bool operator<(const port& x, const port& y) {
  return x.data_ < y.data_;
}

bool convert(const port& p, data& d) {
  d = to_string(p);
  return true;
}

} // namespace tenzir
