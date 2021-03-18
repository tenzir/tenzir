// SPDX-FileCopyrightText: (c) 2016 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/concept/printable/vast/port.hpp"

#include "vast/concept/printable/to_string.hpp"
#include "vast/data.hpp"
#include "vast/port.hpp"

#include <tuple>

namespace vast {

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
         && (x.type() == y.type() || x.type() == port_type::unknown
             || y.type() == port_type::unknown);
}

bool operator<(const port& x, const port& y) {
  return x.data_ < y.data_;
}

bool convert(const port& p, data& d) {
  d = to_string(p);
  return true;
}

} // namespace vast
