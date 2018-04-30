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

#include <tuple>

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/port.hpp"
#include "vast/json.hpp"
#include "vast/port.hpp"

namespace vast {

port::port(number_type n, port_type t) {
  number(n);
  type(t);
}

port::number_type port::number() const {
  return data_ >> 16;
}

port::port_type port::type() const {
  return static_cast<port_type>(data_ & 0xFF);
}

void port::number(number_type n) {
  data_ |= uint32_t{n} << 16;
}

void port::type(port_type t) {
  data_ |= static_cast<std::underlying_type_t<port_type>>(t);
}

bool operator==(const port& x, const port& y) {
  return x.number() == y.number()
         && (x.type() == y.type()
             || x.type() == port::unknown
             || y.type() == port::unknown);
}

bool operator<(const port& x, const port& y) {
  return x.data_ < y.data_;
}

bool convert(const port& p, json& j) {
  j = to_string(p);
  return true;
}

} // namespace vast
