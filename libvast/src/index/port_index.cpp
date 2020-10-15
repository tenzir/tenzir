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

#include "vast/index/port_index.hpp"

#include "vast/defaults.hpp"
#include "vast/index/container_lookup.hpp"
#include "vast/type.hpp"

#include <caf/deserializer.hpp>
#include <caf/serializer.hpp>
#include <caf/settings.hpp>

#include <cmath>
#include <memory>

namespace vast {

port_index::port_index(vast::type t, caf::settings opts)
  : value_index{std::move(t), std::move(opts)},
    num_{base::uniform(10, 5)}, // [0, 2^16)
    proto_{256}                 // 8-bit proto/next-header field
{
  // nop
}

caf::error port_index::serialize(caf::serializer& sink) const {
  return caf::error::eval([&] { return value_index::serialize(sink); },
                          [&] { return sink(num_, proto_); });
}

caf::error port_index::deserialize(caf::deserializer& source) {
  return caf::error::eval([&] { return value_index::deserialize(source); },
                          [&] { return source(num_, proto_); });
}

bool port_index::append_impl(data_view x, id pos) {
  if (auto p = caf::get_if<view<port>>(&x)) {
    num_.skip(pos - num_.size());
    num_.append(p->number());
    proto_.skip(pos - proto_.size());
    proto_.append(p->type());
    return true;
  }
  return false;
}

caf::expected<ids>
port_index::lookup_impl(relational_operator op, data_view d) const {
  return caf::visit(
    detail::overload{
      [&](auto x) -> caf::expected<ids> {
        return make_error(ec::type_clash, materialize(x));
      },
      [&](view<port> x) -> caf::expected<ids> {
        if (op == in || op == not_in)
          return make_error(ec::unsupported_operator, op);
        auto result = num_.lookup(op, x.number());
        if (all<0>(result))
          return ids{offset(), false};
        if (x.type() != port::unknown) {
          if (op == not_equal)
            result |= proto_.lookup(not_equal, x.type());
          else
            result &= proto_.lookup(equal, x.type());
        }
        return result;
      },
      [&](view<list> xs) { return detail::container_lookup(*this, op, xs); },
    },
    d);
}

} // namespace vast
