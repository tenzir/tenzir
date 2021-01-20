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

#include "vast/index/enumeration_index.hpp"

#include "vast/detail/overload.hpp"
#include "vast/index/container_lookup.hpp"
#include "vast/type.hpp"

#include <caf/deserializer.hpp>
#include <caf/serializer.hpp>
#include <caf/settings.hpp>

namespace vast {

enumeration_index::enumeration_index(vast::type t, caf::settings opts)
  : value_index{std::move(t), std::move(opts)},
    index_{std::numeric_limits<enumeration>::max() + 1} {
  // nop
}

caf::error enumeration_index::serialize(caf::serializer& sink) const {
  return caf::error::eval([&] { return value_index::serialize(sink); },
                          [&] { return sink(index_); });
}

caf::error enumeration_index::deserialize(caf::deserializer& source) {
  return caf::error::eval([&] { return value_index::deserialize(source); },
                          [&] { return source(index_); });
}

bool enumeration_index::append_impl(data_view x, id pos) {
  if (auto e = caf::get_if<view<enumeration>>(&x)) {
    index_.skip(pos - index_.size());
    index_.append(*e);
    return true;
  }
  return false;
}

caf::expected<ids>
enumeration_index::lookup_impl(relational_operator op, data_view d) const {
  auto f = detail::overload{
    [&](auto x) -> caf::expected<ids> {
      return caf::make_error(ec::type_clash, materialize(x));
    },
    [&](view<enumeration> x) -> caf::expected<ids> {
      if (op == in || op == not_in)
        return caf::make_error(ec::unsupported_operator, op);
      return index_.lookup(op, x);
    },
    [&](view<list> xs) { return detail::container_lookup(*this, op, xs); },
  };
  return caf::visit(f, d);
}

size_t enumeration_index::memusage_impl() const {
  return index_.memusage();
}

} // namespace vast
