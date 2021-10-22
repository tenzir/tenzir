//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/index/address_index.hpp"

#include "vast/detail/overload.hpp"
#include "vast/index/container_lookup.hpp"
#include "vast/legacy_type.hpp"

#include <caf/deserializer.hpp>
#include <caf/serializer.hpp>
#include <caf/settings.hpp>

#include <memory>

namespace vast {

address_index::address_index(vast::legacy_type t, caf::settings opts)
  : value_index{std::move(t), std::move(opts)} {
  bytes_.fill(byte_index{8});
}

caf::error address_index::serialize(caf::serializer& sink) const {
  return caf::error::eval([&] { return value_index::serialize(sink); },
                          [&] { return sink(bytes_, v4_); });
}

caf::error address_index::deserialize(caf::deserializer& source) {
  return caf::error::eval([&] { return value_index::deserialize(source); },
                          [&] { return source(bytes_, v4_); });
}

bool address_index::append_impl(data_view x, id pos) {
  auto addr = caf::get_if<view<address>>(&x);
  if (!addr)
    return false;
  for (auto i = 0u; i < 16; ++i) {
    bytes_[i].skip(pos - bytes_[i].size());
    auto bytes = static_cast<address::byte_array>(*addr);
    bytes_[i].append(bytes[i]);
  }
  v4_.skip(pos - v4_.size());
  v4_.append(addr->is_v4());
  return true;
}

caf::expected<ids>
address_index::lookup_impl(relational_operator op, data_view d) const {
  return caf::visit(
    detail::overload{
      [&](auto x) -> caf::expected<ids> {
        return caf::make_error(ec::type_clash, materialize(x));
      },
      [&](view<address> x) -> caf::expected<ids> {
        if (!(op == relational_operator::equal
              || op == relational_operator::not_equal))
          return caf::make_error(ec::unsupported_operator, op);
        auto result = x.is_v4() ? v4_.coder().storage() : ids{offset(), true};
        for (auto i = x.is_v4() ? 12u : 0u; i < 16; ++i) {
          auto bytes = static_cast<address::byte_array>(x);
          auto bm = bytes_[i].lookup(relational_operator::equal, bytes[i]);
          result &= bm;
          if (all<0>(result))
            return ids{offset(), op == relational_operator::not_equal};
        }
        if (op == relational_operator::not_equal)
          result.flip();
        return result;
      },
      [&](view<subnet> x) -> caf::expected<ids> {
        if (!(op == relational_operator::in
              || op == relational_operator::not_in))
          return caf::make_error(ec::unsupported_operator, op);
        auto topk = x.length();
        if (topk == 0)
          return caf::make_error(ec::unspecified,
                                 "invalid IP subnet length: ", topk);
        auto is_v4 = x.network().is_v4();
        if ((is_v4 ? topk + 96 : topk) == 128)
          // Asking for /32 or /128 membership is equivalent to an equality
          // lookup.
          return lookup_impl(op == relational_operator::in
                               ? relational_operator::equal
                               : relational_operator::not_equal,
                             x.network());
        auto result = is_v4 ? v4_.coder().storage() : ids{offset(), true};
        auto network = static_cast<address::byte_array>(x.network());
        size_t i = is_v4 ? 12 : 0;
        for (; i < 16 && topk >= 8; ++i, topk -= 8)
          result &= bytes_[i].lookup(relational_operator::equal, network[i]);
        for (auto j = 0u; j < topk; ++j) {
          auto bit = 7 - j;
          auto& bm = bytes_[i].coder().storage()[bit];
          result &= (network[i] >> bit) & 1 ? ~bm : bm;
        }
        if (op == relational_operator::not_in)
          result.flip();
        return result;
      },
      [&](view<list> xs) { return detail::container_lookup(*this, op, xs); },
    },
    d);
}

size_t address_index::memusage_impl() const {
  auto acc = v4_.memusage();
  for (const auto& byte_index : bytes_)
    acc += byte_index.memusage();
  return acc;
}

} // namespace vast
