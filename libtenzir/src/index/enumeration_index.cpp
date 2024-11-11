//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/index/enumeration_index.hpp"

#include "tenzir/detail/legacy_deserialize.hpp"
#include "tenzir/detail/overload.hpp"
#include "tenzir/fbs/value_index.hpp"
#include "tenzir/index/container_lookup.hpp"
#include "tenzir/type.hpp"

#include <caf/binary_serializer.hpp>
#include <caf/serializer.hpp>
#include <caf/settings.hpp>

namespace tenzir {

enumeration_index::enumeration_index(tenzir::type t, caf::settings opts)
  : value_index{std::move(t), std::move(opts)},
    index_{std::numeric_limits<enumeration>::max() + 1} {
  // nop
}

bool enumeration_index::inspect_impl(supported_inspectors& inspector) {
  return value_index::inspect_impl(inspector)
         && std::visit(
           [this](auto visitor) {
             return visitor.get().apply(index_);
           },
           inspector);
}

bool enumeration_index::append_impl(data_view x, id pos) {
  if (auto e = try_as<view<enumeration>>(&x)) {
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
      if (op == relational_operator::in || op == relational_operator::not_in)
        return caf::make_error(ec::unsupported_operator, op);
      return index_.lookup(op, x);
    },
    [&](view<list> xs) {
      return detail::container_lookup(*this, op, xs);
    },
  };
  return caf::visit(f, d);
}

size_t enumeration_index::memusage_impl() const {
  return index_.memusage();
}

flatbuffers::Offset<fbs::ValueIndex> enumeration_index::pack_impl(
  flatbuffers::FlatBufferBuilder& builder,
  flatbuffers::Offset<fbs::value_index::detail::ValueIndexBase> base_offset) {
  const auto index_offset = pack(builder, index_);
  const auto enumeration_index_offset
    = fbs::value_index::CreateEnumerationIndex(builder, base_offset,
                                               index_offset);
  return fbs::CreateValueIndex(builder,
                               fbs::value_index::ValueIndex::enumeration,
                               enumeration_index_offset.Union());
}

caf::error enumeration_index::unpack_impl(const fbs::ValueIndex& from) {
  const auto* from_enumeration = from.value_index_as_enumeration();
  TENZIR_ASSERT(from_enumeration);
  return unpack(*from_enumeration->index(), index_);
}

} // namespace tenzir
