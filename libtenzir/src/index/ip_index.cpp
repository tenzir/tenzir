//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/index/ip_index.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/legacy_deserialize.hpp"
#include "tenzir/detail/overload.hpp"
#include "tenzir/fbs/value_index.hpp"
#include "tenzir/index/container_lookup.hpp"
#include "tenzir/type.hpp"

#include <caf/binary_serializer.hpp>
#include <caf/serializer.hpp>
#include <caf/settings.hpp>

#include <memory>

namespace tenzir {

ip_index::ip_index(tenzir::type t, caf::settings opts)
  : value_index{std::move(t), std::move(opts)} {
  for (auto& byte : bytes_)
    byte = byte_index{8};
}

bool ip_index::inspect_impl(supported_inspectors& inspector) {
  return value_index::inspect_impl(inspector)
         && std::visit(
           [this](auto visitor) {
             return visitor.get().apply(bytes_) && visitor.get().apply(v4_);
           },
           inspector);
}

bool ip_index::append_impl(data_view x, id pos) {
  auto addr = try_as<view<ip>>(&x);
  if (!addr)
    return false;
  for (auto i = 0u; i < 16; ++i) {
    bytes_[i].skip(pos - bytes_[i].size());
    auto bytes = static_cast<ip::byte_array>(*addr);
    bytes_[i].append(bytes[i]);
  }
  v4_.skip(pos - v4_.size());
  v4_.append(addr->is_v4());
  return true;
}

caf::expected<ids>
ip_index::lookup_impl(relational_operator op, data_view d) const {
  return match(d, detail::overload{
      [&](auto x) -> caf::expected<ids> {
        return caf::make_error(ec::type_clash, materialize(x));
      },
      [&](view<ip> x) -> caf::expected<ids> {
        if (!(op == relational_operator::equal
              || op == relational_operator::not_equal))
          return caf::make_error(ec::unsupported_operator, op);
        auto result = x.is_v4() ? v4_.coder().storage() : ids{offset(), true};
        for (auto i = x.is_v4() ? 12u : 0u; i < 16; ++i) {
          auto bytes = static_cast<ip::byte_array>(x);
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
        // Asking for /128 membership is equivalent to an equality lookup.
        if (topk == 128)
          return lookup_impl(op == relational_operator::in
                               ? relational_operator::equal
                               : relational_operator::not_equal,
                             x.network());
        // OPTIMIZATION: If we're in a /96 subnet and the network can be
        // represented as a valid IPv4 address, then we can just return the
        // v4_ bitmap.
        if (topk == 96 && x.network().is_v4()) {
          return is_negated(op) ? ~v4_.coder().storage()
                                : v4_.coder().storage();
        }
        auto result = ids{offset(), true};
        auto network = static_cast<ip::byte_array>(x.network());
        size_t i = 0;
        for (; i < 16 && topk >= 8; ++i, topk -= 8)
          result &= bytes_[i].lookup(relational_operator::equal, network[i]);
        for (auto j = 0u; j < topk; ++j) {
          auto bit = 7 - j;
          auto& bm = bytes_[i].coder().storage()[bit];
          result &= (network[i] >> bit) & 1 ? ~bm : bm;
        }
        if (is_negated(op))
          result.flip();
        return result;
      },
      [&](view<list> xs) {
        return detail::container_lookup(*this, op, xs);
      },
    });
}

size_t ip_index::memusage_impl() const {
  auto acc = v4_.memusage();
  for (const auto& byte_index : bytes_)
    acc += byte_index.memusage();
  return acc;
}

flatbuffers::Offset<fbs::ValueIndex> ip_index::pack_impl(
  flatbuffers::FlatBufferBuilder& builder,
  flatbuffers::Offset<fbs::value_index::detail::ValueIndexBase> base_offset) {
  auto byte_index_offsets
    = std::vector<flatbuffers::Offset<fbs::BitmapIndex>>{};
  byte_index_offsets.reserve(bytes_.size());
  for (const auto& byte_index : bytes_)
    byte_index_offsets.emplace_back(pack(builder, byte_index));
  const auto v4_index_offset = pack(builder, v4_);
  const auto ip_index_offset = fbs::value_index::CreateIPIndexDirect(
    builder, base_offset, &byte_index_offsets, v4_index_offset);
  return fbs::CreateValueIndex(builder, fbs::value_index::ValueIndex::ip,
                               ip_index_offset.Union());
}

caf::error ip_index::unpack_impl(const fbs::ValueIndex& from) {
  const auto* from_ip = from.value_index_as_ip();
  TENZIR_ASSERT(from_ip);
  if (from_ip->byte_indexes()->size() != bytes_.size())
    return caf::make_error(ec::format_error,
                           fmt::format("unexpected number of byte indexes in "
                                       "IP index: expected {}, got {}",
                                       bytes_.size(),
                                       from_ip->byte_indexes()->size()));
  for (size_t i = 0; i < bytes_.size(); ++i)
    if (auto err = unpack(*from_ip->byte_indexes()->Get(i), bytes_[i]))
      return err;
  return unpack(*from_ip->v4_index(), v4_);
}

} // namespace tenzir
