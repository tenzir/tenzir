//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/index/string_index.hpp"

#include "tenzir/bitmap_algorithms.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/inspection_common.hpp"
#include "tenzir/detail/legacy_deserialize.hpp"
#include "tenzir/detail/overload.hpp"
#include "tenzir/fbs/value_index.hpp"
#include "tenzir/index/container_lookup.hpp"
#include "tenzir/type.hpp"

#include <caf/binary_serializer.hpp>
#include <caf/serializer.hpp>
#include <caf/settings.hpp>

namespace tenzir {

string_index::string_index(tenzir::type t, caf::settings opts)
  : value_index{std::move(t), std::move(opts)} {
  max_length_
    = caf::get_or(options(), "max-size", defaults::index::max_string_size);
  auto b = base::uniform(10, std::log10(max_length_) + !!(max_length_ % 10));
  length_ = length_bitmap_index{std::move(b)};
}

bool string_index::inspect_impl(supported_inspectors& inspector) {
  return value_index::inspect_impl(inspector)
         && std::visit(
           [this](auto visitor) {
             return detail::apply_all(visitor.get(), max_length_, length_,
                                      chars_);
           },
           inspector);
}

bool string_index::append_impl(data_view x, id pos) {
  auto str = try_as<view<std::string>>(&x);
  if (!str)
    return false;
  auto length = str->size();
  if (length > max_length_)
    length = max_length_;
  if (length > chars_.size()) {
    chars_.reserve(length);
    for (size_t i = chars_.size(); i < length; ++i)
      chars_.emplace_back(8);
  }
  for (auto i = 0u; i < length; ++i) {
    chars_[i].skip(pos - chars_[i].size());
    chars_[i].append(static_cast<uint8_t>((*str)[i]));
  }
  length_.skip(pos - length_.size());
  length_.append(length);
  return true;
}

caf::expected<ids>
string_index::lookup_impl(relational_operator op, data_view x) const {
  auto f = detail::overload{
    [&](auto x) -> caf::expected<ids> {
      return caf::make_error(ec::type_clash, materialize(x));
    },
    [&](view<pattern>) -> caf::expected<ids> {
      switch (op) {
        default:
          return caf::make_error(ec::unsupported_operator, op);
        case relational_operator::equal:
        case relational_operator::not_equal: {
          return ids{offset(), true};
        }
      }
    },
    [&](view<std::string> str) -> caf::expected<ids> {
      auto str_size = str.size();
      if (str_size > max_length_)
        str_size = max_length_;
      switch (op) {
        default:
          return caf::make_error(ec::unsupported_operator, op);
        case relational_operator::equal:
        case relational_operator::not_equal: {
          if (str_size == 0) {
            auto result = length_.lookup(relational_operator::equal, 0);
            if (op == relational_operator::not_equal)
              result.flip();
            return result;
          }
          if (str_size > chars_.size())
            return ids{offset(), op == relational_operator::not_equal};
          auto result
            = length_.lookup(relational_operator::less_equal, str_size);
          if (all<0>(result))
            return ids{offset(), op == relational_operator::not_equal};
          for (auto i = 0u; i < str_size; ++i) {
            auto b = chars_[i].lookup(relational_operator::equal,
                                      static_cast<uint8_t>(str[i]));
            result &= b;
            if (all<0>(result))
              return ids{offset(), op == relational_operator::not_equal};
          }
          if (op == relational_operator::not_equal)
            result.flip();
          return result;
        }
        case relational_operator::ni:
        case relational_operator::not_ni: {
          if (str_size == 0)
            return ids{offset(), op == relational_operator::ni};
          if (str_size > chars_.size())
            return ids{offset(), op == relational_operator::not_ni};
          // TODO: Be more clever than iterating over all k-grams (#45).
          ids result{offset(), false};
          for (auto i = 0u; i < chars_.size() - str_size + 1; ++i) {
            ids substr{offset(), true};
            auto skip = false;
            for (auto j = 0u; j < str_size; ++j) {
              auto bm
                = chars_[i + j].lookup(relational_operator::equal, str[j]);
              if (all<0>(bm)) {
                skip = true;
                break;
              }
              substr &= bm;
            }
            if (!skip)
              result |= substr;
          }
          if (op == relational_operator::not_ni)
            result.flip();
          return result;
        }
      }
    },
    [&](view<list> xs) {
      return detail::container_lookup(*this, op, xs);
    },
  };
  return match(x, f);
}

size_t string_index::memusage_impl() const {
  size_t acc = length_.memusage();
  for (const auto& char_index : chars_)
    acc += char_index.memusage();
  return acc;
}

flatbuffers::Offset<fbs::ValueIndex> string_index::pack_impl(
  flatbuffers::FlatBufferBuilder& builder,
  flatbuffers::Offset<fbs::value_index::detail::ValueIndexBase> base_offset) {
  auto char_index_offsets
    = std::vector<flatbuffers::Offset<fbs::BitmapIndex>>{};
  char_index_offsets.reserve(chars_.size());
  for (const auto& char_index : chars_)
    char_index_offsets.emplace_back(pack(builder, char_index));
  const auto length_index_offset = pack(builder, length_);
  const auto string_index_offset = fbs::value_index::CreateStringIndexDirect(
    builder, base_offset, max_length_, length_index_offset,
    &char_index_offsets);
  return fbs::CreateValueIndex(builder, fbs::value_index::ValueIndex::string,
                               string_index_offset.Union());
}

caf::error string_index::unpack_impl(const fbs::ValueIndex& from) {
  const auto* from_string = from.value_index_as_string();
  TENZIR_ASSERT(from_string);
  max_length_ = from_string->max_length();
  if (auto err = unpack(*from_string->length_index(), length_))
    return err;
  chars_.clear();
  chars_.reserve(from_string->char_indexes()->size());
  for (const auto* char_index : *from_string->char_indexes()) {
    auto& to = chars_.emplace_back();
    if (auto err = unpack(*char_index, to))
      return err;
  }
  return caf::none;
}

} // namespace tenzir
