//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/index/list_index.hpp"

#include "tenzir/base.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/detail/legacy_deserialize.hpp"
#include "tenzir/detail/overload.hpp"
#include "tenzir/fbs/value_index.hpp"
#include "tenzir/index/container_lookup.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/type.hpp"
#include "tenzir/value_index_factory.hpp"

#include <caf/binary_serializer.hpp>
#include <caf/serializer.hpp>
#include <caf/settings.hpp>

#include <cmath>
#include <type_traits>

namespace tenzir {

list_index::list_index(tenzir::type t, caf::settings opts)
  : value_index{std::move(t), std::move(opts)} {
  max_size_ = caf::get_or(options(), "max-size",
                          defaults::index::max_container_elements);
  auto f = detail::overload{
    [](const auto&) {
      return tenzir::type{};
    },
    [](const list_type& x) {
      return x.value_type();
    },
  };
  value_type_ = match(value_index::type(), f);
  TENZIR_ASSERT(value_type_);
  size_t components = std::log10(max_size_);
  if (max_size_ % 10 != 0)
    ++components;
  size_ = size_bitmap_index{base::uniform(10, components)};
}

bool list_index::inspect_impl(supported_inspectors& inspector) {
  return value_index::inspect_impl(inspector)
         && std::visit(
           [this](auto visitor) {
             return detail::apply_all(visitor.get(), elements_, size_,
                                      max_size_, value_type_);
           },
           inspector);
}

bool list_index::append_impl(data_view x, id pos) {
  auto f = [&](const auto& v) {
    using view_type = std::decay_t<decltype(v)>;
    if constexpr (std::is_same_v<view_type, view<list>>) {
      auto seq_size = std::min(v->size(), max_size_);
      if (seq_size > elements_.size()) {
        auto old = elements_.size();
        elements_.resize(seq_size);
        for (auto i = old; i < elements_.size(); ++i) {
          elements_[i] = factory<value_index>::make(value_type_, options());
          if (!elements_[i])
            TENZIR_DEBUG("{} failed to create value index for type {}",
                         detail::pretty_type_name(this), value_type_);
        }
      }
      auto x = v->begin();
      for (auto i = 0u; i < seq_size; ++i)
        if (elements_[i])
          elements_[i]->append(*x++, pos);
      size_.skip(pos - size_.size());
      size_.append(seq_size);
      return true;
    }
    return false;
  };
  return match(x, f);
}

caf::expected<ids>
list_index::lookup_impl(relational_operator op, data_view x) const {
  if (!(op == relational_operator::ni || op == relational_operator::not_ni))
    return caf::make_error(ec::unsupported_operator, op);
  auto result = ids{};
  if (elements_.empty())
    return ids{};
  for (auto i = 0u; i < elements_.size(); ++i) {
    if (elements_[i]) {
      auto mbm = elements_[i]->lookup(relational_operator::equal, x);
      if (mbm)
        result |= *mbm;
      else
        return mbm;
    }
  }
  if (op == relational_operator::not_ni)
    result.flip();
  return result;
}

size_t list_index::memusage_impl() const {
  size_t acc = 0;
  for (const auto& element : elements_)
    if (element)
      acc += element->memusage();
  acc += size_.memusage();
  return acc;
}

flatbuffers::Offset<fbs::ValueIndex> list_index::pack_impl(
  flatbuffers::FlatBufferBuilder& builder,
  flatbuffers::Offset<fbs::value_index::detail::ValueIndexBase> base_offset) {
  auto element_offsets = std::vector<flatbuffers::Offset<fbs::ValueIndex>>{};
  element_offsets.reserve(elements_.size());
  for (const auto& element : elements_)
    element_offsets.emplace_back(pack(builder, element));
  const auto size_bitmap_index_offset = pack(builder, size_);
  const auto list_index_offset
    = fbs::value_index::CreateListIndexDirect(builder, base_offset,
                                              &element_offsets, max_size_,
                                              size_bitmap_index_offset);
  return fbs::CreateValueIndex(builder, fbs::value_index::ValueIndex::list,
                               list_index_offset.Union());
}

caf::error list_index::unpack_impl(const fbs::ValueIndex& from) {
  const auto* from_list = from.value_index_as_list();
  TENZIR_ASSERT(from_list);
  elements_.clear();
  elements_.reserve(from_list->elements()->size());
  for (const auto* element : *from_list->elements()) {
    auto& to = elements_.emplace_back();
    if (auto err = unpack(*element, to))
      return err;
  }
  max_size_ = from_list->max_size();
  if (auto err = unpack(*from_list->size_bitmap_index(), size_))
    return err;
  // The value type can simply be retreived from the base classes' type, not
  // sure why it is stored separately. — DL
  value_type_ = as<list_type>(type()).value_type();
  return caf::none;
}

} // namespace tenzir
