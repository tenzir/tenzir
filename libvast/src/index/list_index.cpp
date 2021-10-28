//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/index/list_index.hpp"

#include "vast/base.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/overload.hpp"
#include "vast/index/container_lookup.hpp"
#include "vast/legacy_type.hpp"
#include "vast/logger.hpp"
#include "vast/value_index_factory.hpp"

#include <caf/deserializer.hpp>
#include <caf/serializer.hpp>
#include <caf/settings.hpp>

#include <cmath>
#include <type_traits>

namespace vast {

list_index::list_index(vast::legacy_type t, caf::settings opts)
  : value_index{std::move(t), std::move(opts)} {
  max_size_ = caf::get_or(options(), "max-size",
                          defaults::index::max_container_elements);
  auto f = detail::overload{
    [](const auto&) {
      return vast::legacy_type{};
    },
    [](const legacy_list_type& x) {
      return x.value_type;
    },
  };
  value_type_ = caf::visit(f, value_index::type());
  VAST_ASSERT(!caf::holds_alternative<legacy_none_type>(value_type_));
  size_t components = std::log10(max_size_);
  if (max_size_ % 10 != 0)
    ++components;
  size_ = size_bitmap_index{base::uniform(10, components)};
}

caf::error list_index::serialize(caf::serializer& sink) const {
  return caf::error::eval(
    [&] { return value_index::serialize(sink); },
    [&] { return sink(elements_, size_, max_size_, value_type_); });
}

caf::error list_index::deserialize(caf::deserializer& source) {
  return caf::error::eval(
    [&] { return value_index::deserialize(source); },
    [&] { return source(elements_, size_, max_size_, value_type_); });
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
          // TODO: Support the #skip attribute here.
          if (!elements_[i])
            VAST_WARN("{} failed to create value index for type {}; values "
                      "can be exported, but will not be directly queryable",
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
  return caf::visit(f, x);
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

} // namespace vast
