//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/address.hpp"
#include "vast/detail/generator.hpp"
#include "vast/detail/hash_scalar.hpp"
#include "vast/detail/passthrough.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/die.hpp"
#include "vast/hash/hash.hpp"
#include "vast/type.hpp"
#include "vast/view.hpp"

#include <arrow/array.h>

#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace vast::detail {

// FIXME: wait for the implementation of to land in master.
template <concrete_type Type>
view<type_to_data_t<Type>>
value_at(const Type& t, const type_to_arrow_array_storage_t<Type>&,
         int64_t) noexcept {
  static auto x = t.construct(); // ¯\_(ツ)_/¯
  return view<type_to_data_t<Type>>{x};
}

template <incremental_hash HashAlgorithm = default_hash>
struct array_hasher {
public:
  // Overload that handles all stateless hash computations that only dependend
  // on the array value.
  template <basic_type Type>
  generator<uint64_t> operator()(const Type& t, const arrow::Array& xs) const {
    if (xs.null_count() > 0)
      co_yield nil_hash_digest;
    // Work around value_at.
    const auto& array = [&]() -> decltype(auto) {
      if constexpr (arrow::is_extension_type<type_to_arrow_type_t<Type>>::value)
        return *caf::get<type_to_arrow_array_t<Type>>(xs).storage();
      else
        return caf::get<type_to_arrow_array_t<Type>>(xs);
    }();
    for (auto i = 0; i < xs.length(); ++i) {
      if (!xs.IsNull(i)) {
        auto x = value_at(t, array, i);
        co_yield hash_scalar<Type>(x);
      }
    }
  }

  generator<uint64_t>
  operator()(const bool_type&, const arrow::Array& xs) const {
    const auto& ys = caf::get<type_to_arrow_array_t<bool_type>>(xs);
    static const auto f = hash_scalar<bool_type>(false);
    static const auto t = hash_scalar<bool_type>(true);
    if (ys.null_count() > 0)
      co_yield nil_hash_digest;
    if (ys.false_count() > 0)
      co_yield f;
    if (ys.true_count() > 0)
      co_yield t;
  }

  // We hash enums as strings to make it possible to compare strings at the
  // query side with enums.
  generator<uint64_t>
  operator()(const enumeration_type&, const arrow::Array& xs) const {
    const auto& ys = caf::get<enumeration_type::array_type>(xs);
    return (*this)(string_type{}, *ys.storage()->dictionary());
  }

  generator<uint64_t>
  operator()(const list_type& t, const arrow::Array& xs) const {
    const auto& ys = caf::get<type_to_arrow_array_t<list_type>>(xs);
    return caf::visit(*this, t.value_type(), passthrough(*ys.values()));
  }

  generator<uint64_t>
  operator()(const map_type& t, const arrow::Array& xs) const {
    const auto& ys = caf::get<type_to_arrow_array_t<map_type>>(xs);
    const auto& keys = *ys.keys();
    const auto& values = *ys.items();
    for (auto digest : caf::visit(*this, t.key_type(), passthrough(keys)))
      co_yield digest;
    for (auto digest : caf::visit(*this, t.value_type(), passthrough(values)))
      co_yield digest;
  }

  generator<uint64_t>
  operator()(const record_type& t, const arrow::Array& xs) const {
    const auto& ys = caf::get<type_to_arrow_array_t<record_type>>(xs);
    for (auto i = 0u; i < t.num_fields(); ++i) {
      const auto& zs = *ys.field(i);
      for (auto digest : caf::visit(*this, t.field(i).type, passthrough(zs)))
        co_yield digest;
    }
  }
};

/// Convenience instantiation for easier use at the call site.
auto hash_array(const arrow::Array& xs) {
  auto inferred_type = type::from_arrow(*xs.type());
  return caf::visit(array_hasher{}, inferred_type, passthrough(xs));
}

} // namespace vast::detail
