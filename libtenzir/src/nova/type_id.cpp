//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/type_id.hpp"

#include "tenzir/hash/hash.hpp"
#include "tenzir/hash/legacy_hash.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/fundamental_array_builder.hpp"
#include "tenzir/nova/list_array.hpp"
#include "tenzir/nova/record_array.hpp"
#include "tenzir/nova/type_system.hpp"

#include <fmt/format.h>

#include <algorithm>
#include <vector>

namespace tenzir::nova {

namespace {

auto hash_type(RowView<Data> row) -> legacy_hash::result_type {
  return match(row, []<typename T>(RowView<T> value) {
    auto hash = legacy_hash{};
    if constexpr (fundamental_view_type<T>) {
      hash_append(hash, Type<TagForViewType<T>>::static_name);
    } else {
      hash_append(hash, Type<T>::static_name);
    }
    if constexpr (std::same_as<T, Record>) {
      for (auto [name, field] : value) {
        hash_append(hash, name, hash_type(field));
      }
    } else if constexpr (std::same_as<T, List>) {
      auto alternatives = std::vector<legacy_hash::result_type>{};
      for (auto element : value) {
        alternatives.push_back(hash_type(element));
      }
      std::ranges::sort(alternatives);
      auto [first_duplicate, end] = std::ranges::unique(alternatives);
      alternatives.erase(first_duplicate, end);
      hash_append(hash, alternatives);
    }
    return std::move(hash).finish();
  });
}

} // namespace

auto type_id(Array<Data> const& array, storage::BitMap const& mask)
  -> Array<String> {
  TENZIR_ASSERT_EQ(array.length(), mask.length());
  auto builder = ArrayBuilder<String>{};
  for (auto index : storage::bitmap_iteration(mask)) {
    if (not index) {
      builder.skip();
      continue;
    }
    builder.data(fmt::format("{:x}", hash_type(array.get(*index))));
  }
  return builder.finish();
}

} // namespace tenzir::nova
