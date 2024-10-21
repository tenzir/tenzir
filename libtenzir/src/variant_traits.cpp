//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/variant_traits.hpp"

namespace tenzir {

auto variant_traits<type>::index(const type& x) -> size_t {
  static constexpr auto table = std::invoke([] {
    return std::invoke(
      []<size_t... Is>(std::index_sequence<Is...>) {
        constexpr auto max_index
          = std::max({caf::detail::tl_at_t<concrete_types, Is>::type_index...});
        constexpr auto table_size = max_index + 1;
        auto table = std::array<uint8_t, table_size>{};
        std::ranges::fill(table, -1);
        ((table[caf::detail::tl_at_t<concrete_types, Is>::type_index] = Is),
         ...);
        // TODO: Why doesn't this make it throw below?
        static_assert(table_size
                      == caf::detail::tl_size<concrete_types>::value + 2);
        for (auto value : table) {
          if (value == -1) {
            throw std::runtime_error("element not set");
          }
        }
        return table;
      },
      std::make_index_sequence<count>());
  });
  auto idx = x.type_index();
  TENZIR_ASSERT(idx < table.size());
  // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-constant-array-index)
  return table[idx];
}

auto arrow_type_to_type_variant_index(const arrow::DataType& ty) -> size_t {
  auto type_id = ty.id();
  auto result = size_t{};
  // This could also be a lookup table, but the performance of what's below is
  // probably not worse (maybe even better).
  auto found = std::invoke(
    [&]<size_t... Is>(std::index_sequence<Is...>) {
      return (
        std::invoke([&] {
          using Type = caf::detail::tl_at_t<concrete_types, Is>;
          // TODO: Extension!
          if (Type::arrow_type::type_id != type_id) {
            return false;
          }
          if constexpr (extension_type<Type>) {
            if (static_cast<const arrow::ExtensionType&>(ty).extension_name()
                != Type::arrow_type::name) {
              return false;
            }
          }
          result = Is;
          return true;
        })
        || ...);
    },
    std::make_index_sequence<caf::detail::tl_size<concrete_types>::value>());
  TENZIR_ASSERT(found);
  return result;
}

} // namespace tenzir
