//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/modules.hpp"

#include "tenzir/detail/heterogeneous_string_hash.hpp"
#include "tenzir/legacy_type.hpp"
#include "tenzir/taxonomies.hpp"

#include <boost/unordered/unordered_flat_map.hpp>

#include <mutex>
#include <utility>
#include <vector>

namespace tenzir::modules {

namespace {

struct global_module_registry {
  std::mutex mutex;
  boost::unordered_flat_map<
    std::string, variant<type, legacy_type, ast::type_def>,
    detail::heterogeneous_string_hash, detail::heterogeneous_string_equal>
    types;
  concepts_map concepts;
};

bool initialized = false;

auto get_impl() -> global_module_registry& {
  static global_module_registry data;
  return data;
}

} // namespace

void init(symbol_map symbols, symbol_map2 symbols2, concepts_map concepts) {
  TENZIR_ASSERT(not initialized);
  auto& impl = get_impl();
  auto lock = std::unique_lock{impl.mutex};
  for (auto& [name, ty] : symbols) {
    TENZIR_ASSERT(name == ty.name());
    if (is<legacy_record_type>(ty)) {
      impl.types.emplace(name, std::move(ty));
    }
  }
  for (auto& [name, def] : symbols2) {
    auto [_, success] = impl.types.emplace(name, std::move(def));
    TENZIR_ASSERT(success,
                  "name conflict between .schema and .tql type definition for "
                  "schema `{}`",
                  name);
  }
  get_impl().concepts = std::move(concepts);
  initialized = true;
}

namespace {
struct visitor {
  global_module_registry& global;

  auto operator()(ast::type_def& def, std::string_view alias = {}) -> type {
    auto converted = match(def);
    if (! alias.empty() && converted.name() != alias) {
      return type{alias, converted};
    }
    return converted;
  }

private:
  auto match(ast::type_def& def) -> type {
    return tenzir::match(
      def,
      [&](ast::type_name& name) -> type {
        return resolve(name.id.name);
      },
      [&](ast::record_def& record) -> type {
        auto fields = std::vector<struct record_type::field>{};
        fields.reserve(record.fields.size());
        for (auto& field : record.fields) {
          fields.emplace_back(field.name.name, (*this)(field.type));
        }
        return type{record_type{fields}};
      },
      [&](ast::list_def& list) -> type {
        auto value = (*this)(list.type);
        return type{list_type{std::move(value)}};
      });
  }

  auto resolve(std::string_view name) -> type {
    if (auto builtin = translate_builtin_type(name)) {
      return *builtin;
    }
    auto it = global.types.find(name);
    TENZIR_ASSERT(it != global.types.end());
    return tenzir::match(
      it->second,
      [](const type& ty) {
        return ty;
      },
      [&](legacy_type& legacy) {
        auto converted = type::from_legacy_type(legacy);
        it->second = converted;
        return converted;
      },
      [&](ast::type_def& def) {
        auto converted = (*this)(def, name);
        it->second = converted;
        return converted;
      });
  }
};
} // namespace

auto get_schema(std::string_view name) -> std::optional<type> {
  auto& global = get_impl();
  // The critical section here is very small once the type has been converted.
  // This function should thus be fine to call outside of tight loops.
  auto lock = std::unique_lock{global.mutex};
  auto it = global.types.find(name);
  if (it == global.types.end()) {
    return std::nullopt;
  }
  return match(
    it->second,
    [](const type& ty) {
      return ty;
    },
    [&](const legacy_type& legacy) {
      auto converted = type::from_legacy_type(legacy);
      it->second = converted;
      return converted;
    },
    [&](ast::type_def& def) -> type {
      auto converted = visitor{global}(def, it->first);
      it->second = converted;
      return converted;
    });
}

// Get the concepts map.
// Returns an empty map if init(...) was not called.
auto concepts() -> const concepts_map& {
  return get_impl().concepts;
}

} // namespace tenzir::modules
