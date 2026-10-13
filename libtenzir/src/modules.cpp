//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/modules.hpp"

#include "tenzir/detail/heterogeneous_string_hash.hpp"
#include "tenzir/legacy_type.hpp"
#include "tenzir/logger.hpp"
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
  /// Schema directories that no lookup has needed yet. Empty once they are
  /// read, which happens at most once.
  Option<detail::stable_set<std::filesystem::path>> pending_dirs;
};

bool initialized = false;

auto get_impl() -> global_module_registry& {
  static global_module_registry data;
  return data;
}

/// Adds parsed definitions to the registry. The caller holds the lock.
void add_symbols(global_module_registry& impl, symbol_map symbols,
                 symbol_map2 symbols2) {
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
}

/// Reads the deferred schema directories, if any are left. The caller holds the
/// lock. Reading them is not fast, but it happens once per process and only for
/// processes that look a schema up.
void load_pending_dirs(global_module_registry& impl) {
  if (not impl.pending_dirs) {
    return;
  }
  auto dirs = std::move(*impl.pending_dirs);
  impl.pending_dirs.reset();
  auto symbols = load_symbols(dirs);
  if (not symbols) {
    TENZIR_ERROR("failed to read schema dirs: {}", symbols.error());
    return;
  }
  auto symbols2 = load_symbols2(dirs);
  if (not symbols2) {
    if (symbols2 != ec::silent) {
      TENZIR_ERROR("failed to read schema dirs: {}", symbols2.error());
    }
    return;
  }
  add_symbols(impl, std::move(*symbols), std::move(*symbols2));
}

} // namespace

void init(symbol_map symbols, symbol_map2 symbols2, concepts_map concepts) {
  TENZIR_ASSERT(not initialized);
  auto& impl = get_impl();
  auto lock = std::unique_lock{impl.mutex};
  add_symbols(impl, std::move(symbols), std::move(symbols2));
  impl.concepts = std::move(concepts);
  initialized = true;
}

void init(detail::stable_set<std::filesystem::path> module_dirs,
          concepts_map concepts) {
  TENZIR_ASSERT(not initialized);
  auto& impl = get_impl();
  auto lock = std::unique_lock{impl.mutex};
  impl.pending_dirs = std::move(module_dirs);
  impl.concepts = std::move(concepts);
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

auto get_schema(std::string_view name) -> Option<type> {
  auto& global = get_impl();
  // The critical section here is very small once the type has been converted.
  // This function should thus be fine to call outside of tight loops.
  auto lock = std::unique_lock{global.mutex};
  load_pending_dirs(global);
  auto it = global.types.find(name);
  if (it == global.types.end()) {
    return None{};
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
