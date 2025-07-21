//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/modules.hpp"

#include "tenzir/detail/byteswap.hpp"
#include "tenzir/detail/heterogeneous_string_hash.hpp"
#include "tenzir/fbs/type.hpp"
#include "tenzir/io/read.hpp"
#include "tenzir/legacy_type.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/module.hpp"
#include "tenzir/taxonomies.hpp"

#include <boost/unordered/unordered_flat_map.hpp>
#include <caf/actor_system_config.hpp>

#include <mutex>
#include <utility>

namespace tenzir::modules {

namespace {

struct global_module_registry {
  std::mutex mutex;
  boost::unordered_flat_map<std::string, variant<type, legacy_type>,
                            detail::heterogeneous_string_hash,
                            detail::heterogeneous_string_equal>
    types;

  concepts_map concepts;
};

bool initialized = false;

auto get_impl() -> global_module_registry& {
  static global_module_registry data;
  return data;
}

} // namespace

void init(const caf::actor_system_config& cfg, symbol_map symbols,
          concepts_map concepts) {
  TENZIR_ASSERT(not initialized);
  auto& impl = get_impl();
  auto lock = std::unique_lock{impl.mutex};
  for (const auto& dir : get_module_dirs(cfg)) {
    const auto path = dir / "schema.bin";
    auto ec = std::error_code{};
    if (not std::filesystem::exists(path, ec)) {
      if (ec) {
        TENZIR_WARN("failed to load compiled schema from `{}`: {}", path,
                    ec.message());
      }
      continue;
    }
    const auto contents = io::read(path);
    if (not contents) {
      TENZIR_WARN("failed to read `{}`: {}", path, contents.error());
      continue;
    }
    auto bytes = as_bytes(*contents);
    while (bytes.size() != 0) {
      auto uncompressed_size = uint64_t{};
      if (bytes.size() < sizeof(uncompressed_size)) {
        TENZIR_WARN("invalid schema file `{}`: invalid uncompressed length",
                    path);
        break;
      }
      std::memcpy(&uncompressed_size, bytes.data(), sizeof(uncompressed_size));
      uncompressed_size = detail::to_host_order(uncompressed_size);
      bytes = bytes.subspan(sizeof(uncompressed_size));
      auto compressed_size = uint64_t{};
      if (bytes.size() < sizeof(compressed_size)) {
        TENZIR_WARN("invalid schema file `{}`: invalid compressed length",
                    path);
        break;
      }
      std::memcpy(&compressed_size, bytes.data(), sizeof(compressed_size));
      compressed_size = detail::to_host_order(compressed_size);
      bytes = bytes.subspan(sizeof(compressed_size));
      if (bytes.size() < compressed_size) {
        TENZIR_WARN("invalid schema file `{}`: invalid schema ({}/{} "
                    "remaining)",
                    path, bytes.size(), compressed_size);
        break;
      }
      auto fb = flatbuffer<fbs::Type>::make(check(chunk::decompress(
        bytes.subspan(0, compressed_size), uncompressed_size)));
      if (not fb) {
        TENZIR_WARN("invalid schema file `{}`: {}", path, fb.error());
        break;
      }
      auto ty = type{std::move(*fb)};
      const auto name = ty.name();
      impl.types.emplace(name, std::move(ty));
      bytes = bytes.subspan(compressed_size);
    }
  }
  for (auto& [name, ty] : symbols) {
    TENZIR_ASSERT(name == ty.name());
    if (is<legacy_record_type>(ty)) {
      impl.types.emplace(name, std::move(ty));
    }
  }
  get_impl().concepts = std::move(concepts);
  initialized = true;
}

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
    });
}

auto expensive_get_all_schemas() -> std::unordered_set<type> {
  auto& global = get_impl();
  auto result = std::unordered_set<type>{};
  // The critical section here is very small once the type has been converted.
  // This function should thus be fine to call outside of tight loops.
  auto lock = std::unique_lock{global.mutex};
  result.reserve(global.types.size());
  for (auto& [_, stored] : global.types) {
    match(
      stored,
      [&](const type& ty) {
        result.insert(ty);
      },
      [&](const legacy_type& legacy) {
        auto converted = type::from_legacy_type(legacy);
        stored = converted;
        result.insert(std::move(converted));
      });
  }
  return result;
}

// Get the concepts map.
// Returns an empty map if init(...) was not called.
auto concepts() -> const concepts_map& {
  return get_impl().concepts;
}

} // namespace tenzir::modules
