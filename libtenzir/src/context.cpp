//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/context.hpp"

namespace tenzir {

auto context_plugin::get_latest_loader() const -> const context_loader& {
  TENZIR_ASSERT(not loaders_.empty());
  return **std::ranges::max_element(loaders_, std::ranges::less{},
                                    [](const auto& loader) {
                                      return loader->version();
                                    });
}

auto context_plugin::get_versioned_loader(int version) const
  -> const context_loader* {
  auto it = std::ranges::find(loaders_, version, [](const auto& loader) {
    return loader->version();
  });
  if (it == loaders_.end()) {
    return nullptr;
  }
  return it->get();
}

void context_plugin::register_loader(std::unique_ptr<context_loader> loader) {
  loaders_.emplace_back(std::move(loader));
}

} // namespace tenzir
