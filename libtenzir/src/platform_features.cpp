//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/platform_features.hpp"

#include <mutex>
#include <shared_mutex>

namespace tenzir {

namespace {

std::shared_mutex g_platform_features_mutex;
std::unordered_set<std::string> g_platform_features;

} // namespace

auto set_platform_features(std::unordered_set<std::string> features) -> void {
  auto lock = std::unique_lock{g_platform_features_mutex};
  g_platform_features = std::move(features);
}

auto platform_supports_feature(std::string_view feature) -> bool {
  auto lock = std::shared_lock{g_platform_features_mutex};
  return g_platform_features.contains(std::string{feature});
}

} // namespace tenzir
