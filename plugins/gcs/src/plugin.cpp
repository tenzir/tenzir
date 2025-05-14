//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/plugin.hpp>

namespace tenzir::plugins::gcs {

namespace {

class registrar final : public plugin {
public:
  auto initialize([[maybe_unused]] const record& plugin_config,
                  [[maybe_unused]] const record& global_config)
    -> caf::error override {
    return caf::none;
  }

  auto name() const -> std::string override {
    return "gcs";
  }
};

} // namespace
} // namespace tenzir::plugins::gcs

TENZIR_REGISTER_PLUGIN(tenzir::plugins::gcs::registrar)
