//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/plugin.hpp>

namespace tenzir::plugins::abs {

class registrar final : public plugin {
public:
  auto name() const -> std::string override {
    return "azure-blob-storage";
  }
};

} // namespace tenzir::plugins::abs

TENZIR_REGISTER_PLUGIN(tenzir::plugins::abs::registrar)
