//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/plugin.hpp>

namespace tenzir::plugins::ai {

namespace {

class registrar final : public plugin {
public:
  auto name() const -> std::string override {
    return "ai";
  }
};

} // namespace

} // namespace tenzir::plugins::ai

TENZIR_REGISTER_PLUGIN(tenzir::plugins::ai::registrar)
