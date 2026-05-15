//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/plugin.hpp>

namespace tenzir::plugins::cloudwatch {

namespace {

class registrar final : public plugin {
public:
  auto name() const -> std::string override {
    return "cloudwatch";
  }
};

} // namespace

} // namespace tenzir::plugins::cloudwatch

TENZIR_REGISTER_PLUGIN(tenzir::plugins::cloudwatch::registrar)
