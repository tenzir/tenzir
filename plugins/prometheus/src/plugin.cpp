//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/plugin.hpp>
#include <tenzir/plugin/register.hpp>

namespace tenzir::plugins::prometheus {

class registrar final : public plugin {
public:
  auto name() const -> std::string override {
    return "prometheus";
  }
};

} // namespace tenzir::plugins::prometheus

TENZIR_REGISTER_PLUGIN(tenzir::plugins::prometheus::registrar)
