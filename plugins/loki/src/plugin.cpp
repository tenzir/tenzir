//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/plugin.hpp>
#include <tenzir/plugin/register.hpp>

namespace tenzir::plugins::loki {

class registrar final : public plugin {
public:
  auto name() const -> std::string override {
    return "loki";
  }
};

} // namespace tenzir::plugins::loki

TENZIR_REGISTER_PLUGIN(tenzir::plugins::loki::registrar)
