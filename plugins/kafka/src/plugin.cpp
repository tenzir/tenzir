//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/data.hpp>
#include <tenzir/error.hpp>
#include <tenzir/plugin.hpp>

#include <string>

namespace tenzir::plugins::kafka {
namespace {

class registrar final : public plugin {
public:
  auto initialize(const record&, const record&) -> caf::error override {
    return caf::none;
  }

  auto name() const -> std::string override {
    return "kafka";
  }
};

} // namespace

} // namespace tenzir::plugins::kafka

TENZIR_REGISTER_PLUGIN(tenzir::plugins::kafka::registrar)
