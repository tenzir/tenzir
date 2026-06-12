//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/plugin.hpp>

namespace tenzir::plugins::velociraptor {

namespace {

class plugin final : public tenzir::plugin {
public:
  auto name() const -> std::string override {
    return "from_velociraptor";
  }
};

} // namespace

} // namespace tenzir::plugins::velociraptor

TENZIR_REGISTER_PLUGIN(tenzir::plugins::velociraptor::plugin)
