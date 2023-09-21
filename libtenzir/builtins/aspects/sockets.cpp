//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/os.hpp>
#include <tenzir/plugin.hpp>

namespace tenzir::plugins::sockets {

namespace {

class plugin final : public virtual aspect_plugin {
public:
  auto name() const -> std::string override {
    return "sockets";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto show(operator_control_plane& ctrl) const
    -> generator<table_slice> override {
    auto system = os::make();
    if (not system) {
      diagnostic::error("failed to create OS shim").emit(ctrl.diagnostics());
      co_return;
    }
    co_yield system->sockets();
  }
};

} // namespace

} // namespace tenzir::plugins::sockets

TENZIR_REGISTER_PLUGIN(tenzir::plugins::sockets::plugin)
