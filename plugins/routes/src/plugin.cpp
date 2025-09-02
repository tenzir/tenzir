//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors

#include <tenzir/plugin.hpp>

#include "routes/fwd.hpp"
#include "routes/config.hpp"
#include "routes/proxy_actor.hpp"

namespace tenzir::plugins::routes {

class routes_plugin final : public virtual plugin {
public:
  auto name() const -> std::string override {
    return "routes";
  };
};

} // namespace tenzir::plugins::routes

TENZIR_REGISTER_PLUGIN_TYPE_ID_BLOCK(routes_types)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::routes::routes_plugin)
