//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/plugin.hpp>

namespace tenzir::plugins::nats {

class registrar final : public plugin {
public:
  auto initialize(record const&, record const& global_config)
    -> caf::error override;

  auto name() const -> std::string override {
    return "nats";
  }
};

} // namespace tenzir::plugins::nats

#include "tenzir_plugins/nats/common.hpp"

namespace tenzir::plugins::nats {

auto registrar::initialize(record const&, record const& global_config)
  -> caf::error {
  initialize_folly_event_loop_adapter();
  return set_default_url_from_global_config(global_config, name());
}

} // namespace tenzir::plugins::nats

TENZIR_REGISTER_PLUGIN(tenzir::plugins::nats::registrar)
