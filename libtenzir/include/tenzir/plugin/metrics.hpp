//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/data.hpp"
#include "tenzir/plugin/base.hpp"
#include "tenzir/type.hpp"

#include <caf/expected.hpp>
#include <caf/fwd.hpp>

#include <chrono>
#include <functional>
#include <string>

namespace tenzir {

// -- metrics plugin ----------------------------------------------------------

class metrics_plugin : public virtual plugin {
public:
  using collector = std::function<caf::expected<record>()>;

  /// The name under which this metric should be displayed.
  [[nodiscard]] virtual auto metric_name() const -> std::string {
    return name();
  }

  /// The format in which metrics will be reported by this plugin.
  [[nodiscard]] virtual auto metric_layout() const -> record_type = 0;

  /// Create a metrics collector.
  /// Plugins may return an error if the collector is not supported on the
  /// platform the node is currently running on.
  [[nodiscard]] virtual auto make_collector(caf::actor_system& system) const
    -> caf::expected<collector>
    = 0;

  /// Returns the frequency for collecting the metrics, expressed as the
  /// interval between calls to the collector.
  [[nodiscard]] virtual auto metric_frequency() const -> duration {
    return std::chrono::seconds{1};
  }
};

} // namespace tenzir
