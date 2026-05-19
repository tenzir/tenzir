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
#include <string_view>
#include <vector>

namespace tenzir {

namespace metrics {

inline constexpr auto prometheus_role_attribute = "prometheus.role";
inline constexpr auto prometheus_type_attribute = "prometheus.type";
inline constexpr auto prometheus_unit_attribute = "prometheus.unit";

/// Marks a metric field as a Prometheus label.
template <type_or_concrete_type Type>
[[nodiscard]] auto prometheus_label(Type const& nested) -> type {
  return type{nested, {{prometheus_role_attribute, "label"}}};
}

/// Marks a metric field or subtree as ignored by Prometheus shaping.
template <type_or_concrete_type Type>
[[nodiscard]] auto prometheus_ignore(Type const& nested) -> type {
  return type{nested, {{prometheus_role_attribute, "ignore"}}};
}

template <type_or_concrete_type Type>
[[nodiscard]] auto
prometheus_metric(Type const& nested, std::string_view prometheus_type,
                  std::string_view unit = {}) -> type {
  auto attributes = std::vector<type::attribute_view>{
    {prometheus_role_attribute, "metric"},
    {prometheus_type_attribute, prometheus_type},
  };
  if (not unit.empty()) {
    attributes.push_back({prometheus_unit_attribute, unit});
  }
  return type{nested, std::move(attributes)};
}

/// Marks a metric field as a Prometheus gauge.
template <type_or_concrete_type Type>
[[nodiscard]] auto
prometheus_gauge(Type const& nested, std::string_view unit = {}) -> type {
  return prometheus_metric(nested, "gauge", unit);
}

/// Marks a metric field as a Prometheus counter.
template <type_or_concrete_type Type>
[[nodiscard]] auto
prometheus_counter(Type const& nested, std::string_view unit = {}) -> type {
  return prometheus_metric(nested, "counter", unit);
}

} // namespace metrics

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
