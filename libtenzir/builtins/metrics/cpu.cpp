//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/plugin.hpp>
#include <tenzir/type.hpp>

#include <caf/typed_event_based_actor.hpp>

#include <cstdlib>

namespace tenzir::plugins::health_cpu {

namespace {

auto get_cpuinfo() -> caf::expected<record> {
  auto loadavg = std::array<double, 3>{0.0, 0.0, 0.0};
  // `getloadavg()` returns the number of samples retrieved,
  // but if it's less than three we use 0 as a placehold to
  // keep the layout uniform.
  auto error = ::getloadavg(loadavg.data(), loadavg.size());
  if (error == -1) {
    return caf::make_error(ec::system_error, "failed to get cpu load average");
  }
  auto result = record{};
  result["loadavg_1m"] = loadavg[0];
  result["loadavg_5m"] = loadavg[1];
  result["loadavg_15m"] = loadavg[2];
  return result;
}

class plugin final : public virtual metrics_plugin {
public:
  auto name() const -> std::string override {
    return "cpu";
  }

  auto metric_name() const -> std::string override {
    return "cpu";
  }

  auto make_collector() const -> caf::expected<collector> override {
    return get_cpuinfo;
  }

  auto metric_layout() const -> record_type override {
    return record_type{{
      {"loadavg_1m", double_type{}},
      {"loadavg_5m", double_type{}},
      {"loadavg_15m", double_type{}},
    }};
  }
};

} // namespace

} // namespace tenzir::plugins::health_cpu

TENZIR_REGISTER_PLUGIN(tenzir::plugins::health_cpu::plugin)
