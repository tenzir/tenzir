//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/detail/process.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/type.hpp>

#include <caf/typed_event_based_actor.hpp>

#include <filesystem>

namespace tenzir::plugins::health_process {

namespace {

auto get_raminfo() -> caf::expected<record> {
  return detail::get_status();
}

class plugin final : public virtual health_metrics_plugin {
public:
  auto name() const -> std::string override {
    return "health-process";
  }

  auto make_collector() const -> caf::expected<collector> override {
    return get_raminfo;
  }

  auto metric_name() const -> std::string override {
    return "process";
  }

  auto metric_layout() const -> record_type override {
    return record_type{{
#ifdef TENZIR_LINUX
      {"swap-space-usage", uint64_type{}},
#endif
#if defined(TENZIR_LINUX) || defined(TENZIR_MACOS)
      {"current-memory-usage", uint64_type{}},
#endif
#ifdef TENZIR_POSIX
      {"peak-memory-usage", uint64_type{}},
#endif
    }};
  }
};

} // namespace

} // namespace tenzir::plugins::health_process

TENZIR_REGISTER_PLUGIN(tenzir::plugins::health_process::plugin)
