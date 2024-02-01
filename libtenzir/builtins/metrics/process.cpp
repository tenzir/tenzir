//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/os.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/type.hpp>

#include <caf/typed_event_based_actor.hpp>

#include <filesystem>

namespace tenzir::plugins::health_process {

namespace {

auto get_process_statistics() -> caf::expected<record> {
  auto result = record{};
  auto os = os::make();
  if (!os)
    return caf::make_error(ec::system_error, "failed to create os");
  auto process = os->current_process();
  auto assign_optional = [&result](std::string_view key, auto value) {
    if (value)
      result[key] = *value;
    else
      result[key] = caf::none;
  };
  assign_optional("swap_space_usage", process.swap);
  assign_optional("open_fds", process.open_fds);
  assign_optional("peak_memory_usage", process.peak_mem);
  assign_optional("current_memory_usage", process.rsize);
  return result;
}

class plugin final : public virtual metrics_plugin {
public:
  auto name() const -> std::string override {
    return "process";
  }

  auto make_collector() const -> caf::expected<collector> override {
    return get_process_statistics;
  }

  auto metric_name() const -> std::string override {
    return "process";
  }

  auto metric_layout() const -> record_type override {
    return record_type{{
      {"swap_space_usage", uint64_type{}},
      {"open_fds", uint64_type{}},
      {"current_memory_usage", uint64_type{}},
      {"peak_memory_usage", uint64_type{}},
    }};
  }
};

} // namespace

} // namespace tenzir::plugins::health_process

TENZIR_REGISTER_PLUGIN(tenzir::plugins::health_process::plugin)
