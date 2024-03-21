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

#include <filesystem>

namespace tenzir::plugins::health_disk {

namespace {

auto get_diskspace_info(const std::string& path) -> caf::expected<record> {
  auto result = record{};
  auto ec = std::error_code{};
  auto spaceinfo = std::filesystem::space(path, ec);
  if (ec)
    return caf::make_error(ec::system_error, fmt::to_string(ec));
  // TODO: Find the mount point and/or device name if possible.
  result["path"] = path;
  result["total_bytes"] = spaceinfo.capacity;
  result["free_bytes"] = spaceinfo.free;
  result["used_bytes"] = spaceinfo.capacity - spaceinfo.free;
  return result;
}

class plugin final : public virtual metrics_plugin {
public:
  auto initialize(const record& /*plugin_config*/, const record& global_config)
    -> caf::error override {
    state_directory_ = get_or(global_config, "tenzir.state-directory",
                              defaults::state_directory);
    return {};
  }

  auto name() const -> std::string override {
    return "disk";
  }

  auto metric_name() const -> std::string override {
    return "disk";
  }

  auto make_collector() const -> caf::expected<collector> override {
    return [state_directory = state_directory_]() {
      return get_diskspace_info(state_directory);
    };
  }

  auto metric_layout() const -> record_type override {
    return record_type{{
      {"path", string_type{}},
      {"total_bytes", uint64_type{}},
      {"used_bytes", uint64_type{}},
      {"free_bytes", uint64_type{}},
    }};
  }

private:
  std::string state_directory_ = {};
};

} // namespace

} // namespace tenzir::plugins::health_disk

TENZIR_REGISTER_PLUGIN(tenzir::plugins::health_disk::plugin)
