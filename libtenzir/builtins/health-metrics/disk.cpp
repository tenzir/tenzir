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
  auto spaceinfo = std::filesystem::space(path);
  // TODO: Find the mount point and/or device name if possible.
  result["name"] = path;
  result["total-bytes"] = spaceinfo.capacity;
  result["free-bytes"] = spaceinfo.free;
  result["used-bytes"] = spaceinfo.capacity - spaceinfo.free;
  return result;
}

class plugin final : public virtual health_metrics_plugin {
public:
  auto initialize(const record& config, const record& /*plugin_config*/)
    -> caf::error override {
    dbdir_ = get_or(config, "tenzir.db-directory", defaults::db_directory);
    return {};
  }

  auto name() const -> std::string override {
    return "health-disk";
  }

  auto metric_name() const -> std::string override {
    return "disk";
  }

  auto make_collector() const -> collector override {
    return [dbdir = dbdir_]() {
      return get_diskspace_info(dbdir);
    };
  }

  auto metric_layout() const -> record_type override {
    return record_type{{
      {"name", string_type{}},
      {"total_bytes", uint64_type{}},
      {"used_bytes", uint64_type{}},
    }};
  }

private:
  std::string dbdir_ = {};
};

} // namespace

} // namespace tenzir::plugins::health_disk

TENZIR_REGISTER_PLUGIN(tenzir::plugins::health_disk::plugin)
