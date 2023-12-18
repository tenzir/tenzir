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

namespace tenzir::plugins::health_memory {

namespace {

#ifdef TENZIR_LINUX

auto get_raminfo() -> caf::expected<record> {
  auto result = record{};
  static auto pagesize = ::sysconf(_SC_PAGESIZE);
  auto phys_pages = ::sysconf(_SC_PHYS_PAGES);
  auto available_pages = ::sysconf(_SC_AVPHYS_PAGES);
  auto total_bytes = phys_pages * pagesize;
  auto available_bytes = available_pages * pagesize;
  result["total_bytes"] = total_bytes;
  result["free_bytes"] = available_bytes;
  result["used_bytes"] = total_bytes - available_bytes;
  return result;
}

#endif

class plugin final : public virtual health_metrics_plugin {
public:
  auto name() const -> std::string override {
    return "health-memory";
  }

  auto make_collector() const -> caf::expected<collector> override {
#ifdef TENZIR_LINUX
    return get_raminfo;
#else
    return caf::make_error(ec::invalid_configuration,
                           "currently only supported on linux");
#endif
  }

  auto metric_name() const -> std::string override {
    return "mem";
  }

  auto metric_layout() const -> record_type override {
    return record_type{{
      {"total_bytes", uint64_type{}},
      {"free_bytes", uint64_type{}},
      {"used_bytes", uint64_type{}},
    }};
  }
};

} // namespace

} // namespace tenzir::plugins::health_memory

TENZIR_REGISTER_PLUGIN(tenzir::plugins::health_memory::plugin)
