//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_memory_pool.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/type.hpp>

#include <caf/typed_event_based_actor.hpp>

#ifdef _SC_AVPHYS_PAGES
#  include <filesystem>
#elif __has_include(<mach/mach.h>)
#  include <mach/mach.h>
#  include <mach/mach_host.h>
#endif

namespace tenzir::plugins::health_memory {

namespace {

#ifdef _SC_AVPHYS_PAGES

auto get_raminfo() -> caf::expected<record> {
  static const auto pagesize = ::sysconf(_SC_PAGESIZE);
  const auto phys_pages = ::sysconf(_SC_PHYS_PAGES);
  const auto available_pages = ::sysconf(_SC_AVPHYS_PAGES);
  const auto total_bytes = phys_pages * pagesize;
  const auto free_bytes = available_pages * pagesize;
  auto* arrow_pool = arrow_memory_pool();
  return record{
    {"total_bytes", total_bytes},
    {"free_bytes", free_bytes},
    {"used_bytes", total_bytes - free_bytes},
    {"arrow_bytes", arrow_pool->bytes_allocated()},
    {"arrow_max_bytes", arrow_pool->max_memory()},
    {"arrow_total_bytes", arrow_pool->total_bytes_allocated()},
    {"arrow_allocations", arrow_pool->num_allocations()},
  };
}

#elif __has_include(<mach/mach.h>)

auto get_raminfo() -> caf::expected<record> {
  static const auto page_size = getpagesize();
  auto host_count = mach_msg_type_number_t{HOST_BASIC_INFO_COUNT};
  auto host = host_basic_info_data_t{};
  if (KERN_SUCCESS
      != host_info(mach_host_self(), HOST_BASIC_INFO, (host_info_t)&host,
                   &host_count)) {
    return caf::make_error(ec::system_error,
                           "failed to get mach host basic info");
  }
  auto vm_count = mach_msg_type_number_t{HOST_VM_INFO64_COUNT};
  auto vm = vm_statistics64_data_t{};
  if (KERN_SUCCESS
      != host_statistics64(mach_host_self(), HOST_VM_INFO64, (host_info_t)&vm,
                           &vm_count)) {
    return caf::make_error(ec::system_error,
                           "failed to get mach vm statistics");
  }
  const auto total_bytes = static_cast<uint64_t>(host.max_mem);
  const auto free_bytes
    = static_cast<uint64_t>(vm.free_count + vm.inactive_count) * page_size;
  return record{
    {"total_bytes", total_bytes},
    {"free_bytes", free_bytes},
    {"used_bytes", total_bytes - free_bytes},
  };
}

#endif

class plugin final : public virtual metrics_plugin {
public:
  auto name() const -> std::string override {
    return "memory";
  }

  auto make_collector(caf::actor_system&) const
    -> caf::expected<collector> override {
#ifdef _SC_AVPHYS_PAGES
    return get_raminfo;
#elif __has_include(<mach/mach.h>)
    return get_raminfo;
#else
    return caf::make_error(ec::invalid_configuration,
                           "not supported on this platform");
#endif
  }

  auto metric_name() const -> std::string override {
    return "memory";
  }

  auto metric_layout() const -> record_type override {
    return record_type{{
      {"total_bytes", uint64_type{}},
      {"free_bytes", uint64_type{}},
      {"used_bytes", uint64_type{}},
      {"arrow_bytes", int64_type{}},
      {"arrow_max_bytes", int64_type{}},
      {"arrow_total_bytes", int64_type{}},
      {"arrow_allocations", int64_type{}},
    }};
  }
};

} // namespace

} // namespace tenzir::plugins::health_memory

TENZIR_REGISTER_PLUGIN(tenzir::plugins::health_memory::plugin)
