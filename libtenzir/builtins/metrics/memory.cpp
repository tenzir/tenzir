//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/allocator.hpp"

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

auto make_from(const memory::stats& stats) -> record {
  auto result = record{};
  result.reserve(2);
  const auto [bytes_it, bytes_success] = result.try_emplace("bytes", record{});
  TENZIR_ASSERT_EXPENSIVE(bytes_success);
  auto& bytes = as<record>(bytes_it->second);
  bytes.reserve(3);
  bytes.try_emplace("current", stats.bytes_current.load());
  bytes.try_emplace("max", stats.bytes_max.load());
  bytes.try_emplace("total", stats.bytes_total.load());
  const auto [alloc_it, alloc_success]
    = result.try_emplace("allocations", record{});
  TENZIR_ASSERT_EXPENSIVE(alloc_success);
  auto& allocations = as<record>(alloc_it->second);
  allocations.reserve(3);
  allocations.try_emplace("current", stats.allocations_current.load());
  allocations.try_emplace("max", stats.allocations_max.load());
  allocations.try_emplace("total", stats.allocations_total.load());
  return result;
};

#ifdef _SC_AVPHYS_PAGES

auto get_raminfo() -> caf::expected<record> {
  static const auto pagesize = ::sysconf(_SC_PAGESIZE);
  const auto phys_pages = ::sysconf(_SC_PHYS_PAGES);
  const auto available_pages = ::sysconf(_SC_AVPHYS_PAGES);
  const auto total_bytes = phys_pages * pagesize;
  const auto free_bytes = available_pages * pagesize;
  auto result = record{};
  result.reserve(3 + 3);
  result.try_emplace("total_bytes", total_bytes);
  result.try_emplace("free_bytes", free_bytes);
  result.try_emplace("used_bytes", total_bytes - free_bytes);
  result.try_emplace("arrow", make_from(memory::arrow_allocator().stats()));
  result.try_emplace("cpp", make_from(memory::cpp_allocator().stats()));
  result.try_emplace("c", make_from(memory::c_allocator().stats()));
  return result;
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
  auto result = record{};
  result.reserve(3 + 3);
  result.try_emplace("total_bytes", total_bytes);
  result.try_emplace("free_bytes", free_bytes);
  result.try_emplace("used_bytes", total_bytes - free_bytes);
  result.try_emplace("arrow", make_from(memory::arrow_allocator().stats()));
  result.try_emplace("cpp", make_from(memory::cpp_allocator().stats()));
  result.try_emplace("c", make_from(memory::c_allocator().stats()));
  return result;
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
    const auto stats = record_type{
      {"current", int64_type{}},
      {"max", int64_type{}},
      {"total", int64_type{}},
    };
    const auto bytes_and_allocations = record_type{
      {"bytes", stats},
      {"allocations", stats},
    };
    return record_type{{
      {"total_bytes", uint64_type{}},
      {"free_bytes", uint64_type{}},
      {"used_bytes", uint64_type{}},
      {"cpp", bytes_and_allocations},
      {"arrow", bytes_and_allocations},
    }};
  }
};

} // namespace

} // namespace tenzir::plugins::health_memory

TENZIR_REGISTER_PLUGIN(tenzir::plugins::health_memory::plugin)
