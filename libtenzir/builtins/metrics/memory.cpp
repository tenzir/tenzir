//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/allocator.hpp>
#include <tenzir/arrow_memory_pool.hpp>
#include <tenzir/config.hpp>
#include <tenzir/os.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/type.hpp>

#include <caf/typed_event_based_actor.hpp>

#include <atomic>
#include <charconv>
#include <cctype>
#include <cstdint>
#include <fstream>
#include <optional>
#include <string>
#include <string_view>

#ifdef _SC_AVPHYS_PAGES
#  include <filesystem>
#elif __has_include(<mach/mach.h>)
#  include <mach/mach.h>
#  include <mach/mach_host.h>
#endif

#if defined(__GLIBC__)
#  include <features.h>
#  include <malloc.h>
#endif

#if TENZIR_LINUX
#  include <unistd.h>
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
  bytes.try_emplace("current",
                    stats.bytes_current.load(std::memory_order_relaxed));
  bytes.try_emplace("peak", stats.bytes_peak.load(std::memory_order_relaxed));
  bytes.try_emplace("cumulative",
                    stats.bytes_cumulative.load(std::memory_order_relaxed));
  const auto [alloc_it, alloc_success]
    = result.try_emplace("allocations", record{});
  TENZIR_ASSERT_EXPENSIVE(alloc_success);
  auto& allocations = as<record>(alloc_it->second);
  allocations.reserve(3);
  allocations.try_emplace(
    "current", stats.allocations_current.load(std::memory_order_relaxed));
  allocations.try_emplace(
    "peak", stats.allocations_peak.load(std::memory_order_relaxed));
  allocations.try_emplace(
    "total", stats.allocations_cumulative.load(std::memory_order_relaxed));
  return result;
};

auto make_process_statistics() -> record {
  static auto os = os::make();
  auto result = record{};
  if (not os) {
    return result;
  }
  const auto process = os->current_process();
  result.reserve(3);
  auto assign_optional = [&result](std::string_view key, auto value) {
    if (value) {
      result.try_emplace(key, *value);
    } else {
      result.try_emplace(key, caf::none);
    }
  };
  // peak mem is in kb
  assign_optional("peak_bytes", process.peak_mem.transform([](const auto v) {
    return v * 1000;
  }));
  assign_optional("current_bytes", process.rsize);
  assign_optional("swap_bytes", process.swap);
  return result;
}

#ifdef _SC_AVPHYS_PAGES

auto make_system_info() -> record {
  static const auto pagesize = ::sysconf(_SC_PAGESIZE);
  const auto phys_pages = ::sysconf(_SC_PHYS_PAGES);
  const auto available_pages = ::sysconf(_SC_AVPHYS_PAGES);
  const auto total_bytes = phys_pages * pagesize;
  const auto free_bytes = available_pages * pagesize;
  auto result = record{};
  result.reserve(3);
  result.try_emplace("total_bytes", total_bytes);
  result.try_emplace("free_bytes", free_bytes);
  result.try_emplace("used_bytes", total_bytes - free_bytes);
  return result;
}

#elif __has_include(<mach/mach.h>)

auto make_system_info() -> record {
  static const auto page_size = getpagesize();
  auto host_count = mach_msg_type_number_t{HOST_BASIC_INFO_COUNT};
  auto host = host_basic_info_data_t{};
  if (KERN_SUCCESS
      != host_info(mach_host_self(), HOST_BASIC_INFO, (host_info_t)&host,
                   &host_count)) {
    auto result = record{};
    result.reserve(3);
    result.try_emplace("total_bytes", caf::none);
    result.try_emplace("free_bytes", caf::none);
    result.try_emplace("used_bytes", caf::none);
    return result;
  }
  auto vm_count = mach_msg_type_number_t{HOST_VM_INFO64_COUNT};
  auto vm = vm_statistics64_data_t{};
  if (KERN_SUCCESS
      != host_statistics64(mach_host_self(), HOST_VM_INFO64, (host_info_t)&vm,
                           &vm_count)) {
    auto result = record{};
    result.reserve(3);
    result.try_emplace("total_bytes", caf::none);
    result.try_emplace("free_bytes", caf::none);
    result.try_emplace("used_bytes", caf::none);
    return result;
  }
  const auto total_bytes = static_cast<uint64_t>(host.max_mem);
  const auto free_bytes
    = static_cast<uint64_t>(vm.free_count + vm.inactive_count) * page_size;
  auto result = record{};
  result.reserve(3);
  result.try_emplace("total_bytes", total_bytes);
  result.try_emplace("free_bytes", free_bytes);
  result.try_emplace("used_bytes", total_bytes - free_bytes);
  return result;
}

#endif

auto make_chunk_info() -> record {
  return record{
    {"bytes", chunk::get_bytes()},
    {"count", chunk::get_count()},
  };
}

auto make_allocator_metrics() -> record {
  auto result = record{};
  result.reserve(7);
  for (const auto* key : {
         "arena_bytes",
         "uordblks_bytes",
         "fordblks_bytes",
         "keepcost_bytes",
         "hblkhd_bytes",
         "ordblks_count",
         "smblks_count",
       }) {
    result.try_emplace(key, caf::none);
  }
#if defined(__GLIBC__)
  auto set_field = [&result](std::string_view key, uint64_t value) {
    if (auto it = result.find(std::string{key}); it != result.end()) {
      it->second = value;
    }
  };
#  if defined(__GLIBC_PREREQ) && __GLIBC_PREREQ(2, 33)
  const auto info = ::mallinfo2();
#  else
  const auto info = ::mallinfo();
#  endif
  set_field("arena_bytes", static_cast<uint64_t>(info.arena));
  set_field("uordblks_bytes", static_cast<uint64_t>(info.uordblks));
  set_field("fordblks_bytes", static_cast<uint64_t>(info.fordblks));
  set_field("keepcost_bytes", static_cast<uint64_t>(info.keepcost));
  set_field("hblkhd_bytes", static_cast<uint64_t>(info.hblkhd));
  set_field("ordblks_count", static_cast<uint64_t>(info.ordblks));
  set_field("smblks_count", static_cast<uint64_t>(info.smblks));
#endif
  return result;
}

#if TENZIR_LINUX
auto parse_proc_kb_value(std::string_view line, std::string_view key)
  -> std::optional<uint64_t> {
  if (!line.starts_with(key)) {
    return std::nullopt;
  }
  const auto key_length = key.size();
  if (line.size() <= key_length || line[key_length] != ':') {
    return std::nullopt;
  }
  auto rest = line.substr(key_length + 1);
  while (!rest.empty()
         && std::isspace(static_cast<unsigned char>(rest.front()))) {
    rest.remove_prefix(1);
  }
  const auto delimiter = rest.find_first_of(" \t");
  const auto number_sv
    = delimiter == std::string_view::npos ? rest : rest.substr(0, delimiter);
  if (number_sv.empty()) {
    return std::nullopt;
  }
  uint64_t value = 0;
  const auto parsed = std::from_chars(
    number_sv.data(), number_sv.data() + number_sv.size(), value);
  if (parsed.ec != std::errc{}) {
    return std::nullopt;
  }
  if (delimiter != std::string_view::npos) {
    auto unit = rest.substr(delimiter);
    while (!unit.empty()
           && std::isspace(static_cast<unsigned char>(unit.front()))) {
      unit.remove_prefix(1);
    }
    if (unit.starts_with("kB") || unit.starts_with("KB")) {
      value *= 1024;
    }
  }
  return value;
}
#endif

auto make_procfs_metrics() -> record {
  auto make_status_record = [] {
    auto status = record{};
    status.reserve(6);
    status.try_emplace("vm_rss_bytes", caf::none);
    status.try_emplace("vm_data_bytes", caf::none);
    status.try_emplace("vm_swap_bytes", caf::none);
    status.try_emplace("rss_anon_bytes", caf::none);
    status.try_emplace("file_rss_bytes", caf::none);
    status.try_emplace("rss_shmem_bytes", caf::none);
    return status;
  };
  auto make_smaps_record = [] {
    auto smaps = record{};
    smaps.reserve(7);
    smaps.try_emplace("rss_bytes", caf::none);
    smaps.try_emplace("pss_bytes", caf::none);
    smaps.try_emplace("private_clean_bytes", caf::none);
    smaps.try_emplace("private_dirty_bytes", caf::none);
    smaps.try_emplace("anonymous_rss_bytes", caf::none);
    smaps.try_emplace("swap_bytes", caf::none);
    smaps.try_emplace("hugetlb_bytes", caf::none);
    return smaps;
  };
  auto make_heap_record = [] {
    auto heap = record{};
    heap.reserve(1);
    heap.try_emplace("program_break_bytes", caf::none);
    return heap;
  };
  auto status = make_status_record();
  auto smaps = make_smaps_record();
  auto heap = make_heap_record();
#if TENZIR_LINUX
  auto set_record_field = [](record& rec, std::string_view key,
                             uint64_t value) {
    if (auto it = rec.find(std::string{key}); it != rec.end()) {
      it->second = value;
    } else {
      rec.emplace(std::string{key}, value);
    }
  };
  auto add_value = [](std::optional<uint64_t>& target, uint64_t value) {
    if (target) {
      *target += value;
    } else {
      target = value;
    }
  };
  auto assign_value = [](std::optional<uint64_t>& target, uint64_t value) {
    target = value;
  };
  std::optional<uint64_t> rss;
  std::optional<uint64_t> pss;
  std::optional<uint64_t> private_clean;
  std::optional<uint64_t> private_dirty;
  std::optional<uint64_t> anonymous_bytes;
  std::optional<uint64_t> swap;
  std::optional<uint64_t> shared_hugetlb;
  std::optional<uint64_t> private_hugetlb;
  std::optional<uint64_t> hugetlb_total;
  auto parse_smaps_stream = [&](std::istream& stream) {
    std::string line;
    while (std::getline(stream, line)) {
      const auto view = std::string_view{line};
      if (const auto value = parse_proc_kb_value(view, "Rss")) {
        add_value(rss, *value);
      } else if (const auto value = parse_proc_kb_value(view, "Pss")) {
        add_value(pss, *value);
      } else if (const auto value = parse_proc_kb_value(view, "Private_Clean")) {
        add_value(private_clean, *value);
      } else if (const auto value = parse_proc_kb_value(view, "Private_Dirty")) {
        add_value(private_dirty, *value);
      } else if (const auto value = parse_proc_kb_value(view, "Anonymous")) {
        add_value(anonymous_bytes, *value);
      } else if (const auto value = parse_proc_kb_value(view, "Swap")) {
        add_value(swap, *value);
      } else if (const auto value
                 = parse_proc_kb_value(view, "Shared_Hugetlb")) {
        add_value(shared_hugetlb, *value);
      } else if (const auto value
                 = parse_proc_kb_value(view, "Private_Hugetlb")) {
        add_value(private_hugetlb, *value);
      } else if (const auto value = parse_proc_kb_value(view, "Hugetlb")) {
        add_value(hugetlb_total, *value);
      }
    }
  };
  if (std::ifstream smaps_rollup{"/proc/self/smaps_rollup"}) {
    parse_smaps_stream(smaps_rollup);
  } else if (std::ifstream smaps_file{"/proc/self/smaps"}) {
    parse_smaps_stream(smaps_file);
  }
  if (rss) {
    set_record_field(smaps, "rss_bytes", *rss);
  }
  if (pss) {
    set_record_field(smaps, "pss_bytes", *pss);
  }
  if (private_clean) {
    set_record_field(smaps, "private_clean_bytes", *private_clean);
  }
  if (private_dirty) {
    set_record_field(smaps, "private_dirty_bytes", *private_dirty);
  }
  if (anonymous_bytes) {
    set_record_field(smaps, "anonymous_rss_bytes", *anonymous_bytes);
  }
  if (swap) {
    set_record_field(smaps, "swap_bytes", *swap);
  }
  std::optional<uint64_t> hugetlb_bytes;
  if (shared_hugetlb) {
    add_value(hugetlb_bytes, *shared_hugetlb);
  }
  if (private_hugetlb) {
    add_value(hugetlb_bytes, *private_hugetlb);
  }
  if (!hugetlb_bytes && hugetlb_total) {
    assign_value(hugetlb_bytes, *hugetlb_total);
  }
  if (hugetlb_bytes) {
    set_record_field(smaps, "hugetlb_bytes", *hugetlb_bytes);
  }
  std::optional<uint64_t> vm_rss;
  std::optional<uint64_t> vm_data;
  std::optional<uint64_t> vm_swap;
  std::optional<uint64_t> rss_anon;
  std::optional<uint64_t> rss_file;
  std::optional<uint64_t> rss_shmem;
  if (std::ifstream status_file{"/proc/self/status"}) {
    std::string line;
    while (std::getline(status_file, line)) {
      const auto view = std::string_view{line};
      if (const auto value = parse_proc_kb_value(view, "VmRSS")) {
        assign_value(vm_rss, *value);
      } else if (const auto value = parse_proc_kb_value(view, "VmData")) {
        assign_value(vm_data, *value);
      } else if (const auto value = parse_proc_kb_value(view, "VmSwap")) {
        assign_value(vm_swap, *value);
      } else if (const auto value = parse_proc_kb_value(view, "RssAnon")) {
        assign_value(rss_anon, *value);
      } else if (const auto value = parse_proc_kb_value(view, "RssFile")) {
        assign_value(rss_file, *value);
      } else if (const auto value = parse_proc_kb_value(view, "RssShmem")) {
        assign_value(rss_shmem, *value);
      }
    }
  }
  if (vm_rss) {
    set_record_field(status, "vm_rss_bytes", *vm_rss);
    if (!rss) {
      set_record_field(smaps, "rss_bytes", *vm_rss);
    }
  }
  if (vm_data) {
    set_record_field(status, "vm_data_bytes", *vm_data);
  }
  if (vm_swap) {
    set_record_field(status, "vm_swap_bytes", *vm_swap);
    if (!swap) {
      set_record_field(smaps, "swap_bytes", *vm_swap);
    }
  }
  if (rss_anon) {
    set_record_field(status, "rss_anon_bytes", *rss_anon);
    if (!anonymous_bytes) {
      set_record_field(smaps, "anonymous_rss_bytes", *rss_anon);
    }
  }
  if (rss_file) {
    set_record_field(status, "file_rss_bytes", *rss_file);
  }
  if (rss_shmem) {
    set_record_field(status, "rss_shmem_bytes", *rss_shmem);
  }
  if (auto* program_break = ::sbrk(0);
      program_break && program_break != reinterpret_cast<void*>(-1)) {
    set_record_field(heap, "program_break_bytes",
                     static_cast<uint64_t>(
                       reinterpret_cast<std::uintptr_t>(program_break)));
  }
#endif
  auto result = record{};
  result.reserve(3);
  result.try_emplace("status", std::move(status));
  result.try_emplace("smaps", std::move(smaps));
  result.try_emplace("heap", std::move(heap));
  return result;
}

auto get_raminfo() -> caf::expected<record> {
  auto result = record{};
  result.reserve(9);
  result.try_emplace("system", make_system_info());
  result.try_emplace("process", make_process_statistics());
  result.try_emplace("procfs", make_procfs_metrics());
  result.try_emplace("arrow", make_from(memory::arrow_allocator().stats()));
  result.try_emplace("cpp", make_from(memory::cpp_allocator().stats()));
  result.try_emplace("c", make_from(memory::c_allocator().stats()));
  result.try_emplace("malloc", make_allocator_metrics());
  const auto table_slice_stats = table_slice::memory_stats();
  auto table_slice_record = record{};
  table_slice_record.reserve(3);
  table_slice_record.try_emplace("serialized",
                                 table_slice_stats.serialized_bytes);
  table_slice_record.try_emplace("non_serialized",
                                 table_slice_stats.non_serialized_bytes);
  table_slice_record.try_emplace("count", table_slice_stats.instances);
  result.try_emplace("table_slices", std::move(table_slice_record));
  result.try_emplace("chunk", make_chunk_info());
  return result;
}

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
      {"peak", int64_type{}},
      {"cumulative", int64_type{}},
    };
    const auto bytes_and_allocations = record_type{
      {"bytes", stats},
      {"allocations", stats},
    };
    const auto table_slice_stats = record_type{
      {"serialized", int64_type{}},
      {"non_serialized", int64_type{}},
      {"count", int64_type{}},
    };
    const auto bytes_and_count = record_type{
      {"bytes", int64_type{}},
      {"count", int64_type{}},
    };
    const auto procfs_status = record_type{
      {"vm_rss_bytes", uint64_type{}},
      {"vm_data_bytes", uint64_type{}},
      {"vm_swap_bytes", uint64_type{}},
      {"rss_anon_bytes", uint64_type{}},
      {"file_rss_bytes", uint64_type{}},
      {"rss_shmem_bytes", uint64_type{}},
    };
    const auto procfs_smaps = record_type{
      {"rss_bytes", uint64_type{}},
      {"pss_bytes", uint64_type{}},
      {"private_clean_bytes", uint64_type{}},
      {"private_dirty_bytes", uint64_type{}},
      {"anonymous_rss_bytes", uint64_type{}},
      {"swap_bytes", uint64_type{}},
      {"hugetlb_bytes", uint64_type{}},
    };
    const auto procfs_heap = record_type{
      {"program_break_bytes", uint64_type{}},
    };
    const auto procfs = record_type{
      {"status", procfs_status},
      {"smaps", procfs_smaps},
      {"heap", procfs_heap},
    };
    const auto malloc_stats = record_type{
      {"arena_bytes", uint64_type{}},
      {"uordblks_bytes", uint64_type{}},
      {"fordblks_bytes", uint64_type{}},
      {"keepcost_bytes", uint64_type{}},
      {"hblkhd_bytes", uint64_type{}},
      {"ordblks_count", uint64_type{}},
      {"smblks_count", uint64_type{}},
    };
    return record_type{{
      {"system",
       record_type{
         {"total_bytes", uint64_type{}},
         {"free_bytes", uint64_type{}},
         {"used_bytes", uint64_type{}},
       }},
      {"process",
       record_type{
         {"peak_bytes", uint64_type{}},
         {"current_bytes", uint64_type{}},
         {"swap_bytes", uint64_type{}},
       }},
      {"procfs", procfs},
      {"arrow", bytes_and_allocations},
      {"cpp", bytes_and_allocations},
      {"c", bytes_and_allocations},
      {"malloc", malloc_stats},
      {"table_slices", table_slice_stats},
      {"chunk", bytes_and_count},
    }};
  }
};

} // namespace

} // namespace tenzir::plugins::health_memory

TENZIR_REGISTER_PLUGIN(tenzir::plugins::health_memory::plugin)
