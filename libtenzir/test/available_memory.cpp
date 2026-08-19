//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/available_memory.hpp"

#include "tenzir/test/test.hpp"
#include "tenzir/uuid.hpp"

#include <fmt/format.h>

#include <fstream>
#include <string_view>

namespace tenzir {

namespace {

/// A throwaway directory holding cgroup control files.
class FakeCgroup {
public:
  FakeCgroup()
    : dir_{std::filesystem::temp_directory_path()
           / fmt::format("tnz-test-cgroup-{}", uuid::random())} {
    std::filesystem::create_directories(dir_);
  }

  ~FakeCgroup() {
    auto error = std::error_code{};
    std::filesystem::remove_all(dir_, error);
  }

  FakeCgroup(FakeCgroup const&) = delete;
  FakeCgroup(FakeCgroup&&) = delete;
  auto operator=(FakeCgroup const&) -> FakeCgroup& = delete;
  auto operator=(FakeCgroup&&) -> FakeCgroup& = delete;

  auto write(std::string_view name, std::string_view content) const
    -> FakeCgroup const& {
    auto file = std::ofstream{dir_ / name};
    file << content;
    return *this;
  }

  auto available() const -> Option<uint64_t> {
    return detail::read_cgroup_memory_available(dir_);
  }

private:
  std::filesystem::path dir_;
};

TEST("no cgroup limit yields no reading") {
  CHECK_EQUAL(FakeCgroup{}.available(), None{});
  CHECK_EQUAL(FakeCgroup{}.write("memory.current", "1000").available(), None{});
  CHECK_EQUAL(FakeCgroup{}
                .write("memory.current", "1000")
                .write("memory.max", "max")
                .available(),
              None{});
}

TEST("charged memory without a memory.stat counts in full") {
  CHECK_EQUAL(FakeCgroup{}
                .write("memory.current", "400")
                .write("memory.max", "1000")
                .available(),
              600u);
}

TEST("reclaimable page cache does not count as consumed") {
  // Of the 900 charged bytes, 700 are clean page cache and 100 are reclaimable
  // slab, leaving 100 bytes genuinely consumed out of a 1000 byte limit.
  auto const cgroup = FakeCgroup{};
  cgroup.write("memory.current", "900")
    .write("memory.max", "1000")
    .write("memory.stat", "anon 100\nfile 700\nslab_reclaimable 100\n");
  CHECK_EQUAL(cgroup.available(), 900u);
}

TEST("shmem and unwritten page cache stay consumed") {
  // The 700 bytes of page cache hold 200 bytes of shmem, 50 dirty bytes, and
  // 50 bytes under writeback, so only 400 of them are reclaimable.
  auto const cgroup = FakeCgroup{};
  cgroup.write("memory.current", "900")
    .write("memory.max", "1000")
    .write("memory.stat", "anon 200\nfile 700\nshmem 200\nfile_dirty 50\n"
                          "file_writeback 50\n");
  CHECK_EQUAL(cgroup.available(), 500u);
}

TEST("pinned page cache is not reclaimable") {
  // 200 of the 700 bytes of page cache are pinned with `mlock`, leaving 500
  // reclaimable. This kernel does not report the file LRU lists, so the
  // unevictable counter is the only way to spot the pinned pages.
  auto const cgroup = FakeCgroup{};
  cgroup.write("memory.current", "900")
    .write("memory.max", "1000")
    .write("memory.stat", "anon 100\nfile 700\nunevictable 200\n");
  CHECK_EQUAL(cgroup.available(), 600u);
}

TEST("the file LRU lists measure reclaimable page cache") {
  // The same 700 bytes of page cache, but now split across the LRU lists: 100
  // active, 400 inactive, and 200 pinned onto the unevictable list. Only the
  // 500 bytes on the file lists are reclaimable.
  auto const cgroup = FakeCgroup{};
  cgroup.write("memory.current", "900")
    .write("memory.max", "1000")
    .write("memory.stat", "anon 100\nfile 700\nactive_file 100\n"
                          "inactive_file 400\nunevictable 200\n");
  CHECK_EQUAL(cgroup.available(), 600u);
}

TEST("swap-backed shmem never reduces the file LRU lists") {
  // Shmem is swap-backed, so the kernel keeps it on the anonymous LRU lists and
  // out of the file ones, even though the aggregate `file` counter includes it.
  // Of the 700 bytes of page cache, 200 are shmem and the other 500 are on the
  // file lists; subtracting shmem from those lists as well would count it twice
  // and understate what is reclaimable.
  auto const cgroup = FakeCgroup{};
  cgroup.write("memory.current", "900")
    .write("memory.max", "1000")
    .write("memory.stat", "anon 100\nfile 700\nshmem 200\nactive_file 100\n"
                          "inactive_file 400\n");
  CHECK_EQUAL(cgroup.available(), 600u);
}

TEST("unwritten pages on the file LRU lists stay consumed") {
  // Dirty pages and pages under writeback do sit on the file lists and need
  // writeback before the kernel can reclaim them, so they keep counting: of the
  // 500 bytes on those lists, only 400 are reclaimable.
  auto const cgroup = FakeCgroup{};
  cgroup.write("memory.current", "900")
    .write("memory.max", "1000")
    .write("memory.stat", "anon 100\nfile 700\nshmem 200\nactive_file 100\n"
                          "inactive_file 400\nfile_dirty 50\n"
                          "file_writeback 50\n");
  CHECK_EQUAL(cgroup.available(), 500u);
}

TEST("cgroup v1 counters all come from the subtree scope") {
  // A real cgroup v1 `memory.stat` reports every counter twice: once for the
  // cgroup alone, and once for the whole subtree with a `total_` prefix. Mixing
  // the two would subtract only the local 20 bytes of shared memory from the
  // subtree's 700 bytes of page cache, treating the 80 bytes of shared memory
  // held by descendants as reclaimable and overstating what is available.
  auto const cgroup = FakeCgroup{};
  cgroup.write("memory.usage_in_bytes", "900")
    .write("memory.limit_in_bytes", "1000")
    .write("memory.stat", "cache 300\nshmem 20\ndirty 10\nwriteback 0\n"
                          "total_cache 700\ntotal_shmem 100\n"
                          "total_dirty 100\ntotal_writeback 0\n");
  CHECK_EQUAL(cgroup.available(), 600u);
}

TEST("cgroup v1 scope follows memory.use_hierarchy") {
  // The `total_` counters are reported whether or not hierarchical accounting
  // is enabled, but `memory.usage_in_bytes` covers the subtree only when it is.
  // With it disabled, the descendants' page cache is not charged here either,
  // so the local counters are the ones matching the charge: 300 bytes of cache,
  // less 20 bytes of shared memory and 10 dirty bytes, leaves 270 reclaimable.
  auto const stat = "cache 300\nshmem 20\ndirty 10\nwriteback 0\n"
                    "total_cache 700\ntotal_shmem 100\n"
                    "total_dirty 100\ntotal_writeback 0\n";
  auto const enabled = FakeCgroup{};
  enabled.write("memory.usage_in_bytes", "900")
    .write("memory.limit_in_bytes", "1000")
    .write("memory.use_hierarchy", "1")
    .write("memory.stat", stat);
  CHECK_EQUAL(enabled.available(), 600u);
  auto const disabled = FakeCgroup{};
  disabled.write("memory.usage_in_bytes", "900")
    .write("memory.limit_in_bytes", "1000")
    .write("memory.use_hierarchy", "0")
    .write("memory.stat", stat);
  CHECK_EQUAL(disabled.available(), 370u);
}

TEST("a cgroup at its limit reports nothing available") {
  CHECK_EQUAL(FakeCgroup{}
                .write("memory.current", "1000")
                .write("memory.max", "1000")
                .write("memory.stat", "anon 1000\nfile 0\n")
                .available(),
              0u);
}

TEST("implausible statistics never overstate the limit") {
  // A `memory.stat` that reports more reclaimable memory than is charged must
  // not wrap around or exceed the limit.
  CHECK_EQUAL(FakeCgroup{}
                .write("memory.current", "100")
                .write("memory.max", "1000")
                .write("memory.stat", "file 5000\n")
                .available(),
              1000u);
}

} // namespace

} // namespace tenzir
