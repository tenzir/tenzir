//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/available_cpus.hpp"

#include "tenzir/test/test.hpp"
#include "tenzir/uuid.hpp"

#include <fmt/format.h>

#include <fstream>
#include <string_view>
#include <thread>

namespace tenzir {

namespace {

/// A throwaway directory holding a hierarchy of cgroup control files.
class FakeCgroups {
public:
  FakeCgroups()
    : root_{std::filesystem::temp_directory_path()
            / fmt::format("tnz-test-cgroup-cpu-{}", uuid::random())} {
    std::filesystem::create_directories(root_);
  }

  ~FakeCgroups() {
    auto error = std::error_code{};
    std::filesystem::remove_all(root_, error);
  }

  FakeCgroups(FakeCgroups const&) = delete;
  FakeCgroups(FakeCgroups&&) = delete;
  auto operator=(FakeCgroups const&) -> FakeCgroups& = delete;
  auto operator=(FakeCgroups&&) -> FakeCgroups& = delete;

  auto write(std::filesystem::path const& cgroup, std::string_view name,
             std::string_view content) const -> FakeCgroups const& {
    std::filesystem::create_directories(root_ / cgroup);
    auto file = std::ofstream{root_ / cgroup / name};
    file << content;
    return *this;
  }

  auto limit(std::filesystem::path const& cgroup = {}) const -> Option<size_t> {
    return detail::read_cgroup_cpu_limit(root_ / cgroup);
  }

  auto tightest(std::filesystem::path const& cgroup) const -> Option<size_t> {
    return detail::read_cgroup_cpu_limit(root_, cgroup);
  }

  /// The quota of `cgroup` with this directory as the mount point of the
  /// cgroup v2 subtree at `subtree`.
  auto mounted(std::filesystem::path const& subtree,
               std::filesystem::path const& cgroup) const -> Option<size_t> {
    return detail::read_cgroup2_cpu_limit(
      detail::cgroup2_mount{.root = subtree, .mount_point = root_}, cgroup);
  }

private:
  std::filesystem::path root_;
};

} // namespace

TEST("no cgroup quota yields no limit") {
  CHECK_EQUAL(FakeCgroups{}.limit(), None{});
  CHECK_EQUAL(FakeCgroups{}.write("", "cpu.max", "max 100000\n").limit(),
              None{});
  CHECK_EQUAL(FakeCgroups{}
                .write("", "cpu.cfs_quota_us", "-1\n")
                .write("", "cpu.cfs_period_us", "100000\n")
                .limit(),
              None{});
  CHECK_EQUAL(FakeCgroups{}.write("", "cpu.max", "garbage\n").limit(), None{});
  CHECK_EQUAL(FakeCgroups{}.write("", "cpu.cfs_quota_us", "200000\n").limit(),
              None{});
}

TEST("a cgroup v2 quota counts whole CPUs, rounded up") {
  CHECK_EQUAL(FakeCgroups{}.write("", "cpu.max", "200000 100000\n").limit(),
              size_t{2});
  CHECK_EQUAL(FakeCgroups{}.write("", "cpu.max", "150000 100000\n").limit(),
              size_t{2});
  CHECK_EQUAL(FakeCgroups{}.write("", "cpu.max", "50000 100000\n").limit(),
              size_t{1});
}

TEST("a cgroup v1 quota counts whole CPUs, rounded up") {
  CHECK_EQUAL(FakeCgroups{}
                .write("", "cpu.cfs_quota_us", "250000\n")
                .write("", "cpu.cfs_period_us", "100000\n")
                .limit(),
              size_t{3});
}

TEST("the tightest quota along the hierarchy applies") {
  auto const cgroups = FakeCgroups{};
  cgroups.write("", "cpu.max", "max 100000\n")
    .write("kubepods", "cpu.max", "400000 100000\n")
    .write("kubepods/pod", "cpu.max", "max 100000\n")
    .write("kubepods/pod/container", "cpu.max", "800000 100000\n");
  CHECK_EQUAL(cgroups.tightest("/kubepods/pod/container"), size_t{4});
  CHECK_EQUAL(cgroups.tightest("/"), None{});
  // A cgroup outside of the mounted hierarchy leaves the limit of the mount
  // point, and so does one that does not exist below it. Normalizing the path
  // of the former would turn it into the unrelated `elsewhere` below the mount
  // point.
  cgroups.write("", "cpu.max", "300000 100000\n")
    .write("elsewhere", "cpu.max", "100000 100000\n");
  CHECK_EQUAL(cgroups.tightest("/elsewhere"), size_t{1});
  CHECK_EQUAL(cgroups.tightest("/../elsewhere"), size_t{3});
  CHECK_EQUAL(cgroups.tightest("/../../elsewhere"), size_t{3});
  CHECK_EQUAL(cgroups.tightest("/docker/abc"), size_t{3});
}

TEST("a cgroup v2 mount of a subtree reads cgroups relative to its root") {
  // A container that only sees the subtree of its pod.
  auto const cgroups = FakeCgroups{};
  cgroups.write("", "cpu.max", "400000 100000\n")
    .write("container", "cpu.max", "100000 100000\n")
    .write("elsewhere", "cpu.max", "100000 100000\n");
  CHECK_EQUAL(cgroups.mounted("/kubepods/pod", "/kubepods/pod/container"),
              size_t{1});
  CHECK_EQUAL(cgroups.mounted("/kubepods/pod", "/kubepods/pod"), size_t{4});
  // Outside of the subtree, only the mount point's own quota counts, not that
  // of the unrelated `elsewhere` below it.
  CHECK_EQUAL(cgroups.mounted("/kubepods/pod", "/elsewhere"), size_t{4});
  // The whole hierarchy, mounted at its root.
  CHECK_EQUAL(cgroups.mounted("/", "/container"), size_t{1});
  CHECK_EQUAL(cgroups.mounted("/", "/"), size_t{4});
}

TEST("the available CPUs are at least one and at most all of them") {
  auto const cpus = detail::available_cpus();
  CHECK_GREATER_EQUAL(cpus, size_t{1});
  if (auto const hardware = std::thread::hardware_concurrency(); hardware > 0) {
    CHECK_LESS_EQUAL(cpus, size_t{hardware});
  }
}

} // namespace tenzir
