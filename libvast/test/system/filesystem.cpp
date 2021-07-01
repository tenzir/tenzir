//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE filesystem

#include "vast/chunk.hpp"
#include "vast/io/read.hpp"
#include "vast/io/write.hpp"
#include "vast/system/posix_filesystem.hpp"
#include "vast/system/status.hpp"
#include "vast/test/fixtures/actor_system.hpp"
#include "vast/test/test.hpp"

#include <cstddef>
#include <filesystem>
#include <fstream>

using namespace vast;
using namespace vast::system;
using namespace std::string_literals;

namespace {

struct fixture : fixtures::deterministic_actor_system {
  fixture() {
    filesystem = self->spawn<caf::detached>(posix_filesystem, directory);
  }

  filesystem_actor filesystem;
};

} // namespace

FIXTURE_SCOPE(filesystem_tests, fixture)

TEST(read) {
  MESSAGE("create file");
  auto foo = "foo"s;
  auto filename = directory / foo;
  auto bytes = span<const char>{foo.data(), foo.size()};
  auto err = io::write(filename, as_bytes(bytes));
  REQUIRE(err == caf::none);
  MESSAGE("read file via actor");
  self
    ->request(filesystem, caf::infinite, atom::read_v,
              std::filesystem::path{foo})
    .receive(
      [&](const chunk_ptr& chk) {
        CHECK_EQUAL(as_bytes(chk), as_bytes(bytes));
      },
      [&](const caf::error& err) { FAIL(err); });
}

TEST(write) {
  auto foo = "foo"s;
  auto copy = foo;
  auto chk = chunk::make(std::move(copy));
  REQUIRE(chk);
  auto filename = directory / foo;
  MESSAGE("write file via actor");
  self
    ->request(filesystem, caf::infinite, atom::write_v,
              std::filesystem::path{foo}, chk)
    .receive(
      [&](atom::ok) {
        // all good
      },
      [&](const caf::error& err) { FAIL(err); });
  MESSAGE("verify file contents");
  auto bytes = unbox(io::read(filename));
  CHECK_EQUAL(span<const std::byte>{bytes}, as_bytes(chk));
}

TEST(mmap) {
  MESSAGE("create file");
  auto foo = "foo"s;
  auto filename = directory / foo;
  auto bytes = span<const char>{foo.data(), foo.size()};
  auto err = io::write(filename, as_bytes(bytes));
  MESSAGE("mmap file via actor");
  self
    ->request(filesystem, caf::infinite, atom::mmap_v,
              std::filesystem::path{foo})
    .receive(
      [&](const chunk_ptr& chk) {
        CHECK_EQUAL(as_bytes(chk), as_bytes(bytes));
      },
      [&](const caf::error& err) { FAIL(err); });
}

TEST(status) {
  MESSAGE("create file");
  self
    ->request(filesystem, caf::infinite, atom::read_v,
              std::filesystem::path{"not-there"})
    .receive(
      [&](const chunk_ptr&) { FAIL("should not receive chunk on failure"); },
      [&](const caf::error&) {
        // expected
      });
  self
    ->request(filesystem, caf::infinite, atom::status_v,
              status_verbosity::debug)
    .receive(
      [&](const caf::dictionary<caf::config_value>& status) {
        auto failed
          = caf::get<uint64_t>(status, "filesystem.operations.reads.failed");
        CHECK_EQUAL(failed, 1u);
      },
      [&](const caf::error& err) { FAIL(err); });
}

FIXTURE_SCOPE_END()
