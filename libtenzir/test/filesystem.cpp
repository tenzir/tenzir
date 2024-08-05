//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/chunk.hpp"
#include "tenzir/io/read.hpp"
#include "tenzir/io/write.hpp"
#include "tenzir/posix_filesystem.hpp"
#include "tenzir/status.hpp"
#include "tenzir/test/fixtures/actor_system.hpp"
#include "tenzir/test/test.hpp"

#include <cstddef>
#include <filesystem>
#include <fstream>
#include <span>

using namespace tenzir;
using namespace tenzir;
using namespace std::string_literals;

namespace {

struct fixture : fixtures::deterministic_actor_system {
  fixture() : fixtures::deterministic_actor_system(TENZIR_PP_STRINGIFY(SUITE)) {
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
  auto bytes = std::span<const char>{foo.data(), foo.size()};
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
      [&](const caf::error& err) {
        FAIL(err);
      });
  MESSAGE("attempt reading non-existent file");
  self
    ->request(filesystem, caf::infinite, atom::read_v,
              std::filesystem::path{"bar"})
    .receive(
      [&](const chunk_ptr&) {
        FAIL("fail should not exist");
      },
      [&](const caf::error& err) {
        CHECK_EQUAL(err, ec::no_such_file);
      });
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
      [&](const caf::error& err) {
        FAIL(err);
      });
  MESSAGE("verify file contents");
  auto bytes = unbox(io::read(filename));
  CHECK_EQUAL(as_bytes(bytes), as_bytes(chk));
}

TEST(mmap) {
  MESSAGE("create file");
  auto foo = "foo"s;
  auto filename = directory / foo;
  auto bytes = std::span<const char>{foo.data(), foo.size()};
  auto err = io::write(filename, as_bytes(bytes));
  MESSAGE("mmap file via actor");
  self
    ->request(filesystem, caf::infinite, atom::mmap_v,
              std::filesystem::path{foo})
    .receive(
      [&](const chunk_ptr& chk) {
        CHECK_EQUAL(as_bytes(chk), as_bytes(bytes));
      },
      [&](const caf::error& err) {
        FAIL(err);
      });
}

FIXTURE_SCOPE_END()
