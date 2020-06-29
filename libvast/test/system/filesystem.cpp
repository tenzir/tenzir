/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#define SUITE filesystem

#include "vast/test/fixtures/actor_system.hpp"
#include "vast/test/test.hpp"

#include "vast/chunk.hpp"
#include "vast/io/read.hpp"
#include "vast/io/write.hpp"
#include "vast/system/posix_filesystem.hpp"

#include <fstream>

using namespace vast;
using namespace vast::system;
using namespace std::string_literals;

namespace {

struct fixture : fixtures::deterministic_actor_system {
  fixture() {
    fs = self->spawn<caf::detached>(posix_filesystem, directory);
  }

  filesystem_type fs;
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
  self->request(fs, caf::infinite, atom::read_v, path{foo})
    .receive(
      [&](const chunk_ptr& chk) {
        CHECK_EQUAL(as_bytes(chk), as_bytes(bytes));
      },
      [&](const caf::error& err) { FAIL(err); });
}

TEST(write) {
  auto foo = "foo"s;
  auto chk = chunk::copy(span{foo.data(), foo.size()});
  REQUIRE(chk);
  auto filename = directory / foo;
  MESSAGE("write file via actor");
  self->request(fs, caf::infinite, atom::write_v, path{foo}, chk)
    .receive(
      [&](atom::ok) {
        // all good
      },
      [&](const caf::error& err) { FAIL(err); });
  MESSAGE("verify file contents");
  auto bytes = unbox(io::read(filename));
  CHECK_EQUAL(span<const byte>{bytes}, as_bytes(chk));
}

TEST(mmap) {
  MESSAGE("create file");
  auto foo = "foo"s;
  auto filename = directory / foo;
  auto bytes = span<const char>{foo.data(), foo.size()};
  auto err = io::write(filename, as_bytes(bytes));
  MESSAGE("mmap file via actor");
  self->request(fs, caf::infinite, atom::mmap_v, path{foo})
    .receive(
      [&](const chunk_ptr& chk) {
        CHECK_EQUAL(as_bytes(chk), as_bytes(bytes));
      },
      [&](const caf::error& err) { FAIL(err); });
}

TEST(status) {
  MESSAGE("create file");
  self->request(fs, caf::infinite, atom::read_v, path{"not-there"})
    .receive(
      [&](const chunk_ptr&) { FAIL("should not receive chunk on failure"); },
      [&](const caf::error&) {
        // expected
      });
  self->request(fs, caf::infinite, atom::status_v, status_verbosity::debug)
    .receive(
      [&](const caf::dictionary<caf::config_value>& status) {
        auto failed
          = caf::get<uint64_t>(status, "filesystem.operations.reads.failed");
        CHECK_EQUAL(failed, 1u);
      },
      [&](const caf::error& err) { FAIL(err); });
}

FIXTURE_SCOPE_END()
