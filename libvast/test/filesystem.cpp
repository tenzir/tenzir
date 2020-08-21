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

#include "vast/byte.hpp"
#include "vast/detail/system.hpp"
#include "vast/directory.hpp"
#include "vast/file.hpp"
#include "vast/path.hpp"
#include "vast/si_literals.hpp"

#define SUITE filesystem
#include "vast/test/fixtures/filesystem.hpp"
#include "vast/test/test.hpp"

#if VAST_POSIX
#  include <unistd.h>
#endif

using namespace vast;

TEST(path_operations) {
  path p(".");
  CHECK(p.basename() == ".");
  CHECK(p.extension() == ".");
  CHECK(p.parent() == "");

  p = "..";
  CHECK(p.basename() == "..");
  CHECK(p.extension() == ".");
  CHECK(p.parent() == "");

  p = "/";
  CHECK(p.basename() == "/");
  CHECK(p.extension() == "");
  CHECK(p.parent() == "");

  p = "foo";
  CHECK(p.basename() == "foo");
  CHECK(p.extension() == "");
  CHECK(p.parent() == "");

  p = "/foo";
  CHECK(p.basename() == "foo");
  CHECK(p.extension() == "");
  CHECK(p.parent() == "/");

  p = "foo/";
  CHECK(p.basename() == ".");
  CHECK(p.extension() == "");
  CHECK(p.parent() == "foo");

  p = "/foo/";
  CHECK(p.basename() == ".");
  CHECK(p.extension() == "");
  CHECK(p.parent() == "/foo");

  p = "foo/bar";
  CHECK(p.basename() == "bar");
  CHECK(p.extension() == "");
  CHECK(p.parent() == "foo");

  p = "/foo/bar";
  CHECK(p.basename() == "bar");
  CHECK(p.extension() == "");
  CHECK(p.parent() == "/foo");

  p = "/.";
  CHECK(p.basename() == ".");
  CHECK(p.extension() == ".");
  CHECK(p.parent() == "/");

  p = "./";
  CHECK(p.basename() == ".");
  CHECK(p.extension() == "");
  CHECK(p.parent() == ".");

  p = "/..";
  CHECK(p.basename() == "..");
  CHECK(p.extension() == ".");
  CHECK(p.parent() == "/");

  p = "../";
  CHECK(p.basename() == ".");
  CHECK(p.extension() == "");
  CHECK(p.parent() == "..");

  p = "foo/.";
  CHECK(p.basename() == ".");
  CHECK(p.extension() == ".");
  CHECK(p.parent() == "foo");

  p = "foo/..";
  CHECK(p.basename() == "..");
  CHECK(p.extension() == ".");
  CHECK(p.parent() == "foo");

  p = "foo/./";
  CHECK(p.basename() == ".");
  CHECK(p.extension() == "");
  CHECK(p.parent() == "foo/.");

  p = "foo/../";
  CHECK(p.basename() == ".");
  CHECK(p.extension() == "");
  CHECK(p.parent() == "foo/..");

  p = "foo/./bar";
  CHECK(p.basename() == "bar");
  CHECK(p.extension() == "");
  CHECK(p.parent() == "foo/.");

  p = "/usr/local/bin/foo";
  CHECK(p.parent() == "/usr/local/bin");
  CHECK(p.basename() == "foo");
  CHECK(path("/usr/local/bin/foo.bin").basename(true) == "foo");

  CHECK(p.root() == "/");
  CHECK(path("usr/local").root() == "");

  CHECK(p.complete() == p);
  CHECK(path("foo/").complete() == path::current() / "foo/");

  auto pieces = split(p);
  REQUIRE(pieces.size() == 5);
  CHECK(pieces[0] == "/");
  CHECK(pieces[1] == "usr");
  CHECK(pieces[2] == "local");
  CHECK(pieces[3] == "bin");
  CHECK(pieces[4] == "foo");
}

TEST(path_trimming) {
  path p = "/usr/local/bin/foo";

  CHECK(p.trim(0) == "");
  CHECK(p.trim(1) == "/");
  CHECK(p.trim(2) == "/usr");
  CHECK(p.trim(3) == "/usr/local");
  CHECK(p.trim(4) == "/usr/local/bin");
  CHECK(p.trim(5) == p);
  CHECK(p.trim(6) == p);
  CHECK(p.trim(-1) == "foo");
  CHECK(p.trim(-2) == "bin/foo");
  CHECK(p.trim(-3) == "local/bin/foo");
  CHECK(p.trim(-4) == "usr/local/bin/foo");
  CHECK(p.trim(-5) == p);
  CHECK(p.trim(-6) == p);
}

TEST(path_chopping) {
  path p = "/usr/local/bin/foo";

  CHECK(p.chop(0) == p);
  CHECK(p.chop(-1) == "/usr/local/bin");
  CHECK(p.chop(-2) == "/usr/local");
  CHECK(p.chop(-3) == "/usr");
  CHECK(p.chop(-4) == "/");
  CHECK(p.chop(-5) == "");
  CHECK(p.chop(1) == "usr/local/bin/foo");
  CHECK(p.chop(2) == "local/bin/foo");
  CHECK(p.chop(3) == "bin/foo");
  CHECK(p.chop(4) == "foo");
  CHECK(p.chop(5) == "");
}

TEST(file_and_directory_manipulation) {
  path base = "vast-unit-test-file-system-test";
  path p("/tmp");
  p /= base / std::to_string(detail::process_id());
  CHECK(!p.is_regular_file());
  CHECK(!exists(p));
  CHECK(!mkdir(p));
  CHECK(exists(p));
  CHECK(p.is_directory());
  CHECK(rm(p));
  CHECK(!p.is_directory());
  CHECK(p.parent().is_directory());
  CHECK(rm(p.parent()));
  CHECK(!p.parent().is_directory());
}

FIXTURE_SCOPE(chunk_tests, fixtures::filesystem)

// This test takes almost one minute on macOS 10.14.
#if VAST_POSIX && !VAST_MACOS

TEST(read_large_file) {
  using namespace vast::binary_byte_literals;
  auto filename = directory / "very-large.file";
  auto size = 3_GiB;
  {
    MESSAGE("Generate a sparse file");
    file f{filename};
    REQUIRE(f.open(file::write_only));
    auto fd = f.handle();
    REQUIRE(fd > 0);
    REQUIRE_EQUAL(ftruncate(fd, size), 0);
    REQUIRE(f.close());
  }
  {
    MESSAGE("load into memory");
    file f{filename};
    REQUIRE(f.open(file::read_only));
    std::vector<byte> buffer(size);
    size_t bytes_read;
    auto ptr = reinterpret_cast<char*>(buffer.data());
    if (auto err = f.read(ptr, size, &bytes_read))
      FAIL(err);
    CHECK_EQUAL(bytes_read, size);
  }
}

#endif

FIXTURE_SCOPE_END()
