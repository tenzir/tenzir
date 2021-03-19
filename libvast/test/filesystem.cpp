//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/system.hpp"
#include "vast/file.hpp"
#include "vast/path.hpp"
#include "vast/si_literals.hpp"

#define SUITE filesystem
#include "vast/test/fixtures/filesystem.hpp"
#include "vast/test/test.hpp"

#include <cstddef>
#include <filesystem>

#if VAST_POSIX
#  include <unistd.h>
#endif

using namespace vast;

TEST(path_operations) {
  path p(".");
  CHECK(p.basename() == ".");
  CHECK(p.parent() == "");

  p = "..";
  CHECK(p.basename() == "..");
  CHECK(p.parent() == "");

  p = "/";
  CHECK(p.basename() == "/");
  CHECK(p.parent() == "");

  p = "foo";
  CHECK(p.basename() == "foo");
  CHECK(p.parent() == "");

  p = "/foo";
  CHECK(p.basename() == "foo");
  CHECK(p.parent() == "/");

  p = "foo/";
  CHECK(p.basename() == ".");
  CHECK(p.parent() == "foo");

  p = "/foo/";
  CHECK(p.basename() == ".");
  CHECK(p.parent() == "/foo");

  p = "foo/bar";
  CHECK(p.basename() == "bar");
  CHECK(p.parent() == "foo");

  p = "/foo/bar";
  CHECK(p.basename() == "bar");
  CHECK(p.parent() == "/foo");

  p = "/.";
  CHECK(p.basename() == ".");
  CHECK(p.parent() == "/");

  p = "./";
  CHECK(p.basename() == ".");
  CHECK(p.parent() == ".");

  p = "/..";
  CHECK(p.basename() == "..");
  CHECK(p.parent() == "/");

  p = "../";
  CHECK(p.basename() == ".");
  CHECK(p.parent() == "..");

  p = "foo/.";
  CHECK(p.basename() == ".");
  CHECK(p.parent() == "foo");

  p = "foo/..";
  CHECK(p.basename() == "..");
  CHECK(p.parent() == "foo");

  p = "foo/./";
  CHECK(p.basename() == ".");
  CHECK(p.parent() == "foo/.");

  p = "foo/../";
  CHECK(p.basename() == ".");
  CHECK(p.parent() == "foo/..");

  p = "foo/./bar";
  CHECK(p.basename() == "bar");
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

TEST(file_and_directory_manipulation) {
  path base = "vast-unit-test-file-system-test";
  path p("/tmp");
  p /= base / std::to_string(detail::process_id());
  CHECK(!p.is_regular_file());
  CHECK(!exists(p));
  CHECK(!mkdir(p));
  CHECK(exists(p));
  CHECK(p.is_directory());
  CHECK(std::filesystem::remove_all(std::filesystem::path{p.str()}));
  CHECK(!p.is_directory());
  CHECK(p.parent().is_directory());
  CHECK(std::filesystem::remove_all(std::filesystem::path{p.parent().str()}));
  CHECK(!p.parent().is_directory());
}

FIXTURE_SCOPE(chunk_tests, fixtures::filesystem)

#if VAST_POSIX

// The following write test adds several seconds (or minutes in case of
// macOS) to the execution time. Running it every time would hurt development
// speed, so it must be enabled manually.
TEST_DISABLED(large_file_io) {
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
    std::vector<std::byte> buffer(size);
    auto ptr = reinterpret_cast<char*>(buffer.data());
    if (auto err = f.read(ptr, size))
      FAIL(err);
    REQUIRE(f.close());
    CHECK(std::filesystem::remove_all(std::filesystem::path{filename.str()}));
    MESSAGE("write back to disk");
    auto filename_copy = filename + ".copy";
    auto f2 = file{filename_copy};
    REQUIRE(f2.open(file::write_only));
    if (auto err = f2.write(ptr, size))
      FAIL(err);
    REQUIRE(f2.close());
    CHECK(
      std::filesystem::remove_all(std::filesystem::path{filename_copy.str()}));
  }
}

#endif

FIXTURE_SCOPE_END()
