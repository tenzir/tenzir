//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/test/fixtures/filesystem.hpp"

#include "vast/detail/system.hpp"
#include "vast/file.hpp"
#include "vast/si_literals.hpp"
#include "vast/test/test.hpp"

#include <cstddef>
#include <filesystem>

#if VAST_POSIX
#  include <unistd.h>
#endif

using namespace vast;

namespace {

struct fixture : public fixtures::filesystem {
  fixture() : fixtures::filesystem(VAST_PP_STRINGIFY(SUITE)) {
  }
};

} // namespace

FIXTURE_SCOPE(chunk_tests, fixture)

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
    CHECK(std::filesystem::remove_all(filename));
    MESSAGE("write back to disk");
    auto filename_copy = filename;
    filename_copy += ".copy";
    auto f2 = file{filename_copy};
    REQUIRE(f2.open(file::write_only));
    if (auto err = f2.write(ptr, size))
      FAIL(err);
    REQUIRE(f2.close());
    CHECK(std::filesystem::remove_all(filename_copy));
  }
}

#endif

FIXTURE_SCOPE_END()
