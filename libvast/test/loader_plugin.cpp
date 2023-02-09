//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/plugin.hpp"
#include "vast/test/test.hpp"

#include <iostream>
using namespace vast;

namespace {

struct fixture {
  fixture() {
  }

  caf::expected<input_loader> loader{caf::none};
};

} // namespace

FIXTURE_SCOPE(loader_plugin_tests, fixture)

TEST(stdin loader - process simple input) {
  auto str = std::string{"foobarbaz"};
  std::istringstream iss(str);
  std::cin.rdbuf(iss.rdbuf());
  vast::stdin_loader_plugin loader_plugin;
  loader = loader_plugin.make_loader({}, nullptr);
  REQUIRE(loader);
  auto loaded_chunk_generator = (*loader)();
  auto str_chunk = chunk::copy(str);
  for (const auto& chunk : loaded_chunk_generator) {
    CHECK(std::equal(chunk->begin(), chunk->end(), str_chunk->begin(),
                     str_chunk->end()));
  }
}

TEST(stdin loader - no input) {
  auto str = std::string{""};
  std::istringstream iss(str);
  std::cin.rdbuf(iss.rdbuf());
  vast::stdin_loader_plugin loader_plugin;
  loader = loader_plugin.make_loader({}, nullptr);
  REQUIRE(loader);
  auto loaded_chunk_generator = (*loader)();
  REQUIRE(loaded_chunk_generator.begin() == loaded_chunk_generator.end());
}

TEST(stdin loader - process input with linebreaks) {
  auto str = std::string{"foo\nbar\nbaz"};
  std::istringstream iss(str);
  std::cin.rdbuf(iss.rdbuf());
  vast::stdin_loader_plugin loader_plugin;
  loader = loader_plugin.make_loader({}, nullptr);
  REQUIRE(loader);
  auto loaded_chunk_generator = (*loader)();
  auto str_chunk = chunk::copy(str);
  for (const auto& chunk : loaded_chunk_generator) {
    CHECK(std::equal(chunk->begin(), chunk->end(), str_chunk->begin(),
                     str_chunk->end()));
  }
}

TEST(stdin loader - process input with spaces and tabs) {
  auto str = std::string{"foo bar\tbaz"};
  std::istringstream iss(str);
  std::cin.rdbuf(iss.rdbuf());
  vast::stdin_loader_plugin loader_plugin;
  loader = loader_plugin.make_loader({}, nullptr);
  REQUIRE(loader);
  auto loaded_chunk_generator = (*loader)();
  auto str_chunk = chunk::copy(str);
  for (const auto& chunk : loaded_chunk_generator) {
    CHECK(std::equal(chunk->begin(), chunk->end(), str_chunk->begin(),
                     str_chunk->end()));
  }
}
FIXTURE_SCOPE_END()
