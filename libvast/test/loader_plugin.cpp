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

  ~fixture() {
    if (input_file) {
      fclose(input_file);
    }
  }

  int old_stdin_fd;
  caf::expected<input_loader> loader{caf::none};
  FILE* input_file{};
};

} // namespace

FIXTURE_SCOPE(loader_plugin_tests, fixture)

TEST(stdin loader - process simple input) {
  input_file
    = freopen(VAST_TEST_PATH "artifacts/inputs/simple.txt", "r", stdin);
  auto str = std::string{"foobarbaz\n"};
  vast::stdin_loader_plugin loader_plugin;
  loader = loader_plugin.make_loader({}, nullptr);
  REQUIRE(loader);
  auto loaded_chunk_generator = (*loader)();
  auto str_chunk = chunk::copy(str);
  for (const auto& chunk : loaded_chunk_generator) {
    REQUIRE(std::equal(chunk->begin(), chunk->end(), str_chunk->begin(),
                       str_chunk->end()));
  }
}

TEST(stdin loader - no input) {
  input_file
    = freopen(VAST_TEST_PATH "artifacts/inputs/nothing.txt", "r", stdin);
  auto str = std::string{""};
  vast::stdin_loader_plugin loader_plugin;
  loader = loader_plugin.make_loader({}, nullptr);
  REQUIRE(loader);
  auto loaded_chunk_generator = (*loader)();
  auto str_chunk = chunk::copy(str);
  for (const auto& chunk : loaded_chunk_generator) {
    REQUIRE(std::equal(chunk->begin(), chunk->end(), str_chunk->begin(),
                       str_chunk->end()));
  }
}

TEST(stdin loader - process input with linebreaks) {
  input_file
    = freopen(VAST_TEST_PATH "artifacts/inputs/linebreaks.txt", "r", stdin);
  auto str = std::string{"foo\nbar\nbaz\n"};
  vast::stdin_loader_plugin loader_plugin;
  loader = loader_plugin.make_loader({}, nullptr);
  REQUIRE(loader);
  auto loaded_chunk_generator = (*loader)();
  auto str_chunk = chunk::copy(str);
  for (const auto& chunk : loaded_chunk_generator) {
    REQUIRE(std::equal(chunk->begin(), chunk->end(), str_chunk->begin(),
                       str_chunk->end()));
  }
}

TEST(stdin loader - process input with spaces and tabs) {
  input_file = freopen(VAST_TEST_PATH "artifacts/inputs/spaces_and_tabs.txt",
                       "r", stdin);
  auto str = std::string{"foo bar\tbaz\n"};
  vast::stdin_loader_plugin loader_plugin;
  loader = loader_plugin.make_loader({}, nullptr);
  REQUIRE(loader);
  auto loaded_chunk_generator = (*loader)();
  auto str_chunk = chunk::copy(str);
  for (const auto& chunk : loaded_chunk_generator) {
    REQUIRE(std::equal(chunk->begin(), chunk->end(), str_chunk->begin(),
                       str_chunk->end()));
  }
}

TEST(stdin loader - chunking longer input) {
  input_file
    = freopen(VAST_TEST_PATH "artifacts/inputs/longer_input.txt", "r", stdin);
  const auto file_size = std::filesystem::file_size(
    VAST_TEST_PATH "artifacts/inputs/longer_input.txt");
  vast::stdin_loader_plugin loader_plugin;
  loader = loader_plugin.make_loader({}, nullptr);
  REQUIRE(loader);
  auto loaded_chunk_generator = (*loader)();
  auto generated_chunk_it = loaded_chunk_generator.begin();
  CHECK_EQUAL((*generated_chunk_it)->size(),
              stdin_loader_plugin::max_chunk_size);
  ++generated_chunk_it;
  CHECK_EQUAL((*generated_chunk_it)->size(),
              stdin_loader_plugin::max_chunk_size);
  ++generated_chunk_it;
  CHECK_EQUAL((*generated_chunk_it)->size(),
              file_size - (stdin_loader_plugin::max_chunk_size * 2));
  ++generated_chunk_it;
  REQUIRE(generated_chunk_it == loaded_chunk_generator.end());
}

TEST(stdin loader - one complete chunk) {
  input_file = freopen(VAST_TEST_PATH "artifacts/inputs/one_complete_chunk.txt",
                       "r", stdin);
  auto str = std::string(stdin_loader_plugin::max_chunk_size - 1, '1');
  str += '\n';
  vast::stdin_loader_plugin loader_plugin;
  loader = loader_plugin.make_loader({}, nullptr);
  REQUIRE(loader);
  auto loaded_chunk_generator = (*loader)();
  auto str_chunk = chunk::copy(str);
  auto generated_chunk_it = loaded_chunk_generator.begin();
  CHECK_EQUAL((*generated_chunk_it)->size(),
              stdin_loader_plugin::max_chunk_size);
  REQUIRE(std::equal((*generated_chunk_it)->begin(),
                     (*generated_chunk_it)->end(), str_chunk->begin(),
                     str_chunk->end()));
  ++generated_chunk_it;
  REQUIRE(generated_chunk_it == loaded_chunk_generator.end());
}

FIXTURE_SCOPE_END()
