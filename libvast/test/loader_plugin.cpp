//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/plugin.hpp"
#include "vast/table_slice.hpp"
#include "vast/test/test.hpp"

#include <iostream>
using namespace vast;

namespace {

struct fixture {
  struct mock_control_plane : operator_control_plane {
    [[nodiscard]] virtual auto self() noexcept -> caf::event_based_actor& {
      FAIL("no mock implementation available");
    }
    virtual auto stop([[maybe_unused]] caf::error error = {}) noexcept -> void {
      FAIL("no mock implementation available");
    }

    virtual auto warn([[maybe_unused]] caf::error warning) noexcept -> void {
      FAIL("no mock implementation available");
    }

    virtual auto emit([[maybe_unused]] table_slice metrics) noexcept -> void {
      FAIL("no mock implementation available");
    }

    [[nodiscard]] virtual auto
    demand([[maybe_unused]] type schema = {}) const noexcept -> size_t {
      FAIL("no mock implementation available");
    }

    [[nodiscard]] virtual auto schemas() const noexcept
      -> const std::vector<type>& {
      FAIL("no mock implementation available");
    }

    [[nodiscard]] virtual auto concepts() const noexcept
      -> const concepts_map& {
      FAIL("no mock implementation available");
    }
  };

  fixture() {
  }

  ~fixture() {
    if (input_file) {
      fclose(input_file);
    }
  }

  int old_stdin_fd;
  caf::expected<vast::plugins::loader> current_loader{caf::none};
  FILE* input_file{};
  mock_control_plane control_plane;
};

} // namespace

FIXTURE_SCOPE(loader_plugin_tests, fixture)

TEST(stdin loader - process simple input) {
  input_file
    = freopen(VAST_TEST_PATH "artifacts/inputs/simple.txt", "r", stdin);
  auto str = std::string{"foobarbaz\n"};
  auto loader_plugin = vast::plugins::find<vast::loader_plugin>("stdin");
  REQUIRE(loader_plugin);
  current_loader = loader_plugin->make_loader({}, control_plane);
  REQUIRE(current_loader);
  auto loaded_chunk_generator = (*current_loader)();
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
  auto loader_plugin = vast::plugins::find<vast::loader_plugin>("stdin");
  REQUIRE(loader_plugin);
  current_loader = loader_plugin->make_loader({}, control_plane);
  REQUIRE(current_loader);
  auto loaded_chunk_generator = (*current_loader)();
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
  auto loader_plugin = vast::plugins::find<vast::loader_plugin>("stdin");
  REQUIRE(loader_plugin);
  current_loader = loader_plugin->make_loader({}, control_plane);
  REQUIRE(current_loader);
  auto loaded_chunk_generator = (*current_loader)();
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
  auto loader_plugin = vast::plugins::find<vast::loader_plugin>("stdin");
  REQUIRE(loader_plugin);
  current_loader = loader_plugin->make_loader({}, control_plane);
  REQUIRE(current_loader);
  auto loaded_chunk_generator = (*current_loader)();
  auto str_chunk = chunk::copy(str);
  for (const auto& chunk : loaded_chunk_generator) {
    REQUIRE(std::equal(chunk->begin(), chunk->end(), str_chunk->begin(),
                       str_chunk->end()));
  }
}

TEST(stdin loader - chunking longer input) {
  constexpr auto max_chunk_size = size_t{16384};
  input_file
    = freopen(VAST_TEST_PATH "artifacts/inputs/longer_input.txt", "r", stdin);
  const auto file_size = std::filesystem::file_size(
    VAST_TEST_PATH "artifacts/inputs/longer_input.txt");
  auto loader_plugin = vast::plugins::find<vast::loader_plugin>("stdin");
  REQUIRE(loader_plugin);
  current_loader = loader_plugin->make_loader({}, control_plane);
  REQUIRE(current_loader);
  auto loaded_chunk_generator = (*current_loader)();
  auto generated_chunk_it = loaded_chunk_generator.begin();
  CHECK_EQUAL((*generated_chunk_it)->size(), max_chunk_size);
  ++generated_chunk_it;
  CHECK_EQUAL((*generated_chunk_it)->size(), max_chunk_size);
  ++generated_chunk_it;
  CHECK_EQUAL((*generated_chunk_it)->size(), file_size - (max_chunk_size * 2));
  ++generated_chunk_it;
  REQUIRE(generated_chunk_it == loaded_chunk_generator.end());
}

TEST(stdin loader - one complete chunk) {
  constexpr auto max_chunk_size = size_t{16384};
  input_file = freopen(VAST_TEST_PATH "artifacts/inputs/one_complete_chunk.txt",
                       "r", stdin);
  auto str = std::string(max_chunk_size - 1, '1');
  str += '\n';
  auto loader_plugin = vast::plugins::find<vast::loader_plugin>("stdin");
  REQUIRE(loader_plugin);
  current_loader = loader_plugin->make_loader({}, control_plane);
  REQUIRE(current_loader);
  auto loaded_chunk_generator = (*current_loader)();
  auto str_chunk = chunk::copy(str);
  auto generated_chunk_it = loaded_chunk_generator.begin();
  CHECK_EQUAL((*generated_chunk_it)->size(), max_chunk_size);
  REQUIRE(std::equal((*generated_chunk_it)->begin(),
                     (*generated_chunk_it)->end(), str_chunk->begin(),
                     str_chunk->end()));
  ++generated_chunk_it;
  REQUIRE(generated_chunk_it == loaded_chunk_generator.end());
}

FIXTURE_SCOPE_END()
