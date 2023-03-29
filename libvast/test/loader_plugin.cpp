//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/collect.hpp"
#include "vast/detail/string_literal.hpp"
#include "vast/pipeline.hpp"
#include "vast/plugin.hpp"
#include "vast/table_slice.hpp"
#include "vast/test/test.hpp"

#include <caf/error.hpp>
#include <caf/test/dsl.hpp>

#include <fcntl.h>
#include <unistd.h>

using namespace vast;

namespace {

struct fixture {
  struct mock_control_plane final : operator_control_plane {
    [[nodiscard]] auto self() noexcept -> caf::event_based_actor& override {
      FAIL("no mock implementation available");
    }

    auto abort(caf::error) noexcept -> void override {
      FAIL("no mock implementation available");
    }

    auto warn([[maybe_unused]] caf::error warning) noexcept -> void override {
      FAIL("no mock implementation available");
    }

    auto emit([[maybe_unused]] table_slice metrics) noexcept -> void override {
      FAIL("no mock implementation available");
    }

    [[nodiscard]] auto demand([[maybe_unused]] type schema = {}) const noexcept
      -> size_t override {
      FAIL("no mock implementation available");
    }

    [[nodiscard]] auto schemas() const noexcept
      -> const std::vector<type>& override {
      FAIL("no mock implementation available");
    }

    [[nodiscard]] auto concepts() const noexcept
      -> const concepts_map& override {
      FAIL("no mock implementation available");
    }
  };

  fixture() {
    // TODO: Move this into a separate fixture when we are starting to test more
    // than one loader type.
    loader_plugin = vast::plugins::find<vast::loader_plugin>("stdin");
    REQUIRE(loader_plugin);
    current_loader = [this] {
      return unbox(loader_plugin->make_loader({}, control_plane));
    };
  }

  const vast::loader_plugin* loader_plugin;
  std::function<auto()->generator<chunk_ptr>> current_loader;
  mock_control_plane control_plane;
};

// Helper struct that, as long as it is alive, redirects stdin to the output of
// a file.
template <detail::string_literal FileName = "">
struct stdin_file_input {
  stdin_file_input() {
    old_stdin_fd = ::dup(fileno(stdin));
    REQUIRE_NOT_EQUAL(old_stdin_fd, -1);
    auto input_file_fd = ::open(
      fmt::format("{}{}", VAST_TEST_PATH, FileName.str()).c_str(), O_RDONLY);
    REQUIRE_NOT_EQUAL(input_file_fd, -1);
    ::dup2(input_file_fd, fileno(stdin));
    ::close(input_file_fd);
  }

  ~stdin_file_input() {
    auto old_stdin_status = ::dup2(old_stdin_fd, fileno(stdin));
    REQUIRE_NOT_EQUAL(old_stdin_status, -1);
    ::close(old_stdin_fd);
  }
  int old_stdin_fd;
};

} // namespace

FIXTURE_SCOPE(loader_plugin_tests, fixture)

TEST(stdin loader - process simple input) {
  stdin_file_input<"artifacts/inputs/simple.txt"> file;
  auto str = std::string{"foobarbaz\n"};
  auto str_chunk = chunk::copy(str);
  auto chunks = collect(current_loader());
  REQUIRE_EQUAL(chunks.size(), size_t{1});
  REQUIRE(std::equal(chunks.front()->begin(), chunks.front()->end(),
                     str_chunk->begin(), str_chunk->end()));
}

TEST(stdin loader - no input) {
  stdin_file_input<"artifacts/inputs/nothing.txt"> file;
  auto str = std::string{""};
  auto str_chunk = chunk::copy(str);
  auto chunks = collect(current_loader());
  REQUIRE(chunks.empty());
}

TEST(stdin loader - process input with linebreaks) {
  stdin_file_input<"artifacts/inputs/linebreaks.txt"> file;
  auto str = std::string{"foo\nbar\nbaz\n"};
  auto str_chunk = chunk::copy(str);
  auto chunks = collect(current_loader());
  REQUIRE_EQUAL(chunks.size(), size_t{1});
  REQUIRE(std::equal(chunks.front()->begin(), chunks.front()->end(),
                     str_chunk->begin(), str_chunk->end()));
}

TEST(stdin loader - process input with spaces and tabs) {
  stdin_file_input<"artifacts/inputs/spaces_and_tabs.txt"> file;
  auto str = std::string{"foo bar\tbaz\n"};
  auto str_chunk = chunk::copy(str);
  auto chunks = collect(current_loader());
  REQUIRE_EQUAL(chunks.size(), size_t{1});
  REQUIRE(std::equal(chunks.front()->begin(), chunks.front()->end(),
                     str_chunk->begin(), str_chunk->end()));
}

TEST(stdin loader - chunking longer input) {
  stdin_file_input<"artifacts/inputs/longer_input.txt"> file;
  constexpr auto max_chunk_size = size_t{16384};
  const auto file_size = std::filesystem::file_size(
    VAST_TEST_PATH "artifacts/inputs/longer_input.txt");
  auto chunks = collect(current_loader());
  REQUIRE_EQUAL(chunks.size(), size_t{3});
  REQUIRE_EQUAL(chunks[0]->size(), max_chunk_size);
  REQUIRE_EQUAL(chunks[1]->size(), max_chunk_size);
  REQUIRE_EQUAL(chunks[2]->size(), file_size - (max_chunk_size * 2));
}

TEST(stdin loader - one complete chunk) {
  stdin_file_input<"artifacts/inputs/one_complete_chunk.txt"> file;
  constexpr auto max_chunk_size = size_t{16384};
  auto str = std::string(max_chunk_size - 1, '1');
  str += '\n';
  auto str_chunk = chunk::copy(str);
  auto chunks = collect(current_loader());
  REQUIRE_EQUAL(chunks.size(), size_t{1});
  REQUIRE_EQUAL(chunks.front()->size(), max_chunk_size);
  REQUIRE(std::equal(chunks.front()->begin(), chunks.front()->end(),
                     str_chunk->begin(), str_chunk->end()));
}

TEST(stdin loader - from operator) {
  struct sink final : crtp_operator<sink> {
    auto operator()(generator<table_slice> input) const
      -> generator<std::monostate> {
      for (auto&& slice : input) {
        REQUIRE(slice.rows() == 0);
        co_yield {};
      }
    }

    auto to_string() const -> std::string override {
      return "<sink>";
    }
  };

  stdin_file_input<"artifacts/inputs/nothing.txt"> file;
  auto ops = unbox(pipeline::parse("from stdin | pass")).unwrap();
  ops.push_back(std::make_unique<sink>());
  for (auto&& x : make_local_executor(pipeline{std::move(ops)})) {
    // TODO: When the parser is implemented, replace the checks below with
    // `REQUIRE_NOERROR(x);` (and perhaps write some more tests).
    REQUIRE_ERROR(x);
    REQUIRE_EQUAL(x.error().code(), static_cast<uint8_t>(ec::unimplemented));
  }
}

FIXTURE_SCOPE_END()
