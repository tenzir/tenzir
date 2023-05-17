//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/collect.hpp>
#include <vast/plugin.hpp>
#include <vast/table_slice.hpp>
#include <vast/test/test.hpp>

#include <caf/test/dsl.hpp>

#include <fcntl.h>
#include <unistd.h>

using namespace vast;

namespace {

struct fixture {
  struct mock_control_plane final : operator_control_plane {
    auto self() noexcept -> system::execution_node_actor::base& override {
      FAIL("no mock implementation available");
    }

    auto node() noexcept -> system::node_actor override {
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
    // than one saver type.
    saver_plugin = vast::plugins::find<vast::saver_plugin>("stdout");
    REQUIRE(saver_plugin);
    current_saver = unbox(saver_plugin->make_saver({}, {}, control_plane));
  }

  // Helper struct that, as long as it is alive, captures stdout.
  struct stdout_capture {
    stdout_capture() {
      ::fflush(stdout);
      old_stdout = ::dup(fileno(stdout));
      ::pipe(pipes.data());
      ::dup2(pipes[1], fileno(stdout));
    }

    ~stdout_capture() {
      ::fflush(stdout);
      ::dup2(old_stdout, fileno(stdout));
    }

    std::string flush_captured_stdout_output() {
      ::write(pipes[1], "", 1);
      auto output = std::string{};
      char c;
      while (true) {
        ::read(pipes[0], &c, 1);
        if (c == '\0') {
          break;
        }
        output += c;
      }
      return output;
    }

    int old_stdout;
    std::array<int, 2> pipes;
  };

  auto collect_states(std::function<generator<chunk_ptr>()> output_generator)
    -> std::vector<std::monostate> {
    auto states = std::vector<std::monostate>{};
    for (auto&& x : output_generator()) {
      current_saver(x);
      states.emplace_back();
    }
    return states;
  }

  const vast::saver_plugin* saver_plugin;
  vast::saver_plugin::saver current_saver;
  mock_control_plane control_plane;
};

} // namespace

FIXTURE_SCOPE(saver_plugin_tests, fixture)
TEST(stdout saver - single chunk) {
  auto capture = stdout_capture{};
  auto out = std::string{"output"};
  auto chunk = chunk::copy(out);
  auto output_generator = [&chunk]() -> generator<chunk_ptr> {
    co_yield chunk;
    co_return;
  };
  auto states = collect_states(std::move(output_generator));
  auto output = capture.flush_captured_stdout_output();
  REQUIRE_EQUAL(states.size(), size_t{1});
  REQUIRE_EQUAL(output, "output");
}

TEST(stdout saver - multiple chunks) {
  auto capture = stdout_capture{};
  auto str1 = std::string{"first output\n"};
  auto str2 = std::string{"second output\n"};
  auto first_chunk = chunk::copy(str1);
  auto second_chunk = chunk::copy(str2);
  auto output_generator
    = [&first_chunk, &second_chunk]() -> generator<chunk_ptr> {
    co_yield first_chunk;
    co_yield second_chunk;
    co_return;
  };
  auto states = collect_states(std::move(output_generator));
  auto output = capture.flush_captured_stdout_output();
  REQUIRE_EQUAL(states.size(), size_t{2});
  REQUIRE_EQUAL(output, "first output\nsecond output\n");
}

FIXTURE_SCOPE_END()
