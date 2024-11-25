//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/configuration.hpp"

#include "tenzir/fwd.hpp"

#include "tenzir/application.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/env.hpp"
#include "tenzir/detail/settings.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/test/test.hpp"

#include <vector>

using namespace tenzir;

namespace {

struct fixture {
  template <class... Ts>
  void parse(Ts&&... xs) {
    // Emulate and parse command line
    std::vector<std::string> args = {std::forward<Ts>(xs)...};
    std::vector<char*> cmd_line; // argv
    static std::string argv0 = "tenzir-ctl";
    cmd_line.emplace_back(argv0.data());
    for (auto& arg : args) {
      cmd_line.emplace_back(arg.data());
    }
    auto argc = static_cast<int>(cmd_line.size());
    auto argv = cmd_line.data();
    REQUIRE_EQUAL(cfg.parse(argc, argv), caf::none);
    // Application setup, as Tenzir main does it.
    auto [root, _] = make_application(argv[0]);
    REQUIRE(root);
    // Parse the CLI.
    auto invocation
      = ::parse(*root, cfg.command_line.begin(), cfg.command_line.end());
    REQUIRE_NOERROR(invocation);
    // Merge the options from the CLI into the options from the configuration.
    // From here on, options from the command line can be used.
    detail::merge_settings(invocation->options, cfg.content,
                           policy::merge_lists::yes);
  }

  template <class T>
  T get(std::string_view name) {
    auto x = caf::get_if<T>(&cfg, name);
    if (!x)
      FAIL("no such config entry: " << name);
    return *x;
  }

  template <class T>
  std::vector<T> get_vec(std::string_view name) {
    auto x = detail::unpack_config_list_to_vector<T>(cfg, name);
    if (!x)
      FAIL("failed to unpack " << name << " to vector");
    return *x;
  }

  template <class T>
  bool holds_alternative(std::string_view name) {
    return caf::holds_alternative<T>(cfg, name);
  }

  void env(std::string_view key, std::string_view value) {
    auto result = detail::setenv(key, value);
    env_variables.emplace_back(key);
    REQUIRE_EQUAL(result, caf::none);
  }

  ~fixture() {
    // Clean up fixture-only environment variables so they don't get leaked
    // to other test fixtures.
    for (const auto& key : env_variables) {
      auto unset = detail::unsetenv(key);
      if (unset != caf::none) {
        TENZIR_TRACE(unset);
      }
      TENZIR_ASSERT(unset == caf::none);
    }
  }

  std::vector<std::string> env_variables;
  configuration cfg;
};

} // namespace

FIXTURE_SCOPE(configuration_tests, fixture)

TEST(environment key mangling and value parsing) {
  env("TENZIR_ENDPOINT", ""); // empty values are not considered.
  env("TENZIR_NODE", "true"); // bool parsed late (via automatic conversion)
  env("TENZIR_IMPORT__BATCH_SIZE", "42"); // numbers should not be strings
  env("TENZIR_PLUGINS", "foo,bar");       // list parsed manually
  env("TENZIR_INVALID", "foo,bar");       // list parsed late
  parse();
  CHECK(!holds_alternative<std::string>("tenzir.endpoint"));
  CHECK(get<bool>("tenzir.bare-mode"));
  CHECK(get<bool>("tenzir.node"));
  CHECK_EQUAL(get<caf::config_value::integer>("tenzir.import.batch-size"), 42);
  auto foo_bar = std::vector<std::string>{"foo", "bar"};
  CHECK_EQUAL(get_vec<std::string>("tenzir.plugins"), foo_bar);
  CHECK_EQUAL(get_vec<std::string>("tenzir.invalid"), foo_bar);
}

TEST(environment only) {
  env("TENZIR_ENDPOINT", "1.2.3.4");
  parse();
  CHECK(get<bool>("tenzir.bare-mode"));
  CHECK_EQUAL(get<std::string>("tenzir.endpoint"), "1.2.3.4");
}

TEST(command line overrides environment) {
  env("TENZIR_ENDPOINT", "1.2.3.4");
  parse("--endpoint=5.6.7.8");
  CHECK(get<bool>("tenzir.bare-mode"));
  fmt::print("{}\n", deep_to_string(content(cfg)));
  CHECK_EQUAL(get<std::string>("tenzir.endpoint"), "5.6.7.8");
}

TEST(command line no value for list generates empty list value) {
  parse("--plugins=");
  CHECK(get_vec<std::string>("tenzir.plugins").empty());
}

TEST(command line empty list value for list generates empty list value) {
  parse("--plugins=");
  CHECK(get_vec<std::string>("tenzir.plugins").empty());
}

TEST(environment key no value for plugin list generates empty list value) {
  env("TENZIR_PLUGINS", "");
  parse();
  CHECK(get_vec<std::string>("tenzir.plugins").empty());
}

TEST(environment key empty value for plugin list generates empty list value) {
  env("TENZIR_PLUGINS", "");
  parse();
  CHECK(get_vec<std::string>("tenzir.plugins").empty());
}

TEST(command line overrides environment even for plugins) {
  env("TENZIR_PLUGINS", "plugin1");
  parse("--plugins=plugin2");
  CHECK_EQUAL(get_vec<std::string>("tenzir.plugins"),
              std::vector<std::string>{"plugin2"});
}

TEST(command line no value for timespan value generates default value) {
  parse("--active-partition-timeout=");
  CHECK_EQUAL(get<caf::timespan>("tenzir.active-partition-timeout").count(),
              std::chrono::milliseconds{0}.count());
}

TEST(command line no value for bool value generates default true value by CAF) {
  parse(std::vector<std::string>{"rebuild", "--all="});
  CHECK_EQUAL(get<bool>("tenzir.rebuild.all"), true);
}

FIXTURE_SCOPE_END()
