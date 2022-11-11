//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE configuration

#include "vast/system/configuration.hpp"

#include "vast/fwd.hpp"

#include "vast/detail/env.hpp"
#include "vast/detail/settings.hpp"
#include "vast/system/application.hpp"
#include "vast/test/fixtures/actor_system_and_events.hpp"
#include "vast/test/test.hpp"

#include <vector>

using namespace vast;

namespace {

struct fixture {
  template <class... Ts>
  void parse(Ts&&... xs) {
    // Emulate and parse command line
    std::vector<std::string> args = {std::forward<Ts>(xs)...};
    std::vector<char*> cmd_line; // argv
    static std::string argv0 = "vast-test";
    cmd_line.emplace_back(argv0.data());
    for (const auto& arg : args)
      cmd_line.emplace_back(const_cast<char*>(arg.data()));
    auto argc = static_cast<int>(cmd_line.size());
    auto argv = cmd_line.data();
    // Application setup, as VAST main does it.
    auto [root, _] = system::make_application(argv[0]);
    REQUIRE_EQUAL(cfg.parse(argc, argv, *root), caf::none);
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
  bool holds_alternative(std::string_view name) {
    return caf::holds_alternative<T>(cfg, name);
  }

  void env(std::string_view key, std::string_view value) {
    auto result = detail::setenv(key, value);
    REQUIRE_EQUAL(result, caf::none);
  }

  system::configuration cfg;
};

} // namespace

FIXTURE_SCOPE(configuration_tests, fixture)

TEST(environment key mangling and value parsing) {
  env("VAST_ENDPOINT", "");      // empty values are not considered.
  env("VAST_BARE_MODE", "true"); // bool parsed manually
  env("VAST_NODE", "true");      // bool parsed late (via automatic conversion)
  env("VAST_IMPORT__BATCH_SIZE", "42"); // numbers should not be strings
  env("VAST_PLUGINS", "foo,bar");       // list parsed manually
  env("VAST_INVALID", "foo,bar");       // list parsed late
  parse();
  CHECK(!holds_alternative<std::string>("vast.endpoint"));
  CHECK(get<bool>("vast.bare-mode"));
  CHECK(get<bool>("vast.node"));
  CHECK_EQUAL(get<size_t>("vast.import.batch-size"), 42u);
  auto foo_bar = std::vector<std::string>{"foo", "bar"};
  CHECK_EQUAL(get<std::vector<std::string>>("vast.plugins"), foo_bar);
  CHECK_EQUAL(get<std::vector<std::string>>("vast.invalid"), foo_bar);
}

TEST(environment only) {
  env("VAST_BARE_MODE", "true");
  env("VAST_ENDPOINT", "1.2.3.4");
  parse();
  CHECK(get<bool>("vast.bare-mode"));
  CHECK_EQUAL(get<std::string>("vast.endpoint"), "1.2.3.4");
}

TEST(command line overrides environment) {
  env("VAST_BARE_MODE", "true");
  env("VAST_ENDPOINT", "1.2.3.4");
  parse("--endpoint=5.6.7.8");
  CHECK(get<bool>("vast.bare-mode"));
  fmt::print("{}\n", deep_to_string(content(cfg)));
  CHECK_EQUAL(get<std::string>("vast.endpoint"), "5.6.7.8");
}

TEST(command line no value for list generates empty list value) {
  parse("--plugins=");
  CHECK(get<std::vector<std::string>>("vast.plugins").empty());
}

TEST(command line empty list value for list generates empty list value) {
  parse("--plugins=[]");
  CHECK(get<std::vector<std::string>>("vast.plugins").empty());
}

TEST(environment key no value for plugin list generates empty list value) {
  env("VAST_PLUGINS", "");
  parse();
  CHECK(get<std::vector<std::string>>("vast.plugins").empty());
}

TEST(environment key empty value for plugin list generates empty list value) {
  env("VAST_PLUGINS", "[]");
  parse();
  CHECK(get<std::vector<std::string>>("vast.plugins").empty());
}

TEST(command line overrides environment even for plugins) {
  env("VAST_PLUGINS", "plugin1");
  parse("--plugins=[plugin2]");
  CHECK_EQUAL(get<std::vector<std::string>>("vast.plugins"),
              std::vector<std::string>{"plugin2"});
}

TEST(command line no value for integer values generates default value) {
  parse(std::vector<std::string>{"start", "--disk-budget-check-interval="});
  CHECK_EQUAL(get<size_t>("vast.start.disk-budget-check-interval"), 0);

  parse(std::vector<std::string>{"explore", "--max-events-query="});
  CHECK_EQUAL(get<size_t>("vast.explore.max-events-query"), 0);

  parse(std::vector<std::string>{"pivot", "--flush-interval="});
  CHECK_EQUAL(get<size_t>("vast.pivot.flush-interval"), 0);
}

TEST(command line no value for timespan value generates default value) {
  parse("--active-partition-timeout=");
  CHECK_EQUAL(get<caf::timespan>("vast.active-partition-timeout").count(),
              std::chrono::milliseconds{0}.count());
}

TEST(command line no value for bool value generates default value) {
  parse(std::vector<std::string>{"rebuild", "--all="});
  CHECK(get<bool>("vast.rebuild.all") == false);
}

FIXTURE_SCOPE_END()
