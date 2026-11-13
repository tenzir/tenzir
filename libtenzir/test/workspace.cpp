//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/workspace.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/env.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/test/compile.hpp"
#include "tenzir/test/registry.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/registry.hpp"
#include "tenzir/tql2/resolve.hpp"
#include "tenzir/unified.hpp"

#include <fmt/format.h>

#include <fstream>
#include <pwd.h>
#include <string>
#include <unistd.h>
#include <vector>

namespace tenzir {

namespace {

/// A scratch directory, removed again when the test is done.
struct Scratch {
  Scratch() {
    auto name = fmt::format("tenzir-workspace-test-{}", ::getpid());
    root = std::filesystem::temp_directory_path() / name;
    std::filesystem::remove_all(root);
    std::filesystem::create_directories(root);
  }

  ~Scratch() {
    auto ec = std::error_code{};
    std::filesystem::remove_all(root, ec);
  }

  Scratch(Scratch const&) = delete;
  auto operator=(Scratch const&) -> Scratch& = delete;

  void write(std::filesystem::path const& path, std::string_view text) const {
    std::filesystem::create_directories((root / path).parent_path());
    auto out = std::ofstream{root / path};
    out << text;
  }

  std::filesystem::path root;
};

/// Sets an environment variable, or unsets it, for as long as it lives.
struct Env {
  Env(std::string_view name, Option<std::string> value)
    : name_{name}, old_{detail::getenv(name)} {
    auto error = value ? detail::setenv(name, *value) : detail::unsetenv(name);
    TENZIR_ASSERT(error == caf::none);
  }

  ~Env() {
    auto error = old_ ? detail::setenv(name_, *old_) : detail::unsetenv(name_);
    TENZIR_ASSERT(error == caf::none);
  }

  Env(Env const&) = delete;
  auto operator=(Env const&) -> Env& = delete;

private:
  std::string name_;
  Option<std::string> old_;
};

constexpr auto acme_hash = std::string_view{
  "0000000000000000000000000000000000000000000000000000000000000a11"};

/// A workspace that uses its local `myteam`, which uses the locked `acme`.
void write_workspace(Scratch const& scratch) {
  scratch.write("ws/package.yaml", "dependencies:\n"
                                   "  myteam: {path: packages/myteam}\n");
  scratch.write("ws/packages/myteam/package.yaml", "name: My Team\n"
                                                   "dependencies:\n"
                                                   "  acme: \"^1\"\n");
  scratch.write("ws/packages/myteam/operators/twice.tql",
                "acme::double | acme::double");
  scratch.write("ws/package.lock",
                fmt::format("format: 1\n"
                            "packages:\n"
                            "  - id: acme\n"
                            "    version: 1.2.0\n"
                            "    git: https://example.com/library.git\n"
                            "    commit: abc\n"
                            "    hash: sha256:{}\n",
                            acme_hash));
  scratch.write("ws/pipelines/run.tql", "from {x: 1} | myteam::twice");
}

} // namespace

TEST("workspaces come with the unified command interface") {
  {
    auto unset = Env{"TENZIR_UNIFIED", None{}};
    CHECK(not unified_mode(6));
    CHECK(unified_mode(7));
  }
  {
    auto empty = Env{"TENZIR_UNIFIED", std::string{}};
    CHECK(not unified_mode(6));
    CHECK(unified_mode(7));
  }
  for (auto const* off : {"0", "false", "No"}) {
    auto value = Env{"TENZIR_UNIFIED", std::string{off}};
    CHECK(not unified_mode(6));
    CHECK(not unified_mode(7));
  }
  for (auto const* on : {"1", "true", "YES"}) {
    auto value = Env{"TENZIR_UNIFIED", std::string{on}};
    CHECK(unified_mode(6));
    CHECK(unified_mode(7));
  }
}

TEST("a workspace is found the way Cargo finds one") {
  auto scratch = Scratch{};
  write_workspace(scratch);
  scratch.write("ws/vendor/other/package.yaml", "name: Other\n");
  scratch.write("repo/acme/package.yaml", "name: Acme\n");
  // The workspace found from `from`, or an empty path for none.
  auto found_from = [&](std::filesystem::path const& from) {
    std::filesystem::create_directories(scratch.root / from);
    auto found = find_workspace(scratch.root / from);
    return found ? *found : std::filesystem::path{};
  };
  // The workspace's own manifest, and that of a package under its
  // `packages/`, which belongs to it.
  CHECK_EQUAL(found_from("ws/pipelines"), scratch.root / "ws");
  CHECK_EQUAL(found_from("ws/packages/myteam/operators"), scratch.root / "ws");
  // Any other package is a workspace of its own.
  CHECK_EQUAL(found_from("ws/vendor/other"),
              scratch.root / "ws" / "vendor" / "other");
  CHECK_EQUAL(found_from("repo/acme/operators"),
              scratch.root / "repo" / "acme");
}

TEST("a package of its own uses itself and what it locks") {
  auto scratch = Scratch{};
  auto cache = Env{"XDG_CACHE_HOME", (scratch.root / "cache").string()};
  scratch.write("repo/tools/package.yaml", "name: Tools\n"
                                           "dependencies:\n"
                                           "  acme: \"^1\"\n");
  scratch.write("repo/tools/operators/twice.tql",
                "acme::double | acme::double");
  auto dh = collecting_diagnostic_handler{};
  // Not locked yet: the error says what to run.
  CHECK(not load_workspace(scratch.root / "repo" / "tools", dh));
  auto unlocked = std::move(dh).collect();
  REQUIRE(not unlocked.empty());
  CHECK(unlocked.front().message.contains("nor locked"));
  scratch.write("repo/tools/package.lock",
                fmt::format("format: 1\n"
                            "packages:\n"
                            "  - id: acme\n"
                            "    version: 1.2.0\n"
                            "    git: https://example.com/library.git\n"
                            "    commit: abc\n"
                            "    hash: sha256:{}\n",
                            acme_hash));
  scratch.write(fmt::format("cache/tenzir/packages/{}/package.yaml", acme_hash),
                "name: Acme\n");
  scratch.write(fmt::format("cache/tenzir/packages/{}/operators/double.tql",
                            acme_hash),
                "x = x * 2");
  auto loaded_dh = collecting_diagnostic_handler{};
  auto workspace = load_workspace(scratch.root / "repo" / "tools", loaded_dh);
  REQUIRE(workspace);
  CHECK_EQUAL(workspace->packages.size(), size_t{2});
  auto reg = test::builtin_registry().clone();
  auto prelude = add_workspace_packages(
    *reg, workspace->dependencies, workspace->packages, loaded_dh, nullptr);
  REQUIRE(prelude);
  CHECK(test::compile_errors(*reg, *prelude,
                             "from {x: 1} | tools::twice | acme::double "
                             "| discard")
          .empty());
}

TEST("a workspace that needs nothing locked needs no lock") {
  auto scratch = Scratch{};
  scratch.write("ws/package.yaml", "dependencies:\n"
                                   "  myteam: {path: packages/myteam}\n");
  scratch.write("ws/packages/myteam/package.yaml", "name: My Team\n");
  scratch.write("ws/packages/myteam/operators/twice.tql", "x = x * 2");
  auto dh = collecting_diagnostic_handler{};
  auto workspace = load_workspace(scratch.root / "ws", dh);
  REQUIRE(workspace);
  CHECK_EQUAL(workspace->packages.size(), size_t{1});
}

TEST("a workspace's manifest lists only its dependencies") {
  auto dh = collecting_diagnostic_handler{};
  CHECK(workspace_dependencies(data{}, "package.yaml", dh)->empty());
  auto listed = workspace_dependencies(
    record{{"dependencies", record{{"acme", data{"^1"}}}}}, "package.yaml", dh);
  REQUIRE(listed);
  CHECK(listed->contains("acme"));
  CHECK(std::move(dh).collect().empty());
  auto refused_dh = collecting_diagnostic_handler{};
  CHECK(not workspace_dependencies(record{{"name", data{"Example"}}},
                                   "package.yaml", refused_dh));
  auto refused = std::move(refused_dh).collect();
  REQUIRE(not refused.empty());
  CHECK(refused.front().message.contains("lists only its `dependencies`"));
}

TEST("a workspace's pipelines use what it lists, from the cache") {
  auto scratch = Scratch{};
  write_workspace(scratch);
  auto cache = Env{"XDG_CACHE_HOME", (scratch.root / "cache").string()};
  auto dh = collecting_diagnostic_handler{};
  // Not fetched yet: the error says what to run.
  CHECK(not load_workspace(scratch.root / "ws", dh));
  auto missing = std::move(dh).collect();
  REQUIRE(not missing.empty());
  CHECK(missing.front().message.contains("not in the package cache"));
  scratch.write(fmt::format("cache/tenzir/packages/{}/package.yaml", acme_hash),
                "name: Acme\n");
  scratch.write(fmt::format("cache/tenzir/packages/{}/operators/double.tql",
                            acme_hash),
                "x = x * 2");
  auto loaded_dh = collecting_diagnostic_handler{};
  auto workspace = load_workspace(scratch.root / "ws", loaded_dh);
  REQUIRE(workspace);
  CHECK_EQUAL(workspace->packages.size(), size_t{2});
  auto reg = test::builtin_registry().clone();
  auto prelude = add_workspace_packages(
    *reg, workspace->dependencies, workspace->packages, loaded_dh, nullptr);
  REQUIRE(prelude);
  CHECK(test::compile_errors(*reg, *prelude,
                             "from {x: 1} | myteam::twice | discard")
          .empty());
  // `acme` is what `myteam` uses, not what the workspace lists.
  auto errors = test::compile_errors(*reg, *prelude,
                                     "from {x: 1} | acme::double | discard");
  REQUIRE(not errors.empty());
  CHECK(errors.front().contains("not among the dependencies"));
}

TEST("the package cache ignores a relative XDG cache directory") {
  auto scratch = Scratch{};
  auto home = Env{"HOME", (scratch.root / "home").string()};
  {
    auto cache = Env{"XDG_CACHE_HOME", std::string{"cache"}};
    auto found = package_cache_directory();
    REQUIRE(found);
    CHECK_EQUAL(*found,
                scratch.root / "home" / ".cache" / "tenzir" / "packages");
  }
  auto cache = Env{"XDG_CACHE_HOME", (scratch.root / "cache").string()};
  auto found = package_cache_directory();
  REQUIRE(found);
  CHECK_EQUAL(*found, scratch.root / "cache" / "tenzir" / "packages");
}

TEST("the package cache falls back to the user's home directory") {
  auto cache = Env{"XDG_CACHE_HOME", None{}};
  auto home = Env{"HOME", None{}};
  auto* entry = ::getpwuid(::getuid());
  auto found = package_cache_directory();
  if (not entry or not entry->pw_dir or *entry->pw_dir == '\0') {
    CHECK(not found);
    return;
  }
  REQUIRE(found);
  CHECK_EQUAL(*found, std::filesystem::path{entry->pw_dir} / ".cache" / "tenzir"
                        / "packages");
}

TEST("a lock names packages in the cache only by their digest") {
  auto scratch = Scratch{};
  write_workspace(scratch);
  auto cache = Env{"XDG_CACHE_HOME", (scratch.root / "cache").string()};
  // Where a path in place of the digest would lead, outside the cache.
  scratch.write("elsewhere/package.yaml", "name: Elsewhere\n");
  scratch.write("elsewhere/operators/double.tql", "x = x * 2");
  for (auto const& hash : std::vector<std::string>{
         fmt::format("sha256:{}", (scratch.root / "elsewhere").string()),
         "sha256:../../../elsewhere",
         fmt::format("sha256:{}", std::string(64, 'A')),
         "sha256:0a11",
       }) {
    scratch.write("ws/package.lock",
                  fmt::format("format: 1\n"
                              "packages:\n"
                              "  - id: acme\n"
                              "    version: 1.2.0\n"
                              "    git: https://example.com/library.git\n"
                              "    commit: abc\n"
                              "    hash: \"{}\"\n",
                              hash));
    auto dh = collecting_diagnostic_handler{};
    CHECK(not load_workspace(scratch.root / "ws", dh));
    auto errors = std::move(dh).collect();
    REQUIRE(not errors.empty());
    CHECK(errors.front().message.contains("is not a lock"));
  }
}

TEST("a workspace needs no local packages") {
  auto scratch = Scratch{};
  scratch.write("ws/package.yaml", "dependencies: {}\n");
  scratch.write("ws/package.lock", "format: 1\npackages: []\n");
  auto dh = collecting_diagnostic_handler{};
  auto workspace = load_workspace(scratch.root / "ws", dh);
  REQUIRE(workspace);
  CHECK(workspace->packages.empty());
}

TEST("loading a workspace checks its manifest") {
  auto scratch = Scratch{};
  write_workspace(scratch);
  scratch.write("ws/package.yaml", "id: myteam\n");
  auto dh = collecting_diagnostic_handler{};
  CHECK(not load_workspace(scratch.root / "ws", dh));
  auto errors = std::move(dh).collect();
  REQUIRE(not errors.empty());
  CHECK(errors.front().message.contains("lists only its `dependencies`"));
}

TEST("a workspace package with a file that does not parse does not load") {
  auto scratch = Scratch{};
  write_workspace(scratch);
  scratch.write("ws/packages/myteam/operators/broken.tql", "x = | =");
  auto dh = collecting_diagnostic_handler{};
  CHECK(not load_workspace(scratch.root / "ws", dh));
}

TEST("a workspace package is named by its directory") {
  auto scratch = Scratch{};
  write_workspace(scratch);
  scratch.write("ws/packages/myteam/package.yaml", "id: other\n"
                                                   "name: My Team\n");
  auto dh = collecting_diagnostic_handler{};
  CHECK(not load_workspace(scratch.root / "ws", dh));
  auto errors = std::move(dh).collect();
  REQUIRE(not errors.empty());
  CHECK(errors.front().message.contains("its id is `other`"));
}

} // namespace tenzir
