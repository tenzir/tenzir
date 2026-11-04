//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/workspace.hpp"

#include "tenzir/compile_ctx.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/session.hpp"
#include "tenzir/source.hpp"
#include "tenzir/test/registry.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/parser.hpp"
#include "tenzir/tql2/registry.hpp"
#include "tenzir/tql2/resolve.hpp"
#include "tenzir/tql2/tokens.hpp"

#include <cstdlib>
#include <fstream>
#include <pwd.h>
#include <string>
#include <tuple>
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

/// Sets an environment variable for as long as it lives.
struct Env {
  Env(char const* name, std::string const& value) : name_{name} {
    if (auto const* old = std::getenv(name)) {
      old_ = old;
    }
    ::setenv(name, value.c_str(), 1);
  }

  /// Unsets `name` instead.
  Env(char const* name, None) : name_{name} {
    if (auto const* old = std::getenv(name)) {
      old_ = old;
    }
    ::unsetenv(name);
  }

  ~Env() {
    if (old_) {
      ::setenv(name_, old_->c_str(), 1);
    } else {
      ::unsetenv(name_);
    }
  }

  Env(Env const&) = delete;
  auto operator=(Env const&) -> Env& = delete;

private:
  char const* name_;
  Option<std::string> old_;
};

/// The messages of the errors that compiling `text` against `reg` reports.
/// The messages of the errors that compiling `text` against `reg` reports,
/// letting it name the packages in `prelude`.
auto compile_errors(registry const& reg, ExternPrelude const& prelude,
                    std::string_view text) -> std::vector<std::string> {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh, reg);
  auto ctx = provider.as_session();
  auto source = Source::new_source(std::string{text}, "<test>", false);
  auto sources = SourceMap{};
  sources.add_source(source);
  auto tokens = tokenize_permissive(source->text);
  auto parsed = parse(tokens, *source, ctx);
  if (parsed and resolve_entities(*parsed, prelude, ctx)) {
    auto root = compile_ctx::make_root(base_ctx{ctx.dh(), ctx.reg()}, sources);
    std::ignore = std::move(*parsed).compile(root);
  }
  auto result = std::vector<std::string>{};
  for (auto& diagnostic : std::move(dh).collect()) {
    if (diagnostic.severity == severity::error) {
      result.push_back(std::move(diagnostic.message));
    }
  }
  return result;
}

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

TEST("a workspace is where its lock is") {
  auto scratch = Scratch{};
  write_workspace(scratch);
  {
    auto unset = Env{"TENZIR_WORKSPACE", ""};
    ::unsetenv("TENZIR_WORKSPACE");
    auto found = find_workspace(scratch.root / "ws" / "pipelines");
    REQUIRE(found);
    CHECK_EQUAL(*found, scratch.root / "ws");
  }
  auto none = Env{"TENZIR_WORKSPACE", "none"};
  CHECK(not find_workspace(scratch.root / "ws"));
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
  REQUIRE(require_dependencies("the workspace", workspace->dependencies,
                               workspace->packages, loaded_dh));
  REQUIRE(add_packages(*reg, workspace->packages, loaded_dh, nullptr,
                       PackageVisibility::declared));
  auto prelude = extern_prelude(workspace->dependencies);
  CHECK(compile_errors(*reg, prelude, "from {x: 1} | myteam::twice | discard")
          .empty());
  // `acme` is what `myteam` uses, not what the workspace lists.
  auto errors
    = compile_errors(*reg, prelude, "from {x: 1} | acme::double | discard");
  REQUIRE(not errors.empty());
  CHECK(errors.front().contains("not among the dependencies"));
}

TEST("the package cache ignores a relative XDG cache directory") {
  auto scratch = Scratch{};
  auto home = Env{"HOME", (scratch.root / "home").string()};
  {
    auto cache = Env{"XDG_CACHE_HOME", "cache"};
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

TEST("a workspace manifest lists only dependencies") {
  auto scratch = Scratch{};
  write_workspace(scratch);
  scratch.write("ws/package.yaml", "id: myteam\n");
  auto dh = collecting_diagnostic_handler{};
  CHECK(not load_workspace(scratch.root / "ws", dh));
}

} // namespace tenzir
