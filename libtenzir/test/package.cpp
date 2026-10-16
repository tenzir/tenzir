//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/package.hpp"

#include "tenzir/diagnostics.hpp"
#include "tenzir/source.hpp"
#include "tenzir/test/registry.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/registry.hpp"

#include <string>
#include <vector>

namespace tenzir {

TEST("build_package_operator_module populates UDO source") {
  auto pkg = package{};
  pkg.id = "sourcepkg";
  pkg.name = "Source Package";
  auto op = package_operator{};
  op.tql_body = "where true";
  pkg.operators.emplace(std::vector<std::string>{"echo"}, std::move(op));
  auto dh = null_diagnostic_handler{};
  auto module
    = build_package_operator_module(pkg, dh, test::builtin_registry());
  REQUIRE(module);
  auto package_it = (*module)->defs.find("sourcepkg");
  REQUIRE(package_it != (*module)->defs.end());
  REQUIRE(package_it->second.mod != nullptr);
  auto operator_it = package_it->second.mod->defs.find("echo");
  REQUIRE(operator_it != package_it->second.mod->defs.end());
  REQUIRE(operator_it->second.op.has_value());
  auto const* udo
    = try_as<user_defined_operator>(&operator_it->second.op->inner());
  REQUIRE(udo != nullptr);
  CHECK(udo->source->index != 0);
  CHECK(udo->source->text == "where true");
  CHECK(udo->source->origin == "<packages/sourcepkg:echo>");
}

TEST("a package loads from its files") {
  auto files = package_files{
    {"package.yaml", "name: Files\n"},
    {"operators/nested/double.tql", "x = x * 2"},
    {"operators/notes.md", "not an operator"},
    {"pipelines/run.tql", "from {x: 1} | discard"},
    {"constants.tql", "let $base = 21"},
  };
  auto dh = collecting_diagnostic_handler{};
  auto loaded = package::load(files, "packages/files", "files", dh, false);
  REQUIRE(loaded);
  CHECK(std::move(dh).collect().empty());
  // The id comes from where the files are when the manifest has none.
  CHECK_EQUAL(loaded->id, "files");
  REQUIRE_EQUAL(loaded->operators.size(), size_t{1});
  CHECK(
    loaded->operators.contains(std::vector<std::string>{"nested", "double"}));
  CHECK(loaded->pipelines.contains("run"));
  CHECK_EQUAL(loaded->lets, "let $base = 21");
  // Only entities: the pipelines stay out.
  auto entities = package::load(files, "packages/files", "files", dh, true);
  REQUIRE(entities);
  CHECK(entities->pipelines.empty());
  CHECK_EQUAL(entities->operators.size(), size_t{1});
}

TEST("a package needs its manifest") {
  auto files = package_files{{"operators/double.tql", "x = x * 2"}};
  auto dh = collecting_diagnostic_handler{};
  CHECK(not package::load(files, "packages/files", "files", dh, true));
  auto diagnostics = std::move(dh).collect();
  REQUIRE(not diagnostics.empty());
  CHECK_EQUAL(diagnostics.front().message, "package.yaml does not exist");
}

TEST("packages refer to each other in any order") {
  auto load = [](package_files files, std::string_view id) {
    auto dh = null_diagnostic_handler{};
    auto loaded = package::load(files, id, id, dh, true);
    REQUIRE(loaded);
    return std::move(*loaded);
  };
  // `first` uses `second`, which is only added after it, and `second` refers
  // to a binding that does not exist: names resolve where they are used.
  auto packages = std::vector<package>{
    load({{"package.yaml", "name: First\n"},
          {"operators/apply.tql", "second::double"}},
         "first"),
    load({{"package.yaml", "name: Second\n"},
          {"operators/double.tql", "x = x * 2"},
          {"constants.tql", "let $base = first::$missing"}},
         "second"),
  };
  auto reg = test::builtin_registry().clone();
  auto dh = collecting_diagnostic_handler{};
  REQUIRE(add_packages(*reg, packages, dh));
  CHECK(std::move(dh).collect().empty());
  auto has_operator = [&](std::string module, std::string name) {
    auto path = entity_path{std::string{entity_pkg_packages},
                            {std::move(module), std::move(name)},
                            entity_ns::op};
    return is<entity_ref>(reg->try_get(path));
  };
  CHECK(has_operator("first", "apply"));
  CHECK(has_operator("second", "double"));
}

TEST("packages whose modules collide are not added") {
  auto make = [](std::string id) {
    auto pkg = package{};
    pkg.id = std::move(id);
    pkg.name = pkg.id;
    auto op = package_operator{};
    op.tql_body = "pass";
    pkg.operators.emplace(std::vector<std::string>{"op"}, std::move(op));
    return pkg;
  };
  auto packages = std::vector<package>{make("a-b"), make("a_b")};
  auto reg = test::builtin_registry().clone();
  auto dh = collecting_diagnostic_handler{};
  CHECK(not add_packages(*reg, packages, dh));
  // Neither was added, not even the one that came first.
  auto path = entity_path{
    std::string{entity_pkg_packages}, {"a_b", "op"}, entity_ns::op};
  CHECK(not is<entity_ref>(reg->try_get(path)));
}

} // namespace tenzir
