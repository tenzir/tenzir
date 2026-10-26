//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/package.hpp"

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

#include <string>
#include <tuple>
#include <vector>

namespace tenzir {

namespace {

/// The package made of `files`, with `id` as its id.
auto load(package_files files, std::string_view id) -> package {
  auto dh = null_diagnostic_handler{};
  auto loaded = package::load(files, id, id, dh, true);
  REQUIRE(loaded);
  return std::move(*loaded);
}

/// A workspace that uses the packages `ids`.
auto workspace_using(std::vector<std::string> ids) -> PackageDependencies {
  auto result = PackageDependencies{};
  for (auto& id : ids) {
    result.emplace(std::move(id), PackageDependency{});
  }
  return result;
}

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

/// Whether `messages` holds one that contains `part`.
auto mentions(std::vector<std::string> const& messages, std::string_view part)
  -> bool {
  return std::ranges::any_of(messages, [&](std::string const& message) {
    return message.contains(part);
  });
}

} // namespace

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

TEST("a package declares its version and what it uses") {
  auto files = package_files{
    {"package.yaml", "name: Acme\n"
                     "version: 1.2.0\n"
                     "dependencies:\n"
                     "  base: \"^1.0\"\n"
                     "  internal: {version: \"^0.3\", git: \"https://x\"}\n"
                     "  pinned: {git: \"https://y\", rev: \"3f2a\"}\n"
                     "  tools: {path: packages/tools}\n"
                     "  any: {}\n"},
  };
  auto dh = collecting_diagnostic_handler{};
  auto loaded = package::load(files, "packages/acme", "acme", dh, true);
  REQUIRE(loaded);
  CHECK(std::move(dh).collect().empty());
  CHECK_EQUAL(loaded->version, Option<std::string>{"1.2.0"});
  auto const& deps = loaded->dependencies;
  REQUIRE_EQUAL(deps.size(), size_t{5});
  CHECK_EQUAL(deps.at("base").version, Option<std::string>{"^1.0"});
  CHECK_EQUAL(deps.at("internal").version, Option<std::string>{"^0.3"});
  CHECK_EQUAL(deps.at("internal").git, Option<std::string>{"https://x"});
  CHECK_EQUAL(deps.at("pinned").rev, Option<std::string>{"3f2a"});
  CHECK(not deps.at("pinned").version);
  CHECK_EQUAL(deps.at("tools").path, Option<std::string>{"packages/tools"});
  CHECK(not deps.at("any").version);
  // What it says survives a round trip through its record.
  auto again = package::parse(make_view(loaded->to_record()));
  REQUIRE(again);
  CHECK_EQUAL(again->version, loaded->version);
  CHECK_EQUAL(again->dependencies.size(), deps.size());
  CHECK_EQUAL(again->dependencies.at("internal").git,
              Option<std::string>{"https://x"});
}

TEST("a package's dependencies must be well-formed") {
  for (auto manifest : {
         "name: A\ndependencies: [base]\n",
         "name: A\ndependencies:\n  base: 1\n",
         "name: A\ndependencies:\n  base: {branch: main}\n",
         "name: A\ndependencies:\n  1base: \"^1\"\n",
       }) {
    auto files = package_files{{"package.yaml", manifest}};
    auto dh = collecting_diagnostic_handler{};
    CHECK(not package::load(files, "packages/acme", "acme", dh, true));
  }
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
  REQUIRE(add_packages(*reg, packages, dh, nullptr,
                       PackageVisibility::all_deprecated));
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
  CHECK(not add_packages(*reg, packages, dh, nullptr,
                         PackageVisibility::all_deprecated));
  // Neither was added, not even the one that came first.
  auto path = entity_path{
    std::string{entity_pkg_packages}, {"a_b", "op"}, entity_ns::op};
  CHECK(not is<entity_ref>(reg->try_get(path)));
}

TEST("a package sees only the packages it declares") {
  auto acme = load({{"package.yaml", "name: P\n"},
                    {"operators/double.tql", "x = x * 2"}},
                   "acme");
  auto tools = load({{"package.yaml", "name: P\ndependencies:\n  acme: {}\n"},
                     {"operators/twice.tql", "acme::double | acme::double"}},
                    "tools");
  auto rogue = load({{"package.yaml", "name: P\n"},
                     {"operators/sneak.tql", "acme::double"}},
                    "rogue");
  auto reg = test::builtin_registry().clone();
  auto dh = collecting_diagnostic_handler{};
  auto declared = std::vector<package>{acme, tools};
  REQUIRE(
    add_packages(*reg, declared, dh, nullptr, PackageVisibility::declared));
  CHECK(std::move(dh).collect().empty());
  // `rogue` uses `acme` without declaring it, and fails to add rather than to
  // run, leaving the registry as it was.
  auto other = test::builtin_registry().clone();
  auto rogue_dh = collecting_diagnostic_handler{};
  auto undeclared = std::vector<package>{acme, rogue};
  CHECK(not add_packages(*other, undeclared, rogue_dh, nullptr,
                         PackageVisibility::declared));
  auto messages = std::vector<std::string>{};
  for (auto& diagnostic : std::move(rogue_dh).collect()) {
    messages.push_back(std::move(diagnostic.message));
  }
  CHECK(mentions(messages, "package `acme` is not among the dependencies"));
  auto sneak = entity_path{
    std::string{entity_pkg_packages}, {"rogue", "sneak"}, entity_ns::op};
  CHECK(not is<entity_ref>(other->try_get(sneak)));
  // Pipelines see what the workspace declares, and not what its packages do.
  auto prelude = extern_prelude(workspace_using({"tools"}));
  CHECK(compile_errors(*reg, prelude, "from {x: 1} | tools::twice | discard")
          .empty());
  CHECK(mentions(compile_errors(*reg, prelude,
                                "from {x: 1} | acme::double | discard"),
                 "package `acme` is not among the dependencies"));
  // Code that a builtin writes names no package at all.
  CHECK(mentions(compile_errors(*reg, ExternPrelude::empty(),
                                "from {x: 1} | tools::twice | discard"),
                 "package `tools` is not among the dependencies"));
}

TEST("a package's constants see only the packages it declares") {
  auto base = load({{"package.yaml", "name: P\n"},
                    {"constants.tql", "let $value = 21"}},
                   "base");
  auto user = load({{"package.yaml", "name: P\ndependencies:\n  base: {}\n"},
                    {"constants.tql", "let $twice = base::$value * 2"}},
                   "user");
  auto rogue = load({{"package.yaml", "name: P\n"},
                     {"constants.tql", "let $copy = base::$value"}},
                    "rogue");
  auto reg = test::builtin_registry().clone();
  auto dh = collecting_diagnostic_handler{};
  auto declared = std::vector<package>{base, user};
  REQUIRE(
    add_packages(*reg, declared, dh, nullptr, PackageVisibility::declared));
  // `user::$twice` names `base`, which the workspace does not: it resolves
  // against what `user` may name.
  CHECK(compile_errors(*reg, extern_prelude(workspace_using({"user"})),
                       "from {x: user::$twice} | discard")
          .empty());
  auto other = test::builtin_registry().clone();
  auto undeclared = std::vector<package>{base, rogue};
  CHECK(not add_packages(*other, undeclared, dh, nullptr,
                         PackageVisibility::declared));
}

TEST("a declared package has to be there") {
  auto tools = load({{"package.yaml", "name: P\ndependencies:\n  acme: {}\n"},
                     {"operators/twice.tql", "pass"}},
                    "tools");
  auto packages = std::vector<package>{tools};
  auto reg = test::builtin_registry().clone();
  auto dh = collecting_diagnostic_handler{};
  CHECK(
    not add_packages(*reg, packages, dh, nullptr, PackageVisibility::declared));
  CHECK(not require_dependencies(
    "the workspace", workspace_using({"tools", "missing"}), packages, dh));
  auto messages = std::vector<std::string>{};
  for (auto& diagnostic : std::move(dh).collect()) {
    messages.push_back(std::move(diagnostic.message));
  }
  CHECK(mentions(messages, "package `tools` uses `acme`, which is not "
                           "installed"));
  CHECK(mentions(messages, "the workspace uses `missing`, which is not "
                           "installed"));
}

TEST("packages may not use each other") {
  auto a = load({{"package.yaml", "name: P\ndependencies:\n  b: {}\n"},
                 {"operators/one.tql", "pass"}},
                "a");
  auto b = load({{"package.yaml", "name: P\ndependencies:\n  a: {}\n"},
                 {"operators/two.tql", "pass"}},
                "b");
  auto reg = test::builtin_registry().clone();
  auto dh = collecting_diagnostic_handler{};
  auto packages = std::vector<package>{a, b};
  CHECK(
    not add_packages(*reg, packages, dh, nullptr, PackageVisibility::declared));
  auto messages = std::vector<std::string>{};
  for (auto& diagnostic : std::move(dh).collect()) {
    messages.push_back(std::move(diagnostic.message));
  }
  CHECK(mentions(messages, "packages depend on each other: `a` uses `b` uses "
                           "`a`"));
}

TEST("a package is resolved after the packages it uses") {
  // `top` comes first, but resolves only once `middle` and `bottom` have, so
  // that `middle::$value` holds what `middle` may name, `bottom` included.
  auto top = load({{"package.yaml", "name: P\ndependencies:\n  middle: {}\n"},
                   {"constants.tql", "let $value = middle::$value + 1"}},
                  "top");
  auto middle
    = load({{"package.yaml", "name: P\ndependencies:\n  bottom: {}\n"},
            {"constants.tql", "let $value = bottom::$value + 1"}},
           "middle");
  auto bottom
    = load({{"package.yaml", "name: P\n"}, {"constants.tql", "let $value = 1"}},
           "bottom");
  auto reg = test::builtin_registry().clone();
  auto dh = collecting_diagnostic_handler{};
  auto packages = std::vector<package>{top, middle, bottom};
  REQUIRE(
    add_packages(*reg, packages, dh, nullptr, PackageVisibility::declared));
  CHECK(compile_errors(*reg, extern_prelude(workspace_using({"top"})),
                       "from {x: top::$value} | discard")
          .empty());
}

TEST("packages that declare nothing resolve where they are used") {
  auto broken = load({{"package.yaml", "name: P\n"},
                      {"operators/oops.tql", "does_not_exist"}},
                     "broken");
  auto reg = test::builtin_registry().clone();
  auto dh = collecting_diagnostic_handler{};
  auto packages = std::vector<package>{broken};
  // Deprecated, but as before: a package that names what is not there adds,
  // and fails where it is used.
  REQUIRE(add_packages(*reg, packages, dh, nullptr,
                       PackageVisibility::all_deprecated));
  CHECK(mentions(compile_errors(*reg, ExternPrelude::all_packages_deprecated(),
                                "from {x: 1} | broken::oops | discard"),
                 "does_not_exist"));
}

TEST("a package may not use itself") {
  auto self = load({{"package.yaml", "name: P\ndependencies:\n  self: {}\n"},
                    {"operators/op.tql", "pass"}},
                   "self");
  auto reg = test::builtin_registry().clone();
  auto dh = collecting_diagnostic_handler{};
  auto packages = std::vector<package>{self};
  CHECK(
    not add_packages(*reg, packages, dh, nullptr, PackageVisibility::declared));
  auto messages = std::vector<std::string>{};
  for (auto& diagnostic : std::move(dh).collect()) {
    messages.push_back(std::move(diagnostic.message));
  }
  CHECK(mentions(messages, "package `self` uses itself"));
}

TEST("only the package that fails is reported") {
  auto broken = load({{"package.yaml", "name: P\n"},
                      {"constants.tql", "let $value = does_not_exist()"}},
                     "broken");
  auto user = load({{"package.yaml", "name: P\ndependencies:\n  broken: {}\n"},
                    {"constants.tql", "let $copy = broken::$value"}},
                   "user");
  auto reg = test::builtin_registry().clone();
  auto dh = collecting_diagnostic_handler{};
  auto packages = std::vector<package>{user, broken};
  CHECK(
    not add_packages(*reg, packages, dh, nullptr, PackageVisibility::declared));
  for (auto& diagnostic : std::move(dh).collect()) {
    CHECK(not diagnostic.message.contains("broken::$value"));
  }
}

TEST("a dependency's version and revision are checked") {
  for (auto manifest : {
         "name: A\nversion: 1.2\n",
         "name: A\ndependencies:\n  base: 1.2\n",
         "name: A\ndependencies:\n  base: {version: 1.2}\n",
       }) {
    auto files = package_files{{"package.yaml", manifest}};
    auto dh = collecting_diagnostic_handler{};
    CHECK(not package::load(files, "packages/acme", "acme", dh, true));
    auto diagnostics = std::move(dh).collect();
    REQUIRE(not diagnostics.empty());
    auto hinted = std::ranges::any_of(diagnostics, [](diagnostic const& d) {
      return std::ranges::any_of(d.notes, [](diagnostic_note const& note) {
        return note.message.contains("quote it");
      });
    });
    CHECK(hinted);
  }
  auto files = package_files{
    {"package.yaml", "name: A\ndependencies:\n  base: {rev: \"3f2a\"}\n"}};
  auto dh = collecting_diagnostic_handler{};
  CHECK(not package::load(files, "packages/acme", "acme", dh, true));
}

} // namespace tenzir
