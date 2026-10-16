//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

/// Resolving packages in a root the caller chooses.
///
/// One process may hold several sets of packages at once: a deployment that
/// has taken a new configuration while pipelines compiled against the previous
/// one are still draining. Each set gets a root of its own, and a snapshot
/// says which root it resolves in, so a path resolves to the set it was
/// compiled against and keeps naming it after another set is published.

#include "tenzir/test/test.hpp"
#include "tenzir/tql2/entity_path.hpp"
#include "tenzir/tql2/registry.hpp"

#include <memory>
#include <string>
#include <utility>

using namespace tenzir;

namespace {

/// A module with one `let` binding of the given value, which is the cheapest
/// entity to put in a registry without a plugin behind it.
auto module_with(std::string name, int64_t value)
  -> std::unique_ptr<module_def> {
  auto mod = std::make_unique<module_def>();
  mod->defs[std::move(name)].let_def
    = ast::expression{ast::constant{value, location::unknown}};
  return mod;
}

/// What the `let` under `pkg::foo::answer` holds, or nothing.
auto answer_in(const registry& reg, const entity_pkg& pkg) -> Option<int64_t> {
  auto path = entity_path{pkg, {"foo", "answer"}, entity_ns::let};
  auto result = reg.try_get(path);
  auto* ref = try_as<entity_ref>(&result);
  if (not ref) {
    return None{};
  }
  auto* def = try_as<std::reference_wrapper<const ast::expression>>(ref);
  if (not def) {
    return None{};
  }
  auto* constant = try_as<ast::constant>(&def->get());
  if (not constant) {
    return None{};
  }
  auto* number = try_as<int64_t>(&constant->value);
  if (not number) {
    return None{};
  }
  return *number;
}

} // namespace

TEST("a registry resolves packages in the default root") {
  auto reg = registry{};
  CHECK_EQUAL(reg.packages_root(), std::string{entity_pkg_packages});
}

TEST("a snapshot can resolve packages in a root of its own") {
  auto reg = registry{};
  reg.replace_module("packages@first", "foo", module_with("answer", 1));
  reg.replace_module("packages@second", "foo", module_with("answer", 2));
  // Both sets are in the one registry, told apart by their roots, so the
  // snapshot's root is what decides which of them `foo` means.
  reg.set_packages_root("packages@first");
  CHECK_EQUAL(reg.packages_root(), std::string{"packages@first"});
  REQUIRE(answer_in(reg, reg.packages_root()));
  CHECK_EQUAL(*answer_in(reg, reg.packages_root()), int64_t{1});
  reg.set_packages_root("packages@second");
  REQUIRE(answer_in(reg, reg.packages_root()));
  CHECK_EQUAL(*answer_in(reg, reg.packages_root()), int64_t{2});
}

/// The point of putting the root in the path rather than in the lookup: a path
/// resolved against one set still finds that set once another is published,
/// which is what lets a draining pipeline keep running on the packages it was
/// compiled against.
TEST("a resolved path keeps naming the set it was resolved against") {
  auto reg = registry{};
  reg.replace_module("packages@first", "foo", module_with("answer", 1));
  reg.set_packages_root("packages@first");
  auto resolved
    = entity_path{reg.packages_root(), {"foo", "answer"}, entity_ns::let};
  // A newer set arrives, holding a `foo` of its own, and becomes the one that
  // fresh compilations resolve in.
  auto next = reg.clone();
  next->replace_module("packages@second", "foo", module_with("answer", 2));
  next->set_packages_root("packages@second");
  CHECK_EQUAL(next->packages_root(), std::string{"packages@second"});
  REQUIRE(answer_in(*next, next->packages_root()));
  CHECK_EQUAL(*answer_in(*next, next->packages_root()), int64_t{2});
  // The path from before resolves in the newer snapshot too, and to what it
  // always meant.
  auto result = next->try_get(resolved);
  auto* ref = try_as<entity_ref>(&result);
  REQUIRE(ref);
  REQUIRE(answer_in(*next, "packages@first"));
  CHECK_EQUAL(*answer_in(*next, "packages@first"), int64_t{1});
}

TEST("cloning a registry carries its packages root") {
  auto reg = registry{};
  reg.replace_module("packages@first", "foo", module_with("answer", 1));
  reg.set_packages_root("packages@first");
  auto clone = reg.clone();
  CHECK_EQUAL(clone->packages_root(), std::string{"packages@first"});
  REQUIRE(answer_in(*clone, clone->packages_root()));
  CHECK_EQUAL(*answer_in(*clone, clone->packages_root()), int64_t{1});
}
