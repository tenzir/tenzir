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
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/registry.hpp"

namespace tenzir {

TEST("build_package_operator_module populates UDO source") {
  auto pkg = package{};
  pkg.id = "sourcepkg";
  pkg.name = "Source Package";
  auto op = package_operator{};
  op.tql_body = "where true";
  pkg.operators.emplace(std::vector<std::string>{"echo"}, std::move(op));
  auto dh = null_diagnostic_handler{};
  auto module = build_package_operator_module(pkg, dh);
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

} // namespace tenzir
