//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/ir.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/test/test.hpp>

using namespace tenzir;

TEST("from_velociraptor declines every stage with its reason") {
  auto const* plugin = plugins::find<OperatorPlugin>("from_velociraptor");
  REQUIRE(plugin);
  auto desc = plugin->describe();
  REQUIRE(desc.set_plan);
  REQUIRE(desc.accepts_plan);
  auto args = desc.make_args();
  auto chain = ir::QueryChain{
    .stages = {ir::QueryStage{ir::QueryStage::Limit{1}}},
    .end = "",
  };
  auto absorption = (*desc.accepts_plan)(args, chain);
  CHECK_EQUAL(absorption.prefix, size_t{0});
  CHECK_EQUAL(absorption.declined,
              "a VQL query may have effects that a filter or limit would "
              "change; `max_rows` only sizes response batches");
}
