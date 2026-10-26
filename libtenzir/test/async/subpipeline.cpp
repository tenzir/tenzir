//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/compile_ctx.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/session.hpp"
#include "tenzir/source.hpp"
#include "tenzir/test/registry.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/parser.hpp"
#include "tenzir/tql2/resolve.hpp"
#include "tenzir/tql2/tokens.hpp"

using namespace tenzir;

TEST("planning respects concurrent output when selecting channel kinds") {
  auto dh = collecting_diagnostic_handler{};
  auto const& reg = test::builtin_registry();
  auto provider = session_provider::make(dh, reg);
  auto session = provider.as_session();
  auto source = Source::new_source("group id { summarize id, n=count() } | "
                                   "marker = true | sort id",
                                   "<test>", false);
  auto sources = SourceMap{};
  sources.add_source(source);
  auto tokens = tokenize_permissive(source->text);
  auto parsed = parse(tokens, *source, session);
  REQUIRE(parsed);
  REQUIRE(resolve_entities(*parsed, ExternPrelude::empty(), session));
  auto root = compile_ctx::make_root(base_ctx{dh, reg}, sources);
  auto compiled = std::move(*parsed).compile(root);
  REQUIRE(compiled);
  for (auto fuse :
       {ir::parallelism::Fusing::none, ir::parallelism::Fusing::all}) {
    auto settings = ir::parallelism::disabled;
    settings.fuse = fuse;
    auto plan = ir::make_plan(*compiled, tag_v<nova::Events>, base_ctx{dh, reg},
                              settings);
    REQUIRE(plan);
    auto concurrent_edges = size_t{0};
    auto serial_edges = size_t{0};
    for (auto const& channel : plan->channels) {
      if (channel.from == ir::Port::input or channel.to == ir::Port::output) {
        CHECK(channel.kind == ir::ChannelKind::regular);
      } else if (plan->operators[channel.from].op->concurrent_output()) {
        ++concurrent_edges;
        CHECK(channel.kind != ir::ChannelKind::fused);
      } else {
        ++serial_edges;
        CHECK((channel.kind == ir::ChannelKind::fused)
              == (fuse == ir::parallelism::Fusing::all));
      }
    }
    CHECK_GREATER(concurrent_edges, 0u);
    CHECK_GREATER(serial_edges, 0u);
  }
}
