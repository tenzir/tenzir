//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/compile.hpp"

#include "tenzir/compile_ctx.hpp"
#include "tenzir/try.hpp"

namespace tenzir {

auto compile(ast::pipeline ast, base_ctx ctx) -> failure_or<CompiledPipeline> {
  auto root = compile_ctx::make_root(ctx);
  TRY(auto ir, std::move(ast).compile(root));
  return CompiledPipeline{std::move(ir), std::move(root).source_map()};
}

} // namespace tenzir
