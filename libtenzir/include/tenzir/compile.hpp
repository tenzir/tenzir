//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/base_ctx.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/source_map.hpp"

namespace tenzir {

/// The result of compiling an AST pipeline into IR.
struct CompiledPipeline {
  /// The compiled IR pipeline.
  ir::pipeline ir;

  /// The source map populated during compilation.
  SourceMap source_map;

  friend auto inspect(auto& f, CompiledPipeline& x) -> bool {
    return f.object(x)
      .pretty_name("compiled_pipeline")
      .fields(f.field("ir", x.ir), f.field("source_map", x.source_map));
  }
};

/// Compile an AST pipeline into IR.
///
/// This is the entry point into compilation. It creates the compilation
/// context internally and returns the resulting IR together with the source
/// map that was populated during compilation.
auto compile(ast::pipeline ast, base_ctx ctx) -> failure_or<CompiledPipeline>;

} // namespace tenzir
