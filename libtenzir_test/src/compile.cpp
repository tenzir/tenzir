//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/test/compile.hpp"

#include "tenzir/compile_ctx.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/session.hpp"
#include "tenzir/source.hpp"
#include "tenzir/tql2/parser.hpp"
#include "tenzir/tql2/tokens.hpp"

#include <tuple>

namespace tenzir::test {

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

} // namespace tenzir::test
