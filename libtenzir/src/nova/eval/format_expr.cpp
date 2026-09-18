//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/eval_internal.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova/fundamental_array_builder.hpp"
#include "tenzir/nova/stringify.hpp"
#include "tenzir/tql2/ast.hpp"

#include <string>
#include <utility>
#include <vector>

namespace tenzir::nova {

auto _::EvalRun::eval(ast::format_expr const& x, EvalFrame frame)
  -> Array<Data> {
  auto const& mask = frame.mask();
  using Segment = variant<std::string, Array<String>>;
  auto segments = std::vector<Segment>{};
  segments.reserve(x.segments.size());
  for (auto const& segment : x.segments) {
    match(
      segment,
      [&](std::string const& literal) {
        segments.emplace_back(literal);
      },
      [&](ast::format_expr::replacement const& replacement) {
        auto value = frame.eval(replacement.expr);
        segments.emplace_back(stringify(value, mask));
      });
  }
  auto builder = ArrayBuilder<String>{};
  auto buffer = std::string{};
  for (auto index : storage::bitmap_iteration(mask)) {
    if (not index) {
      builder.skip();
      continue;
    }
    for (auto const& segment : segments) {
      match(
        segment,
        [&](std::string const& literal) {
          buffer.append(literal);
        },
        [&](Array<String> const& replacement) {
          buffer.append(*replacement.get(*index));
        });
    }
    builder.data(buffer);
    buffer.clear();
  }
  return Array<Data>{builder.finish()};
}

} // namespace tenzir::nova
