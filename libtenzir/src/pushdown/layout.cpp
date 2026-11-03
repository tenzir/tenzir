//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pushdown/layout.hpp"

#include "tenzir/nova/eval_util.hpp"
#include "tenzir/tql2/ast.hpp"

namespace tenzir::pushdown {

namespace {

auto segments(std::span<std::string const> path)
  -> std::vector<ast::field_path::segment> {
  auto result = std::vector<ast::field_path::segment>{};
  for (auto const& name : path) {
    result.push_back({
      .id = ast::identifier{name, location::unknown},
      .has_question_mark = false,
    });
  }
  return result;
}

} // namespace

auto apply_layout(nova::Events events, Layout const& layout,
                  diagnostic_handler& dh) -> nova::Events {
  auto const length = events.length();
  auto meta = layout.schema_name
                ? nova::Events::Meta::make_empty(length, *layout.schema_name)
                : std::move(events.meta);
  if (not layout.fields) {
    if (not layout.hidden.empty()) {
      auto hidden = std::vector<ast::field_path>{};
      for (auto const& name : layout.hidden) {
        auto path = ast::field_path::try_from(
          ast::root_field{ast::identifier{name, location::unknown}, false});
        TENZIR_ASSERT(path);
        hidden.push_back(std::move(*path));
      }
      events.data = nova::DropTree::make(hidden).apply(std::move(events.data),
                                                       events.mask);
    }
    return nova::Events{std::move(events.data), std::move(events.mask),
                        std::move(meta)};
  }
  auto result = nova::Array<nova::Record>::make_empty(length);
  for (auto const& [field, column] : *layout.fields) {
    auto value = events.data.field(column);
    if (not value) {
      continue;
    }
    result = nova::assign_nested_field(std::move(result), segments(field),
                                       std::move(*value), dh);
  }
  return nova::Events{std::move(result), std::move(events.mask),
                      std::move(meta)};
}

} // namespace tenzir::pushdown
