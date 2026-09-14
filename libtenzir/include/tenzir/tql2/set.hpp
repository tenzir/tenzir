//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/diagnostics.hpp"
#include "tenzir/option.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/tql2/ast.hpp"

namespace tenzir {

/// Creates a record that maps `path` to `value`.
///
/// # Examples
//
/// ["foo", "bar"] -> {"foo": {"bar": value}}
/// [] -> value
[[nodiscard]] auto consume_path(std::span<const ast::field_path::segment> path,
                                series value) -> series;

enum class assign_position {
  front,
  back,
};

[[nodiscard]] auto assign(std::span<const ast::field_path::segment> left,
                          series right, series input, diagnostic_handler& dh,
                          assign_position position) -> series;

[[nodiscard]] auto
assign(const ast::selector& left, series right, const table_slice& input,
       diagnostic_handler& dh, assign_position position = assign_position::back)
  -> std::vector<table_slice>;

[[nodiscard]] auto
assign(const ast::field_path& left, series right, const table_slice& input,
       diagnostic_handler& dh, assign_position position = assign_position::back)
  -> table_slice;

[[nodiscard]] auto assign(const ast::meta& left, const series& right,
                          const table_slice& input, diagnostic_handler& diag)
  -> std::vector<table_slice>;

[[nodiscard]] auto resolve_move_keyword(ast::assignment assignment)
  -> std::pair<ast::assignment, std::vector<ast::field_path>>;

/// Validates that the left-hand side of an assignment describes a selector.
///
/// Emits a diagnostic and fails for expressions that cannot be assigned to.
/// This includes `$` variables, which only bind names to constant values.
[[nodiscard]] auto resolve_assignment_left(const ast::assignment& assignment,
                                           diagnostic_handler& dh)
  -> failure_or<ast::selector>;

[[nodiscard]] auto
drop(const table_slice& slice, std::span<const ast::field_path> fields,
     diagnostic_handler& dh, bool warn_for_duplicates) -> table_slice;

} // namespace tenzir
