//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/tql2/ast.hpp"

#include <span>
#include <vector>

namespace tenzir {

enum class event_order;

/// Drops fields whose value is null from each row of `slice`.
///
/// When `selectors` is empty, every field of the slice (recursively) is
/// considered. Otherwise, only the named fields and the fields nested under
/// them are considered. Rows are grouped by their null pattern, so the result
/// may contain multiple slices with different schemas.
///
/// The `order` parameter controls whether row order is preserved: with
/// `event_order::ordered` only adjacent rows with the same null pattern are
/// merged; with `event_order::unordered` the function may reorder rows to
/// collapse equal null patterns into fewer output slices. The unordered fast
/// path is only safe when the slice has no offset metadata.
auto drop_null_fields(table_slice slice,
                      std::span<const ast::field_path> selectors,
                      event_order order, diagnostic_handler& dh)
  -> std::vector<table_slice>;

} // namespace tenzir
