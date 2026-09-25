//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/nova/array.hpp"
#include "tenzir/nova/shape_table.hpp"
#include "tenzir/tql2/ast.hpp"

#include <cstddef>
#include <memory>
#include <span>
#include <string_view>

namespace tenzir::nova {

struct FieldPathLookup {
  Option<RowView<Data>> value;
  std::size_t matched_segments;
  Option<std::string_view> non_record_type;
};

/// Looks up an exact field path in a record row. `matched_segments` identifies
/// the first unmatched path segment when `value` is none.
auto lookup_field_path(RowView<Record> record,
                       std::span<ast::field_path::segment const> path)
  -> FieldPathLookup;

/// Assigns a nested field, inserting new fields at `position` and retaining
/// existing field positions. Warns when an active scalar parent is replaced.
auto assign_nested_field(Array<Record> record,
                         std::span<ast::field_path::segment const> path,
                         MaskedArray<Array<Data>> value, diagnostic_handler& dh,
                         FieldPosition position = FieldPosition::back)
  -> Array<Record>;

/// The value of a `this = <expr>` assignment: the records of `value`, with an
/// empty record in every other row. Warns about present rows that are neither
/// records nor `null`, like the legacy assignment to `this`.
auto records_or_empty(MaskedArray<Array<Data>> value, storage::Index length,
                      location rhs, diagnostic_handler& dh) -> Array<Record>;

/// A nested-field-drop specification mirroring the shape of
/// `assign_nested_field`'s path recursion, but for removal instead of writing.
class DropTree {
public:
  DropTree();
  DropTree(DropTree const&) = delete;
  DropTree(DropTree&&) noexcept;
  auto operator=(DropTree const&) -> DropTree& = delete;
  auto operator=(DropTree&&) noexcept -> DropTree&;
  ~DropTree();

  /// Builds a tree whose string views alias `moved`'s field-path storage.
  static auto make(std::span<ast::field_path const> moved) -> DropTree;

  auto empty() const -> bool;
  auto apply(Array<Record> record, storage::BitMap mask) const -> Array<Record>;

private:
  struct Storage;
  std::unique_ptr<Storage> storage_;
};

} // namespace tenzir::nova
