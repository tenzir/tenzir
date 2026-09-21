//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/nova/array_base.hpp"
#include "tenzir/nova/shape_table.hpp"
#include "tenzir/tql2/ast.hpp"

#include <memory>
#include <span>

namespace tenzir::nova {

/// Assigns a nested field, inserting new fields at `position` and retaining
/// existing field positions. Warns when an active scalar parent is replaced.
auto assign_nested_field(Array<Record> record,
                         std::span<ast::field_path::segment const> path,
                         MaskedArray<Array<Data>> value, diagnostic_handler& dh,
                         FieldPosition position = FieldPosition::back)
  -> Array<Record>;

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
