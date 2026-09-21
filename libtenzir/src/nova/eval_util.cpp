//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/eval_util.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/nova/array_merge.hpp"
#include "tenzir/nova/bitmap.hpp"
#include "tenzir/nova/union_array.hpp"
#include "tenzir/option.hpp"
#include "tenzir/variant_traits.hpp"

#include <algorithm>
#include <functional>
#include <ranges>
#include <string_view>
#include <vector>

namespace tenzir::nova {

auto assign_nested_field(Array<Record> record,
                         std::span<ast::field_path::segment const> path,
                         MaskedArray<Array<Data>> value, diagnostic_handler& dh,
                         FieldPosition position) -> Array<Record> {
  TENZIR_ASSERT(not path.empty());
  auto const& name = path[0].id.name;
  if (path.size() == 1) {
    return std::move(record).with_field_overwrite(name, std::move(value),
                                                  position);
  }
  auto const length = record.length();
  auto mask = value.present;
  auto existing = std::move(record).dangerously_extract_field(name);
  // Rows in `mask` that do not hold a record are replaced by an empty one.
  auto record_rows = storage::BitMap{length, false};
  auto sub_record = Array<Record>::make_empty(length);
  auto present = mask;
  // Non-record rows outside `mask` must keep their value.
  auto to_merge = Option<MaskedArray<Array<Data>>>{};
  if (existing) {
    auto warn
      = [&]<data_type Tag>(Array<Tag> const&, storage::BitMap const& active) {
          if constexpr (not std::same_as<Tag, Record>
                        and not std::same_as<Tag, Null>) {
            if (not(active & existing->present & mask).any()) {
              return;
            }
            diagnostic::warning("implicit record for `{}` field overwrites "
                                "`{}` value",
                                path[1].id.name, Type<Tag>::static_name)
              .primary(path[1].id)
              .hint("if this is intentional, drop the parent field before")
              .emit(dh);
          }
        };
    match(existing->data, [&](auto const& array) {
      if constexpr (std::same_as<std::remove_cvref_t<decltype(array)>,
                                 UnionArray>) {
        for (auto const& alternative : array.fields()) {
          match(alternative.data, [&](auto const& typed) {
            warn(typed, alternative.present);
          });
        }
      } else {
        warn(array, existing->present);
      }
    });
    if (auto alternative = existing->data.get_alternative<Record>()) {
      record_rows = std::move(alternative->present) & existing->present;
      sub_record
        = std::move(alternative->data).empty_where(record_rows.make_inverted());
    }
    present = existing->present | mask;
    if (existing->present.and_not(record_rows).and_not(mask).any()) {
      to_merge = std::move(existing);
    }
  }
  existing.reset();
  auto updated = Array<Data>{assign_nested_field(
    std::move(sub_record), path.subspan(1), std::move(value), dh, position)};
  if (to_merge) {
    updated = with_merged(*to_merge, MaskedArray<Array<Data>>{
                                       std::move(updated),
                                       std::move(record_rows) | mask,
                                     });
  }
  return std::move(record).with_field_overwrite(
    name, MaskedArray<Array<Data>>{std::move(updated), std::move(present)},
    position);
}

struct DropTree::Storage {
  std::vector<std::string_view> this_level_fields;
  std::vector<std::string_view> children_names;
  std::vector<DropTree> children;
};

DropTree::DropTree() : storage_{std::make_unique<Storage>()} {
}

DropTree::DropTree(DropTree&&) noexcept = default;

auto DropTree::operator=(DropTree&&) noexcept -> DropTree& = default;

DropTree::~DropTree() = default;

auto DropTree::make(std::span<ast::field_path const> moved) -> DropTree {
  auto root = DropTree{};
  for (auto const& field : moved) {
    auto path = field.path();
    if (path.empty()) {
      continue;
    }
    auto* node = &root;
    for (auto const& segment : path.first(path.size() - 1)) {
      auto it
        = std::ranges::find(node->storage_->children_names, segment.id.name);
      if (it == node->storage_->children_names.end()) {
        node->storage_->children_names.emplace_back(segment.id.name);
        node->storage_->children.emplace_back();
        node = &node->storage_->children.back();
      } else {
        node = &node->storage_->children[static_cast<std::size_t>(
          it - node->storage_->children_names.begin())];
      }
    }
    node->storage_->this_level_fields.push_back(path.back().id.name);
  }
  auto prune = [](DropTree& node, auto&& recurse) -> void {
    auto& fields = node.storage_->this_level_fields;
    std::ranges::sort(fields);
    fields.erase(std::ranges::unique(fields).begin(), fields.end());
    for (auto i = std::size_t{0}; i < node.storage_->children_names.size();) {
      if (std::ranges::find(fields, node.storage_->children_names[i])
          != fields.end()) {
        node.storage_->children_names.erase(
          node.storage_->children_names.begin()
          + static_cast<std::ptrdiff_t>(i));
        node.storage_->children.erase(node.storage_->children.begin()
                                      + static_cast<std::ptrdiff_t>(i));
      } else {
        ++i;
      }
    }
    for (auto& child : node.storage_->children) {
      recurse(child, recurse);
    }
  };
  prune(root, prune);
  return root;
}

auto DropTree::empty() const -> bool {
  return storage_->this_level_fields.empty() and storage_->children.empty();
}

auto DropTree::apply(Array<Record> record, storage::BitMap mask) const
  -> Array<Record> {
  if (not storage_->this_level_fields.empty()) {
    record
      = std::move(record).without_fields(storage_->this_level_fields, mask);
  }
  for (auto&& [name, child] :
       std::views::zip(storage_->children_names, storage_->children)) {
    auto const has_record = std::invoke([&] {
      auto preview = record.field(name);
      return preview and preview->data.get_alternative<Record>().is_some();
    });
    if (not has_record) {
      continue;
    }
    auto existing = std::move(record).dangerously_extract_field(name);
    TENZIR_ASSERT(existing);
    auto alternative = existing->data.get_alternative<Record>();
    TENZIR_ASSERT(alternative);
    auto record_rows = std::move(alternative->present) & existing->present;
    // Non-record rows must keep their value.
    auto present = existing->present;
    auto to_merge = Option<MaskedArray<Array<Data>>>{};
    if (existing->present.and_not(record_rows).any()) {
      to_merge = std::move(existing);
    }
    existing.reset();
    auto updated = Array<Data>{
      child.apply(std::move(alternative->data), record_rows & mask)};
    if (to_merge) {
      updated = with_merged(*to_merge, MaskedArray<Array<Data>>{
                                         std::move(updated),
                                         std::move(record_rows),
                                       });
    }
    record = std::move(record).with_field_overwrite(
      name, MaskedArray<Array<Data>>{std::move(updated), std::move(present)});
  }
  return record;
}

} // namespace tenzir::nova
