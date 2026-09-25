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
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/drop_null_fields.hpp"
#include "tenzir/nova/shape_table.hpp"
#include "tenzir/nova/union_array.hpp"
#include "tenzir/option.hpp"
#include "tenzir/variant_traits.hpp"

#include <algorithm>
#include <functional>
#include <ranges>
#include <string_view>
#include <vector>

namespace tenzir::nova {

auto lookup_field_path(RowView<Record> record,
                       std::span<ast::field_path::segment const> path)
  -> FieldPathLookup {
  TENZIR_ASSERT(not path.empty());
  for (auto [name, value] : record) {
    if (name != path.front().id.name) {
      continue;
    }
    if (path.size() == 1) {
      return {.value = value, .matched_segments = 1, .non_record_type = None{}};
    }
    return match(value, [&](auto view) -> FieldPathLookup {
      using T = std::remove_cvref_t<decltype(view)>;
      if constexpr (std::same_as<T, RowView<Record>>) {
        auto result = lookup_field_path(view, path.subspan(1));
        result.matched_segments += 1;
        return result;
      }
      return {.value = None{},
              .matched_segments = 1,
              .non_record_type = []<data_type Tag>(RowView<Tag>) {
                return Type<Tag>::static_name;
              }(view)};
    });
  }
  return {.value = None{}, .matched_segments = 0, .non_record_type = None{}};
}

auto NullFieldSelection::apply(Array<Record>& record,
                               storage::BitMap const& active) const -> bool {
  if (not active.any()) {
    return false;
  }
  struct Update {
    std::size_t index;
    storage::BitMap removed;
    std::vector<Update> children;
  };
  // Keep only edits, not array handles: the mutation pass can then consume
  // uniquely owned children, while a no-op leaves even the structure shared.
  auto prepare
    = [&](NullFieldSelection const& selection, Array<Record> const& record,
          storage::BitMap const& active, auto&& self) -> std::vector<Update> {
    auto updates = std::vector<Update>{};
    auto visit = [&](std::string_view name, std::size_t index) {
      auto const* child = &selection;
      if (not selection.recursive) {
        auto it = selection.children.find(std::string{name});
        if (it == selection.children.end()) {
          return;
        }
        child = &it->second;
      }
      auto field = record.field(name);
      TENZIR_ASSERT(field);
      auto mask = active & field->present;
      if (not mask.any()) {
        return;
      }
      auto removed = storage::BitMap{record.length(), false};
      if (child->recursive) {
        if (auto nulls = field->data.get_alternative<Null>()) {
          removed = mask & nulls->present;
        }
      }
      auto nested = std::vector<Update>{};
      if (auto records = field->data.get_alternative<Record>()) {
        auto rows = mask & records->present;
        if (rows.any()) {
          nested = self(*child, records->data, rows, self);
        }
      }
      if (removed.any() or not nested.empty()) {
        updates.push_back({index, std::move(removed), std::move(nested)});
      }
    };
    match(record.storage(), [&](auto const& source) {
      if constexpr (std::same_as<std::remove_cvref_t<decltype(source)>,
                                 storage::RecordStorage>) {
        for (auto const& [name, index] : (*source).names) {
          visit(name, index);
        }
      } else {
        auto index = std::size_t{0};
        for (auto const& [name, value] : source.value()) {
          visit(name, index++);
        }
      }
    });
    return updates;
  };
  auto updates = prepare(*this, record, active, prepare);
  if (updates.empty()) {
    return false;
  }
  auto apply = [&](Array<Record> record, std::vector<Update> const& updates,
                   auto&& self) -> Array<Record> {
    record = std::move(record).to_primary().as_unique();
    auto& data = record.primary();
    auto indices = Option<Array<Record>::IndicesStorage::Mutable>{};
    for (auto const& [index, removed, children] : updates) {
      auto& field = data.arrays[index];
      if (not children.empty()) {
        field.data
          = std::move(field.data).map_alternative<Record>([&](auto records) {
              return self(std::move(records.data), children, self);
            });
      }
      if (removed.any()) {
        if (not indices) {
          indices.emplace(std::move(data.shape_indices));
        }
        storage::for_each_true(removed, [&](auto row) {
          auto& shape = indices->data()[row];
          shape = data.shape_table.without_field(
            shape, static_cast<storage::Index>(index));
        });
        field.present = std::move(field.present).and_not(removed);
      }
    }
    if (indices) {
      data.shape_indices = std::move(*indices).finish();
    }
    return record;
  };
  record = apply(std::move(record), updates, apply);
  return true;
}

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
    auto active = [&]() -> storage::BitMap {
      auto preview = record.field(name);
      if (preview) {
        if (auto records = preview->data.get_alternative<Record>()) {
          return mask & preview->present & records->present;
        }
      }
      return storage::BitMap{record.length(), false};
    }();
    if (not active.any()) {
      continue;
    }
    auto existing = std::move(record).dangerously_extract_field(name);
    TENZIR_ASSERT(existing);
    existing->data
      = std::move(existing->data).map_alternative<Record>([&](auto records) {
          return child.apply(std::move(records.data), active);
        });
    record = std::move(record).with_field_overwrite(name, std::move(*existing));
  }
  return record;
}

auto records_or_empty(MaskedArray<Array<Data>> value, storage::Index length,
                      location rhs, diagnostic_handler& dh) -> Array<Record> {
  if (auto record = value.data.try_as<Record>()) {
    return std::move(*record);
  }
  auto result = Array<Record>::make_empty(length);
  if (auto* u = try_as<UnionArray>(value.data)) {
    auto alt = u->get_alternative<Record>();
    if (alt) {
      // The alternative spans every row, but only the rows in `present` are
      // actually records; the others must become empty records here.
      result = std::move(alt->data).empty_where(alt->present.make_inverted());
    }
    if (value.present.and_not(u->alternative_mask<Record>())
          .and_not(u->alternative_mask<Null>())
          .any()) {
      diagnostic::warning("expected `record`").primary(rhs).emit(dh);
    }
    return result;
  }
  if (not value.data.try_as<Null>()) {
    diagnostic::warning("expected `record`").primary(rhs).emit(dh);
  }
  return result;
}

} // namespace tenzir::nova
