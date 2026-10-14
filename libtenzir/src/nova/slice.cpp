//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/slice.hpp"

#include "tenzir/detail/narrow.hpp"
#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/list_array.hpp"
#include "tenzir/nova/union_array.hpp"
#include "tenzir/ref.hpp"

#include <bit>
#include <concepts>
#include <type_traits>
#include <utility>

namespace tenzir::nova {
namespace {

// Selection determines the output row layout; presence controls which source
// values may be read and is a subset of selection. Absent fields still occupy
// their selected output rows.
auto gather(Array<Data> const& array, storage::BitMap const& rows,
            storage::BitMap const& present) -> Array<Data>;
auto gather(Array<Record> const& array, storage::BitMap const& rows,
            storage::BitMap const& present) -> Array<Record>;
auto gather(Array<List> const& array, storage::BitMap const& rows,
            storage::BitMap const& present) -> Array<List>;

template <fundamental_type Tag>
auto gather(Array<Tag> const& array, storage::BitMap const& rows,
            storage::BitMap const& present) -> Array<Tag> {
  auto length = rows.true_count();
  return match(array.storage(), [&](auto const& source) -> Array<Tag> {
    using Storage = std::remove_cvref_t<decltype(source)>;
    if constexpr (std::same_as<Tag, Null>) {
      return Array<Null>{storage::NullStorage{length}};
    } else if constexpr (requires { source.value(); }) {
      // Copy the constant once, keeping constant columns constant. Do not
      // retain its payload when none of the selected rows has this value.
      if (not present.any()) {
        return Array<Tag>{Storage{length, {}}};
      }
      return Array<Tag>{Storage{length, source.value()}};
    } else {
      // Preserve physical widths and copy only present values in this column.
      auto output = typename Storage::Mutable{length};
      auto index = storage::Index{0};
      for (auto row : storage::true_bits(rows)) {
        if (present.get(row)) {
          auto fits = output.set(index, source.get(row));
          TENZIR_ASSERT(fits);
        }
        ++index;
      }
      if constexpr (requires { std::move(output).finish(true); }) {
        return Array<Tag>{std::move(output).finish(true)};
      } else {
        return Array<Tag>{std::move(output).finish()};
      }
    }
  });
}

auto gather(storage::BitMap const& mask, storage::BitMap const& rows)
  -> storage::BitMap {
  auto builder = storage::BitMap::Builder{};
  for (auto row : storage::true_bits(rows)) {
    builder.emplace_back(mask.get(row));
  }
  return builder.finish();
}

auto gather(Array<Record> const& array, storage::BitMap const& rows,
            storage::BitMap const& present) -> Array<Record> {
  if (not present.any()) {
    auto builder = ArrayBuilder<Record>{};
    builder.skip_n(rows.true_count());
    return builder.finish();
  }
  using Constant = storage::ConstantStorage<Record, RowView<Record>>;
  if (is<Constant>(array.storage())) {
    return Array<Record>{
      Constant{rows.true_count(), as<Constant>(array.storage()).value()}};
  }
  auto const& source = *as<storage::RecordStorage>(array.storage());
  auto indices
    = storage::RecordStorage::IndicesStorage::Mutable{rows.true_count()};
  auto index = storage::Index{0};
  for (auto row : storage::true_bits(rows)) {
    indices.set(index++, present.get(row) ? source.shape_indices.get(row) : -1);
  }
  auto columns = storage::RecordStorage::MaskedArrays{};
  for (auto const& field : source.arrays) {
    auto visible = present & field.present;
    columns.push_back(
      {gather(field.data, rows, visible), gather(visible, rows)});
  }
  return Array<Record>{std::move(indices).finish(),
                       ShapeTable{source.shape_table}, source.names,
                       std::move(columns)};
}

// Cache one prefix count per bitmap word to rebase arbitrary list spans
// without allocating a source index for every selected child. Overlapping
// spans keep sharing their children in the compacted array.
class MaskRanks {
public:
  explicit MaskRanks(storage::BitMap const& mask) : mask_{mask} {
    if (mask.data().empty()) {
      return;
    }
    auto counts = storage::DataOwner<storage::Index[]>::Builder{};
    counts.reserve_exact(
      detail::narrow<storage::Index>(mask.data().size() + 1));
    auto count = storage::Index{0};
    counts.emplace_back(count);
    for (auto word : mask.data()) {
      count += static_cast<storage::Index>(std::popcount(word));
      counts.emplace_back(count);
    }
    counts_ = counts.finish();
  }

  auto operator()(storage::Index row) const -> storage::Index {
    TENZIR_ASSERT_LEQ(0, row);
    TENZIR_ASSERT_LEQ(row, mask_->length());
    auto words = mask_->data();
    if (words.empty()) {
      return mask_->any() ? row : 0;
    }
    auto word = row >> storage::BitMap::word_shift;
    auto count = counts_[word];
    auto bit = row & storage::BitMap::bit_mask;
    if (bit != 0) {
      auto preceding = (storage::BitMap::Word{1} << bit) - 1;
      count
        += static_cast<storage::Index>(std::popcount(words[word] & preceding));
    }
    return count;
  }

private:
  Ref<storage::BitMap const> mask_;
  storage::DataOwner<storage::Index[]> counts_;
};

auto gather(Array<List> const& array, storage::BitMap const& rows,
            storage::BitMap const& present) -> Array<List> {
  if (not present.any()) {
    auto builder = ArrayBuilder<List>{};
    builder.skip_n(rows.true_count());
    return builder.finish();
  }
  using Constant = storage::ConstantStorage<List, RowView<List>>;
  if (is<Constant>(array.storage())) {
    return Array<List>{
      Constant{rows.true_count(), as<Constant>(array.storage()).value()}};
  }
  auto const& source = as<storage::ListStorage>(array.storage());
  auto children = storage::BitMap::Mutable{source.values().length()};
  for (auto row : storage::true_bits(present)) {
    auto span = source.spans()[row];
    for (auto child = span.begin; child < span.end; ++child) {
      children.set(child, true);
    }
  }
  auto child_rows = std::move(children).finish();
  auto ranks = MaskRanks{child_rows};
  auto spans = storage::DataOwner<storage::Span[]>::Builder{};
  spans.reserve_exact(rows.true_count());
  for (auto row : storage::true_bits(rows)) {
    auto span = present.get(row) ? source.spans()[row] : storage::Span{0, 0};
    spans.emplace_back(storage::Span{ranks(span.begin), ranks(span.end)});
  }
  return Array<List>{spans.finish(),
                     gather(source.values(), child_rows, child_rows)};
}

auto gather(Array<Data> const& array, storage::BitMap const& rows,
            storage::BitMap const& present) -> Array<Data> {
  return match(
    array,
    [&]<data_type Tag>(Array<Tag> const& concrete) -> Array<Data> {
      return Array<Data>{gather(concrete, rows, present)};
    },
    [&](UnionArray const& source) -> Array<Data> {
      auto indices
        = storage::SparseStorage<storage::Index>::Mutable{rows.true_count()};
      auto index = storage::Index{0};
      for (auto row : storage::true_bits(rows)) {
        indices.set(index++,
                    present.get(row) ? source.alternative_index_at(row) : -1);
      }
      auto alternatives = storage::Vector<UnionArray::MaskedArray>{};
      for (auto const& alternative : source.fields()) {
        auto visible = present & alternative.present;
        auto data = match(
          alternative.data,
          [&]<data_type Tag>(Array<Tag> const& concrete) -> ErasedArray {
            return ErasedArray{gather(concrete, rows, visible)};
          });
        alternatives.push_back({std::move(data), gather(visible, rows)});
      }
      return Array<Data>{
        UnionArray{std::move(indices).finish(), std::move(alternatives)}};
    });
}

} // namespace

auto mask_slice(Events events, storage::Index begin, storage::Index end)
  -> Events {
  TENZIR_ASSERT_LEQ(0, begin);
  TENZIR_ASSERT_LEQ(begin, end);
  TENZIR_ASSERT_LEQ(end, events.length());
  auto range = storage::BitMap::Builder{};
  range.append_n(false, begin);
  range.append_n(true, end - begin);
  range.append_n(false, events.length() - end);
  events.mask = std::move(events.mask) & range.finish();
  return events;
}

auto physically_reduce_to_mask(Events events) -> Events {
  if (events.active_count() == events.length()) {
    return events;
  }
  return Events{gather(events.data, events.mask, events.mask),
                storage::BitMap{events.active_count(), true},
                {gather(events.meta.name, events.mask, events.mask),
                 gather(events.meta.import_time, events.mask, events.mask),
                 gather(events.meta.internal, events.mask, events.mask)}};
}

auto physically_slice(Events events, storage::Index begin, storage::Index end)
  -> Events {
  return physically_reduce_to_mask(mask_slice(std::move(events), begin, end));
}

} // namespace tenzir::nova
