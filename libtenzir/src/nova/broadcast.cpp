//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/broadcast.hpp"

#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/shape_table.hpp"
#include "tenzir/nova/storage.hpp"
#include "tenzir/variant_traits.hpp"

#include <algorithm>
#include <limits>
#include <utility>

namespace tenzir::nova {

namespace {

using Spans = std::span<storage::Span const>;

// -- broadcasting -------------------------------------------------------------

/// Fills the span of every row that `rows` selects with `value(row)` and
/// everything else with `placeholder`.
template <class T, class F>
auto expand(Spans spans, storage::BitMap const& rows, storage::Index length,
            T const& placeholder, F value) -> storage::DataOwner<T[]> {
  TENZIR_ASSERT_EQ(spans.size(), static_cast<std::size_t>(rows.length()));
  auto result = storage::DataOwner<T[]>::make_value(length, placeholder);
  storage::for_each_true(rows, [&](storage::Index row) {
    auto const [begin, end] = spans[row];
    TENZIR_ASSERT_LEQ(0, begin);
    TENZIR_ASSERT_LEQ(begin, end);
    TENZIR_ASSERT_LEQ(end, length);
    std::fill(result.begin() + begin, result.begin() + end, value(row));
  });
  return result;
}

/// Like `expand`, for bits. Elements outside the selected spans are unset.
auto expand_bits(storage::BitMap const& bits, Spans spans,
                 storage::BitMap const& rows, storage::Index length)
  -> storage::BitMap {
  TENZIR_ASSERT_EQ(spans.size(), static_cast<std::size_t>(rows.length()));
  TENZIR_ASSERT_EQ(bits.length(), rows.length());
  if (bits.as_constant() == false) {
    return storage::BitMap{length, false};
  }
  auto result = storage::BitMap::Mutable{length};
  storage::for_each_true(rows, [&](storage::Index row) {
    if (bits.get(row)) {
      auto const [begin, end] = spans[row];
      result.set_range(begin, end, true);
    }
  });
  return std::move(result).finish();
}

auto broadcast_storage(storage::NullStorage const&, Spans,
                       storage::BitMap const&, storage::Index length)
  -> storage::NullStorage {
  return storage::NullStorage{length};
}

auto broadcast_storage(storage::BitMap const& bits, Spans spans,
                       storage::BitMap const& rows, storage::Index length)
  -> storage::BitMap {
  return expand_bits(bits, spans, rows, length);
}

template <class T, class View>
auto broadcast_storage(storage::ConstantStorage<T, View> const& constant, Spans,
                       storage::BitMap const&, storage::Index length)
  -> storage::ConstantStorage<T, View> {
  return constant.with_length(length);
}

template <class T>
auto broadcast_storage(storage::SparseStorage<T> const& sparse, Spans spans,
                       storage::BitMap const& rows, storage::Index length)
  -> storage::SparseStorage<T> {
  auto const* data = sparse.data();
  return storage::SparseStorage<T>{
    expand(spans, rows, length, T{}, [&](storage::Index row) {
      return data[row];
    })};
}

template <class Char, class View>
auto broadcast_storage(storage::DenseOffsetBytesStorage<Char, View> const& bytes,
                       Spans spans, storage::BitMap const& rows,
                       storage::Index length)
  -> storage::DenseOffsetBytesStorage<Char, View> {
  auto ranges
    = expand(spans, rows, length, storage::Span{0, 0}, [&](storage::Index row) {
        return bytes.span(row);
      });
  // Without ranges, the storage takes its length from the data, so an empty
  // result must not keep the buffer.
  auto data = length == 0 ? storage::DataOwner<Char[]>{} : bytes.data();
  return storage::DenseOffsetBytesStorage<Char, View>{std::move(data),
                                                      std::move(ranges)};
}

auto broadcast_storage(storage::ListStorage const& list, Spans spans,
                       storage::BitMap const& rows, storage::Index length)
  -> storage::ListStorage {
  auto const& lists = list.spans();
  auto ranges
    = expand(spans, rows, length, storage::Span{0, 0}, [&](storage::Index row) {
        return lists[row];
      });
  return storage::ListStorage{std::move(ranges), list.values()};
}

auto broadcast_storage(storage::RecordStorage const& record, Spans spans,
                       storage::BitMap const& rows, storage::Index length)
  -> storage::RecordStorage {
  auto const& source = *record;
  auto const* shapes = source.shape_indices.data();
  // A row's shape lists the fields it has, and each of them is present in the
  // row, so the fields agree with the shapes after broadcasting them as well.
  auto indices = expand(spans, rows, length, ShapeTable::empty_shape,
                        [&](storage::Index row) {
                          return shapes[row];
                        });
  auto arrays = storage::RecordStorage::MaskedArrays{};
  arrays.reserve(source.arrays.size());
  for (auto const& field : source.arrays) {
    arrays.push_back(broadcast(field, spans, rows, length));
  }
  return storage::RecordStorage{
    storage::RecordStorage::IndicesStorage{std::move(indices)},
    source.shape_table, source.names, std::move(arrays)};
}

auto broadcast_union(UnionArray const& array, Spans spans,
                     storage::BitMap const& rows, storage::Index length)
  -> UnionArray {
  auto indices
    = expand(spans, rows, length, storage::Index{-1}, [&](storage::Index row) {
        return array.alternative_index_at(row);
      });
  auto alternatives = storage::Vector<UnionArray::MaskedArray>{};
  alternatives.reserve(array.fields().size());
  for (auto const& alternative : array.fields()) {
    // An alternative only holds values for the rows it is present in.
    auto const valid = rows & alternative.present;
    alternatives.push_back({
      .data = match(alternative.data,
                    [&]<data_type Tag>(Array<Tag> const& data) -> ErasedArray {
                      return ErasedArray{broadcast(data, spans, valid, length)};
                    }),
      .present = broadcast_mask(spans, valid, length),
    });
  }
  return UnionArray{storage::SparseStorage<storage::Index>{std::move(indices)},
                    std::move(alternatives)};
}

auto broadcast_data(Array<Data> const& column, Spans spans,
                    storage::BitMap const& rows, storage::Index length)
  -> Array<Data> {
  return match(
    column,
    [&](UnionArray const& array) -> Array<Data> {
      return Array<Data>{broadcast_union(array, spans, rows, length)};
    },
    [&]<data_type Tag>(Array<Tag> const& array) -> Array<Data> {
      return broadcast(array, spans, rows, length);
    });
}

// -- concatenating ranges -----------------------------------------------------

auto total_length(Spans ranges) -> storage::Index {
  auto total = storage::Index{0};
  for (auto const [begin, end] : ranges) {
    TENZIR_ASSERT_LEQ(0, begin);
    TENZIR_ASSERT_LEQ(begin, end);
    TENZIR_ASSERT_LEQ(end - begin,
                      std::numeric_limits<storage::Index>::max() - total);
    total += end - begin;
  }
  return total;
}

/// Lays out `value(row)` for the rows within each of `ranges`, one range after
/// another, or `placeholder` for the rows that `valid` does not select.
template <class T, class F>
auto copy_ranges(Spans ranges, storage::BitMap const& valid,
                 storage::Index total, T const& placeholder, F value)
  -> storage::DataOwner<T[]> {
  auto builder = storage::DataOwner<T[]>::make_uninitialized(total);
  for (auto const [begin, end] : ranges) {
    TENZIR_ASSERT_LEQ(end, valid.length());
    for (auto row = begin; row < end; ++row) {
      builder.emplace_back(valid.get(row) ? value(row) : placeholder);
    }
  }
  TENZIR_ASSERT_EQ(builder.size(), total);
  return builder.finish();
}

auto copy_bits(storage::BitMap const& bits, Spans ranges, storage::Index total)
  -> storage::BitMap {
  if (auto const constant = bits.as_constant()) {
    return storage::BitMap{total, *constant};
  }
  auto builder = storage::BitMap::Builder{};
  for (auto const [begin, end] : ranges) {
    TENZIR_ASSERT_LEQ(end, bits.length());
    for (auto row = begin; row < end; ++row) {
      builder.emplace_back(bits.get(row));
    }
  }
  TENZIR_ASSERT_EQ(builder.size(), total);
  return builder.finish();
}

auto concatenate_data(Array<Data> const& column, Spans ranges,
                      storage::BitMap const& valid, storage::Index total)
  -> Array<Data>;

auto concatenate_masked(MaskedArray<Array<Data>> const& column, Spans ranges,
                        storage::BitMap const& valid, storage::Index total)
  -> MaskedArray<Array<Data>> {
  auto const present = valid & column.present;
  return {
    .data = concatenate_data(column.data, ranges, present, total),
    .present = copy_bits(present, ranges, total),
  };
}

auto concatenate_storage(storage::NullStorage const&, Spans,
                         storage::BitMap const&, storage::Index total)
  -> storage::NullStorage {
  return storage::NullStorage{total};
}

auto concatenate_storage(storage::BitMap const& bits, Spans ranges,
                         storage::BitMap const& valid, storage::Index total)
  -> storage::BitMap {
  return copy_bits(bits & valid, ranges, total);
}

template <class T, class View>
auto concatenate_storage(storage::ConstantStorage<T, View> const& constant,
                         Spans, storage::BitMap const&, storage::Index total)
  -> storage::ConstantStorage<T, View> {
  return constant.with_length(total);
}

template <class T>
auto concatenate_storage(storage::SparseStorage<T> const& sparse, Spans ranges,
                         storage::BitMap const& valid, storage::Index total)
  -> storage::SparseStorage<T> {
  auto const* data = sparse.data();
  return storage::SparseStorage<T>{
    copy_ranges(ranges, valid, total, T{}, [&](storage::Index row) {
      return data[row];
    })};
}

template <class Char, class View>
auto concatenate_storage(
  storage::DenseOffsetBytesStorage<Char, View> const& bytes, Spans ranges,
  storage::BitMap const& valid, storage::Index total)
  -> storage::DenseOffsetBytesStorage<Char, View> {
  auto spans = copy_ranges(ranges, valid, total, storage::Span{0, 0},
                           [&](storage::Index row) {
                             return bytes.span(row);
                           });
  auto data = total == 0 ? storage::DataOwner<Char[]>{} : bytes.data();
  return storage::DenseOffsetBytesStorage<Char, View>{std::move(data),
                                                      std::move(spans)};
}

auto concatenate_storage(storage::ListStorage const& list, Spans ranges,
                         storage::BitMap const& valid, storage::Index total)
  -> storage::ListStorage {
  auto const& lists = list.spans();
  auto spans = copy_ranges(ranges, valid, total, storage::Span{0, 0},
                           [&](storage::Index row) {
                             return lists[row];
                           });
  return storage::ListStorage{std::move(spans), list.values()};
}

auto concatenate_storage(storage::RecordStorage const& record, Spans ranges,
                         storage::BitMap const& valid, storage::Index total)
  -> storage::RecordStorage {
  auto const& source = *record;
  auto const* shapes = source.shape_indices.data();
  auto indices = copy_ranges(ranges, valid, total, ShapeTable::empty_shape,
                             [&](storage::Index row) {
                               return shapes[row];
                             });
  auto arrays = storage::RecordStorage::MaskedArrays{};
  arrays.reserve(source.arrays.size());
  for (auto const& field : source.arrays) {
    arrays.push_back(concatenate_masked(field, ranges, valid, total));
  }
  return storage::RecordStorage{
    storage::RecordStorage::IndicesStorage{std::move(indices)},
    source.shape_table, source.names, std::move(arrays)};
}

auto concatenate_union(UnionArray const& array, Spans ranges,
                       storage::BitMap const& valid, storage::Index total)
  -> UnionArray {
  auto indices = copy_ranges(ranges, valid, total, storage::Index{-1},
                             [&](storage::Index row) {
                               return array.alternative_index_at(row);
                             });
  auto alternatives = storage::Vector<UnionArray::MaskedArray>{};
  alternatives.reserve(array.fields().size());
  for (auto const& alternative : array.fields()) {
    // An alternative only holds values for the rows it is present in.
    auto const present = valid & alternative.present;
    alternatives.push_back({
      .data = match(alternative.data,
                    [&]<data_type Tag>(Array<Tag> const& data) -> ErasedArray {
                      return ErasedArray{match(
                        data.storage(), [&](auto const& storage) -> Array<Tag> {
                          return Array<Tag>{concatenate_storage(
                            storage, ranges, present, total)};
                        })};
                    }),
      .present = copy_bits(present, ranges, total),
    });
  }
  return UnionArray{storage::SparseStorage<storage::Index>{std::move(indices)},
                    std::move(alternatives)};
}

auto concatenate_data(Array<Data> const& column, Spans ranges,
                      storage::BitMap const& valid, storage::Index total)
  -> Array<Data> {
  return match(
    column,
    [&](UnionArray const& array) -> Array<Data> {
      return Array<Data>{concatenate_union(array, ranges, valid, total)};
    },
    [&]<data_type Tag>(Array<Tag> const& array) -> Array<Data> {
      return match(array.storage(), [&](auto const& storage) -> Array<Data> {
        return Array<Tag>{concatenate_storage(storage, ranges, valid, total)};
      });
    });
}

} // namespace

auto broadcast_mask(Spans spans, storage::BitMap const& rows,
                    storage::Index length) -> storage::BitMap {
  return expand_bits(storage::BitMap{rows.length(), true}, spans, rows, length);
}

template <data_type Tag>
auto broadcast(Array<Tag> const& column, Spans spans,
               storage::BitMap const& rows, storage::Index length)
  -> Array<Tag> {
  TENZIR_ASSERT_EQ(column.length(), rows.length());
  return match(column.storage(), [&](auto const& storage) -> Array<Tag> {
    return Array<Tag>{broadcast_storage(storage, spans, rows, length)};
  });
}

auto broadcast(MaskedArray<Array<Data>> const& column, Spans spans,
               storage::BitMap const& rows, storage::Index length)
  -> MaskedArray<Array<Data>> {
  auto const valid = rows & column.present;
  return {
    .data = broadcast_data(column.data, spans, valid, length),
    .present = broadcast_mask(spans, valid, length),
  };
}

auto concatenate_ranges(storage::BitMap const& bits, Spans ranges)
  -> storage::BitMap {
  return copy_bits(bits, ranges, total_length(ranges));
}

auto concatenate_ranges(Array<Data> const& column, Spans ranges)
  -> Array<Data> {
  return concatenate_data(column, ranges,
                          storage::BitMap{column.length(), true},
                          total_length(ranges));
}

#define TENZIR_INSTANTIATE_BROADCAST(Tag)                                      \
  template auto broadcast<Tag>(Array<Tag> const&, Spans,                       \
                               storage::BitMap const&, storage::Index)         \
    -> Array<Tag>

TENZIR_INSTANTIATE_BROADCAST(Null);
TENZIR_INSTANTIATE_BROADCAST(Bool);
TENZIR_INSTANTIATE_BROADCAST(Int);
TENZIR_INSTANTIATE_BROADCAST(UInt);
TENZIR_INSTANTIATE_BROADCAST(Float);
TENZIR_INSTANTIATE_BROADCAST(String);
TENZIR_INSTANTIATE_BROADCAST(Blob);
TENZIR_INSTANTIATE_BROADCAST(Secret);
TENZIR_INSTANTIATE_BROADCAST(Ip);
TENZIR_INSTANTIATE_BROADCAST(Subnet);
TENZIR_INSTANTIATE_BROADCAST(Time);
TENZIR_INSTANTIATE_BROADCAST(Duration);
TENZIR_INSTANTIATE_BROADCAST(List);
TENZIR_INSTANTIATE_BROADCAST(Record);

#undef TENZIR_INSTANTIATE_BROADCAST

} // namespace tenzir::nova
