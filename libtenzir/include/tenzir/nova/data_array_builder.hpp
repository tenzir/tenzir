//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/nova/list_array_builder.hpp"
#include "tenzir/nova/masked_array_builder.hpp"
#include "tenzir/nova/union_array.hpp"

#include <array>

namespace tenzir::nova {

class UnionArrayBuilder {
public:
  auto null() -> void;

  template <fundamental_view_type V>
  auto data(V v) -> void {
    switch_builder<TagForViewType<V>>().data(v);
  }

  auto data(Time v) -> void {
    data<Time>(v);
  }

  auto data(std::string_view v) -> void {
    data<std::string_view>(v);
  }

  auto record() -> ArrayBuilder<Record>::RecordBuilder;
  auto list() -> ArrayBuilder<List>::ListBuilder;
  auto skip() -> void;
  auto skip_n(storage::Index count) -> void;
  auto length() const -> storage::Index;
  /// Removes the last row, which must hold a value, and returns it.
  auto take_last() -> Data;
  auto finish() -> UnionArray;

private:
  using BuilderVariant = data_type_list::wrap<ArrayBuilder>::wrap<
    MaskedArrayBuilder>::apply<variant>;

  /// Alternatives are padded lazily: an alternative's builder only catches up
  /// to `length()` right before it receives a value, and all of them catch up
  /// in `finish()`. Rows that carry no value for an alternative therefore cost
  /// nothing until the next value of that alternative, at which point they are
  /// appended in bulk.
  template <data_type T>
  auto switch_builder() -> ArrayBuilder<T>& {
    constexpr auto type_index = data_type_list::unique_index_of<T>;
    auto& vector_index = type_to_vector_index_[type_index];
    if (vector_index < 0) {
      vector_index = static_cast<storage::Index>(builders_.size());
      builders_.emplace_back(
        std::in_place_type<MaskedArrayBuilder<ArrayBuilder<T>>>);
    }
    auto& builder = std::get<MaskedArrayBuilder<ArrayBuilder<T>>>(
      builders_[static_cast<std::size_t>(vector_index)]);
    catch_up(builder);
    alternative_index_builder_.emplace_back(vector_index);
    return builder.value();
  }

  /// Pads `builder` with absent rows until it is `length()` rows long.
  template <typename Builder>
  auto catch_up(Builder& builder) -> void {
    if (const auto missing = length() - builder.size(); missing > 0) {
      builder.skip_n(missing);
    }
  }

  storage::Vector<BuilderVariant> builders_;
  storage::DataOwner<storage::Index[]>::Builder alternative_index_builder_;
  std::array<storage::Index, data_type_list::size> type_to_vector_index_ = [] {
    auto result = std::array<storage::Index, data_type_list::size>{};
    result.fill(-1);
    return result;
  }();
};

template <>
class ArrayBuilder<Data> : private UnionArrayBuilder {
public:
  using UnionArrayBuilder::data;
  using UnionArrayBuilder::length;
  using UnionArrayBuilder::list;
  using UnionArrayBuilder::null;
  using UnionArrayBuilder::record;
  using UnionArrayBuilder::skip;
  using UnionArrayBuilder::skip_n;
  using UnionArrayBuilder::take_last;

  auto finish() -> Array<Data>;
};

/// The slot of one field in the open row of an `ArrayBuilder<Record>`. When
/// the row already holds the field, the incoming value is merged into the
/// existing one: values join into a list, lists concatenate, records merge
/// field by field, and a scalar meeting a record lives under the key `""`.
/// An existing `null` is simply replaced.
class FieldBuilder {
public:
  template <fundamental_view_type V>
  auto data(V v) -> void;

  auto data(Time v) -> void {
    data<Time>(v);
  }

  auto data(std::string_view v) -> void {
    data<std::string_view>(v);
  }

  auto null() -> void;
  auto record() -> ArrayBuilder<Record>::RecordBuilder;
  auto list() -> ArrayBuilder<List>::ListBuilder;

private:
  friend class ArrayBuilder<Record>;
  using Slot = MaskedArrayBuilder<ArrayBuilder<Data>>;

  FieldBuilder(Slot* slot, bool repeated) : slot_{slot}, repeated_{repeated} {
  }

  Slot* slot_ = nullptr;
  bool repeated_ = false;
};

extern template auto FieldBuilder::data(bool) -> void;
extern template auto FieldBuilder::data(int64_t) -> void;
extern template auto FieldBuilder::data(uint64_t) -> void;
extern template auto FieldBuilder::data(double) -> void;
extern template auto FieldBuilder::data<std::string_view>(std::string_view)
  -> void;
extern template auto FieldBuilder::data(blob_view) -> void;
extern template auto FieldBuilder::data(ip) -> void;
extern template auto FieldBuilder::data(subnet) -> void;
extern template auto FieldBuilder::data<time>(time) -> void;
extern template auto FieldBuilder::data(duration) -> void;

auto append_row(ArrayBuilder<Data>& builder, const RowView<Data>& row) -> void;
auto append_row(ArrayBuilder<List>::ListBuilder& builder,
                const RowView<Data>& row) -> void;
auto append_row(FieldBuilder builder, const RowView<Data>& row) -> void;

/// Appends `value`, recursing into records and lists.
auto append_data(ArrayBuilder<Data>& builder, const Data& value) -> void;
auto append_data(ArrayBuilder<List>::ListBuilder& builder, const Data& value)
  -> void;
auto append_data(FieldBuilder builder, const Data& value) -> void;

/// Materializes a row into an owning value, recursing into records and lists.
auto to_data(const RowView<Data>& row) -> Data;

/// Appends a legacy `tenzir::data` value, recursing into records and lists.
/// Alternatives without a counterpart here (`pattern`, `enumeration`,
/// `map`, `secret`) warn through `dh` and append a null.
auto append_legacy_data(ArrayBuilder<Data>& builder, const data& value,
                        diagnostic_handler& dh) -> void;
auto append_legacy_data(ArrayBuilder<List>::ListBuilder& builder,
                        const data& value, diagnostic_handler& dh) -> void;
auto append_legacy_data(FieldBuilder builder, const data& value,
                        diagnostic_handler& dh) -> void;

} // namespace tenzir::nova
