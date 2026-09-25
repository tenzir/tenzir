#include "tenzir/nova/data_array_builder.hpp"

#include "tenzir/detail/assert.hpp"

#include <algorithm>
#include <string_view>

namespace tenzir::nova {

auto UnionArrayBuilder::null() -> void {
  switch_builder<Null>().null();
}

auto UnionArrayBuilder::record() -> ArrayBuilder<Record>::RecordBuilder {
  return switch_builder<Record>().record();
}

auto UnionArrayBuilder::list() -> ArrayBuilder<List>::ListBuilder {
  return switch_builder<List>().list();
}

auto UnionArrayBuilder::skip() -> void {
  // The alternatives catch up lazily, see `switch_builder`.
  alternative_index_builder_.emplace_back(-1);
}

auto UnionArrayBuilder::skip_n(storage::Index count) -> void {
  alternative_index_builder_.append_n(count, -1);
}

auto UnionArrayBuilder::length() const -> storage::Index {
  return alternative_index_builder_.size();
}

auto UnionArrayBuilder::take_last() -> Data {
  TENZIR_ASSERT_GT(alternative_index_builder_.size(), 0);
  const auto alternative = alternative_index_builder_.back();
  TENZIR_ASSERT_LEQ(0, alternative);
  alternative_index_builder_.pop_back();
  return match(builders_[static_cast<std::size_t>(alternative)],
               [](auto& builder) {
                 return builder.take_last();
               });
}

auto UnionArrayBuilder::finish() -> UnionArray {
  auto fields = storage::Vector<UnionArray::MaskedArray>{};
  fields.reserve(builders_.size());
  for (auto& builder : builders_) {
    match(builder, [&](auto& value) {
      catch_up(value);
      auto field = value.finish();
      fields.push_back(UnionArray::MaskedArray{
        .data = ErasedArray{std::move(field.data)},
        .present = std::move(field.present),
      });
    });
  }
  return UnionArray{
    storage::SparseStorage<storage::Index>{alternative_index_builder_.finish()},
    std::move(fields)};
}

auto ArrayBuilder<Data>::finish() -> Array<Data> {
  return Array<Data>{UnionArrayBuilder::finish()};
}

template <typename Builder>
auto append_row_impl(Builder& builder, const RowView<Data>& row) -> void {
  match(row, [&]<typename V>(RowView<V> view) {
    if constexpr (std::same_as<V, Null>) {
      builder.null();
    } else if constexpr (std::same_as<V, Record>) {
      auto record = builder.record();
      for (auto [name, field] : view) {
        append_row(record.field(name), field);
      }
    } else if constexpr (std::same_as<V, List>) {
      auto list = builder.list();
      for (auto element : view) {
        append_row(list, element);
      }
    } else {
      builder.data(*view);
    }
  });
}

auto append_row(ArrayBuilder<Data>& builder, const RowView<Data>& row) -> void {
  append_row_impl(builder, row);
}

auto to_data(const RowView<Data>& row) -> Data {
  auto builder = ArrayBuilder<Data>{};
  append_row(builder, row);
  return builder.take_last();
}

auto append_row(ArrayBuilder<List>::ListBuilder& builder,
                const RowView<Data>& row) -> void {
  append_row_impl(builder, row);
}

auto append_row(FieldBuilder builder, const RowView<Data>& row) -> void {
  append_row_impl(builder, row);
}

template <typename Builder>
auto append_data_impl(Builder& builder, const Data& value) -> void {
  match(
    value,
    [&](Null) {
      builder.null();
    },
    [&](const Record& r) {
      auto record = builder.record();
      for (const auto& [name, field] : r) {
        append_data(record.field(name), field);
      }
    },
    [&](const List& l) {
      auto list = builder.list();
      for (const auto& element : l) {
        append_data(list, element);
      }
    },
    [&](const String& x) {
      builder.data(std::string_view{x});
    },
    [&](const Blob& x) {
      builder.data(BlobView{x});
    },
    [&]<class T>(const T& x)
      requires fundamental_view_type<T>
    {
      builder.data(x);
    });
}

auto append_data(ArrayBuilder<Data>& builder, const Data& value) -> void {
  append_data_impl(builder, value);
}

auto append_data(ArrayBuilder<List>::ListBuilder& builder, const Data& value)
  -> void {
  append_data_impl(builder, value);
}

auto append_data(FieldBuilder builder, const Data& value) -> void {
  append_data_impl(builder, value);
}

namespace {

/// Re-appends the fields of `previous` into a fresh record row of `builder`.
auto reopen_record(ArrayBuilder<Data>& builder, const Record& previous)
  -> ArrayBuilder<Record>::RecordBuilder {
  auto record = builder.record();
  for (const auto& [name, field] : previous) {
    append_data(record.field(name), field);
  }
  return record;
}

/// Re-appends the elements of `previous` into a fresh list row of `builder`.
auto reopen_list(ArrayBuilder<Data>& builder, const List& previous)
  -> ArrayBuilder<List>::ListBuilder {
  auto list = builder.list();
  for (const auto& element : previous) {
    append_data(list, element);
  }
  return list;
}

} // namespace

template <fundamental_view_type V>
auto FieldBuilder::data(V v) -> void {
  if (not repeated_) {
    slot_->value().data(v);
    return;
  }
  const auto previous = slot_->take_last();
  auto& builder = slot_->value();
  match(
    previous,
    [&](Null) {
      builder.data(v);
    },
    [&](const Record& r) {
      reopen_record(builder, r).field("").data(v);
    },
    [&](const List& l) {
      reopen_list(builder, l).data(v);
    },
    [&](const auto&) {
      auto list = builder.list();
      append_data(list, previous);
      list.data(v);
    });
}

auto FieldBuilder::null() -> void {
  if (not repeated_) {
    slot_->value().null();
    return;
  }
  const auto previous = slot_->take_last();
  auto& builder = slot_->value();
  match(
    previous,
    [&](Null) {
      builder.null();
    },
    [&](const List& l) {
      reopen_list(builder, l).null();
    },
    [&](const auto&) {
      auto list = builder.list();
      append_data(list, previous);
      list.null();
    });
}

auto FieldBuilder::record() -> ArrayBuilder<Record>::RecordBuilder {
  if (not repeated_) {
    return slot_->value().record();
  }
  const auto previous = slot_->take_last();
  auto& builder = slot_->value();
  return match(
    previous,
    [&](Null) {
      return builder.record();
    },
    [&](const Record& r) {
      return reopen_record(builder, r);
    },
    [&](const List& l) {
      return reopen_list(builder, l).record();
    },
    [&](const auto&) {
      auto record = builder.record();
      append_data(record.field(""), previous);
      return record;
    });
}

auto FieldBuilder::list() -> ArrayBuilder<List>::ListBuilder {
  if (not repeated_) {
    return slot_->value().list();
  }
  const auto previous = slot_->take_last();
  auto& builder = slot_->value();
  return match(
    previous,
    [&](Null) {
      return builder.list();
    },
    [&](const List& l) {
      return reopen_list(builder, l);
    },
    [&](const auto&) {
      auto list = builder.list();
      append_data(list, previous);
      return list.list();
    });
}

template auto FieldBuilder::data(bool) -> void;
template auto FieldBuilder::data(int64_t) -> void;
template auto FieldBuilder::data(uint64_t) -> void;
template auto FieldBuilder::data(double) -> void;
template auto FieldBuilder::data<std::string_view>(std::string_view) -> void;
template auto FieldBuilder::data(blob_view) -> void;
template auto FieldBuilder::data(ip) -> void;
template auto FieldBuilder::data(subnet) -> void;
template auto FieldBuilder::data<time>(time) -> void;
template auto FieldBuilder::data(duration) -> void;

} // namespace tenzir::nova
