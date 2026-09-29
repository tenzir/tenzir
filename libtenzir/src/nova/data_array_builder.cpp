#include "tenzir/nova/data_array_builder.hpp"

#include "tenzir/data.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/diagnostics.hpp"

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

auto UnionArrayBuilder::open_record()
  -> Option<ArrayBuilder<Record>::RecordBuilder> {
  constexpr auto type_index = data_type_list::unique_index_of<Record>;
  const auto vector_index = type_to_vector_index_[type_index];
  if (vector_index < 0 or alternative_index_builder_.size() == 0
      or alternative_index_builder_.back() != vector_index) {
    return None{};
  }
  auto& builder = std::get<MaskedArrayBuilder<ArrayBuilder<Record>>>(
    builders_[static_cast<std::size_t>(vector_index)]);
  return builder.last_value().reopen();
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
    [&](const Secret& x) {
      builder.data(SecretView{x});
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

template <fundamental_view_type V>
auto FieldBuilder::data(V v) -> void {
  slot_->value().data(v);
}

auto FieldBuilder::null() -> void {
  slot_->value().null();
}

auto FieldBuilder::record() -> ArrayBuilder<Record>::RecordBuilder {
  return slot_->value().record();
}

auto FieldBuilder::list() -> ArrayBuilder<List>::ListBuilder {
  return slot_->value().list();
}

template auto FieldBuilder::data(bool) -> void;
template auto FieldBuilder::data(int64_t) -> void;
template auto FieldBuilder::data(uint64_t) -> void;
template auto FieldBuilder::data(double) -> void;
template auto FieldBuilder::data<std::string_view>(std::string_view) -> void;
template auto FieldBuilder::data(blob_view) -> void;
template auto FieldBuilder::data(SecretView) -> void;
template auto FieldBuilder::data(ip) -> void;
template auto FieldBuilder::data(subnet) -> void;
template auto FieldBuilder::data<time>(time) -> void;
template auto FieldBuilder::data(duration) -> void;

namespace {

/// The shared body behind the `append_legacy_data` overloads. `Builder` is an
/// `ArrayBuilder<Data>`, an `ArrayBuilder<List>::ListBuilder` or a
/// `FieldBuilder`; the recursion goes back through the overloads so that each
/// nesting level picks the right one.
template <class Builder>
auto append_legacy_data_into(Builder& builder, const data& value,
                             diagnostic_handler& dh) -> void {
  match(
    value,
    [&](caf::none_t) {
      builder.null();
    },
    [&](const record& x) {
      auto row = builder.record();
      for (const auto& [name, field] : x) {
        append_legacy_data(row.field(name), field, dh);
      }
    },
    [&](const list& x) {
      auto elements = builder.list();
      for (const auto& element : x) {
        append_legacy_data(elements, element, dh);
      }
    },
    [&](const std::string& x) {
      builder.data(std::string_view{x});
    },
    [&](const blob& x) {
      builder.data(blob_view{x});
    },
    [&](bool x) {
      builder.data(x);
    },
    [&](int64_t x) {
      builder.data(x);
    },
    [&](uint64_t x) {
      builder.data(x);
    },
    [&](double x) {
      builder.data(x);
    },
    [&](duration x) {
      builder.data(x);
    },
    [&](time x) {
      builder.data(x);
    },
    [&](ip x) {
      builder.data(x);
    },
    [&](subnet x) {
      builder.data(x);
    },
    [&](const auto&) {
      diagnostic::warning("this value type is not supported yet").emit(dh);
      builder.null();
    });
}

} // namespace

auto append_legacy_data(ArrayBuilder<Data>& builder, const data& value,
                        diagnostic_handler& dh) -> void {
  append_legacy_data_into(builder, value, dh);
}

auto append_legacy_data(ArrayBuilder<List>::ListBuilder& builder,
                        const data& value, diagnostic_handler& dh) -> void {
  append_legacy_data_into(builder, value, dh);
}

auto append_legacy_data(FieldBuilder builder, const data& value,
                        diagnostic_handler& dh) -> void {
  append_legacy_data_into(builder, value, dh);
}

} // namespace tenzir::nova
