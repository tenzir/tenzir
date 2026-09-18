#include "tenzir/nova/list_array.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/nova/data_array_builder.hpp"
#include "tenzir/nova/shared_owner.hpp"
#include "tenzir/nova/storage.hpp"
#include "tenzir/nova/union_array.hpp"

#include <limits>
#include <utility>

namespace tenzir::nova {

namespace storage {

struct ListStorage::Storage {
  SharedOwner<Span[]> spans;
  Array<Data> values;
};

ListStorage::ListStorage(SharedOwner<Span[]> spans, Array<Data> values)
  : storage_{
      std::make_shared<Storage>(Storage{std::move(spans), std::move(values)})} {
}

ListStorage::~ListStorage() = default;
ListStorage::ListStorage(ListStorage const&) = default;
ListStorage::ListStorage(ListStorage&&) noexcept = default;
auto ListStorage::operator=(ListStorage&&) noexcept -> ListStorage& = default;
auto ListStorage::operator=(ListStorage const&) -> ListStorage& = default;

auto ListStorage::as_unique() const& -> ListStorage {
  return ListStorage{storage_->spans, storage_->values};
}

auto ListStorage::as_unique() && -> ListStorage {
  if (storage_.unique()) {
    return std::move(*this);
  }
  return static_cast<ListStorage const&>(*this).as_unique();
}

auto ListStorage::data() const& -> Storage const& {
  return *storage_;
}

auto ListStorage::data() && -> Storage&& {
  *this = std::move(*this).as_unique();
  return std::move(*storage_);
}

auto ListStorage::length() const noexcept -> Index {
  return storage_->spans.length();
}

auto ListStorage::get(Index i) const -> ViewType {
  TENZIR_ASSERT_LEQ_EXPENSIVE(0, i);
  TENZIR_ASSERT_LT_EXPENSIVE(i, length());
  return RowView<List>{*storage_, i};
}

auto ListStorage::values() const& -> Array<Data> const& {
  return storage_->values;
}

auto ListStorage::values() && -> Array<Data>&& {
  return std::move(std::move(*this).data().values);
}

auto ListStorage::spans() const& -> SharedOwner<Span[]> const& {
  return storage_->spans;
}

auto ListStorage::spans() && -> SharedOwner<Span[]>&& {
  return std::move(std::move(*this).data().spans);
}

static_assert(storage<ListStorage>);
static_assert(storage<ConstantStorage<List, RowView<List>>>);
} // namespace storage

Array<List>::Array(storage::SharedOwner<storage::Span[]> spans,
                   Array<Data> values)
  : Array{storage::ListStorage{std::move(spans), std::move(values)}} {
}

Array<List>::Array(storage::ListStorage storage)
  : storage_{std::move(storage)} {
}

Array<List>::Array(storage::ConstantStorage<List, RowView<List>> storage)
  : storage_{std::move(storage)} {
}

Array<List>::~Array() = default;
Array<List>::Array(const Array&) = default;
Array<List>::Array(Array&&) noexcept = default;
auto Array<List>::operator=(const Array&) -> Array& = default;
auto Array<List>::operator=(Array&&) noexcept -> Array& = default;
auto Array<List>::storage() const& -> PhysicalStorage const& {
  return storage_;
}

auto Array<List>::storage() && -> PhysicalStorage&& {
  return std::move(storage_);
}

auto Array<List>::as_unique() const& -> Array {
  return match(storage(), [](auto const& physical) -> Array {
    return Array{physical.as_unique()};
  });
}

auto Array<List>::as_unique() && -> Array {
  return match(std::move(storage_), [](auto&& physical) -> Array {
    return Array{std::move(physical).as_unique()};
  });
}

auto Array<List>::length() const noexcept -> storage::Index {
  return match(storage(), [](auto const& physical) {
    return physical.length();
  });
}

auto Array<List>::get(storage::Index i) const -> RowView<List> {
  return match(storage(), [i](auto const& physical) {
    return physical.get(i);
  });
}

auto Array<List>::to_primary() const -> Array {
  if (is<storage::ListStorage>(storage())) {
    return *this;
  }
  auto builder = ArrayBuilder<List>{};
  if (length() > 0) {
    TENZIR_ASSERT_LEQ(get(0).length(),
                      std::numeric_limits<storage::Index>::max() / length());
  }
  for (auto i = storage::Index{0}; i < length(); ++i) {
    auto row = builder.list();
    for (auto value : get(i)) {
      append_row(row, value);
    }
  }
  return builder.finish();
}

RowView<List>::RowView(List const& value) : representation_{Constant{value}} {
  TENZIR_ASSERT_LEQ(value.size(), std::numeric_limits<storage::Index>::max());
}

RowView<List>::RowView(storage::ListStorage::Storage const& storage,
                       storage::Index row)
  : representation_{Primary{storage, row}} {
}

auto RowView<List>::get(storage::Index j) const -> RowView<Data> {
  TENZIR_ASSERT_LEQ_EXPENSIVE(0, j);
  TENZIR_ASSERT_LT_EXPENSIVE(j, length());
  return match(
    representation_,
    [j](Primary const& primary) -> RowView<Data> {
      auto const& span = primary.storage->spans[primary.row];
      TENZIR_ASSERT_LT_EXPENSIVE(j, span.end - span.begin);
      return primary.storage->values.get(span.begin + j);
    },
    [j](Constant const& constant) -> RowView<Data> {
      return RowView<Data>{constant.value.get()[j]};
    });
}

auto RowView<List>::length() const noexcept -> storage::Index {
  return match(
    representation_,
    [](Primary const& primary) -> storage::Index {
      auto const& span = primary.storage->spans[primary.row];
      return span.end - span.begin;
    },
    [](Constant const& constant) -> storage::Index {
      return static_cast<storage::Index>(constant.value->size());
    });
}

RowView<List>::iterator::iterator(Representation representation,
                                  storage::Index index)
  : representation_{std::move(representation)}, index_{index} {
}

auto RowView<List>::iterator::operator*() const -> value_type {
  return match(
    representation_,
    [this](Primary const& primary) -> value_type {
      return RowView<List>{primary.storage.get(), primary.row}.get(index_);
    },
    [this](Constant const& constant) -> value_type {
      return RowView<Data>{constant.value.get()[index_]};
    });
}

auto RowView<List>::iterator::operator++() -> iterator& {
  ++index_;
  return *this;
}

auto operator==(const RowView<List>::iterator&, const RowView<List>::iterator&)
  -> bool
  = default;

auto RowView<List>::begin() const -> iterator {
  return {representation_, 0};
}

auto RowView<List>::end() const -> iterator {
  return {representation_, length()};
}

} // namespace tenzir::nova
