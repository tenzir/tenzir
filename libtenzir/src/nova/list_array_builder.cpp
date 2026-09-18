#include "tenzir/nova/list_array_builder.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/nova/data_array_builder.hpp"

#include <algorithm>

namespace tenzir::nova {

struct ArrayBuilder<List>::Storage {
  /// Where the currently open row started in `values_builder`; only meaningful
  /// while `list_open` is set.
  storage::Index open_begin = 0;
  bool list_open = false;
  storage::SharedOwner<storage::Span[]>::Builder span_builder;
  std::unique_ptr<ArrayBuilder<Data>> values_builder;
};

ArrayBuilder<List>::ListBuilder::ListBuilder(ArrayBuilder* parent)
  : parent_{parent} {
}

ArrayBuilder<List>::ArrayBuilder() : storage_{std::make_unique<Storage>()} {
  storage_->values_builder = std::make_unique<ArrayBuilder<Data>>();
}

ArrayBuilder<List>::ArrayBuilder(ArrayBuilder&&) noexcept = default;
auto ArrayBuilder<List>::operator=(ArrayBuilder&&) noexcept
  -> ArrayBuilder& = default;
ArrayBuilder<List>::~ArrayBuilder() = default;

auto ArrayBuilder<List>::finish_last_row() -> void {
  if (not storage_->list_open) {
    return;
  }
  storage_->span_builder.emplace_back(storage_->open_begin,
                                      storage_->values_builder->length());
  storage_->list_open = false;
}

auto ArrayBuilder<List>::skip() -> void {
  finish_last_row();
  // A skipped row owns no elements, so it is the empty span at the current end.
  auto const end = storage_->values_builder->length();
  storage_->span_builder.emplace_back(end, end);
}

auto ArrayBuilder<List>::skip_n(storage::Index count) -> void {
  finish_last_row();
  auto const end = storage_->values_builder->length();
  storage_->span_builder.append_n(count, storage::Span{end, end});
}

auto ArrayBuilder<List>::list() -> ListBuilder {
  finish_last_row();
  storage_->open_begin = storage_->values_builder->length();
  storage_->list_open = true;
  return ListBuilder{this};
}

auto ArrayBuilder<List>::length() const -> storage::Index {
  return storage_->span_builder.size() + (storage_->list_open ? 1 : 0);
}

auto ArrayBuilder<List>::take_last() -> Data {
  finish_last_row();
  auto& span_builder = storage_->span_builder;
  TENZIR_ASSERT_GT(span_builder.size(), 0);
  const auto span = span_builder.back();
  span_builder.pop_back();
  TENZIR_ASSERT_EQ(span.end, storage_->values_builder->length());
  auto result = List{};
  result.reserve(static_cast<std::size_t>(span.end - span.begin));
  for (auto i = span.begin; i < span.end; ++i) {
    result.push_back(storage_->values_builder->take_last());
  }
  std::ranges::reverse(result);
  return Data{std::move(result)};
}

auto ArrayBuilder<List>::finish() -> Array<List> {
  finish_last_row();
  return {storage_->span_builder.finish(), storage_->values_builder->finish()};
}

template <fundamental_view_type V>
auto ArrayBuilder<List>::ListBuilder::data(V v) -> void {
  parent_->storage_->values_builder->data(v);
}

auto ArrayBuilder<List>::ListBuilder::null() -> void {
  parent_->storage_->values_builder->null();
}

auto ArrayBuilder<List>::ListBuilder::record()
  -> ArrayBuilder<Record>::RecordBuilder {
  return parent_->storage_->values_builder->record();
}

auto ArrayBuilder<List>::ListBuilder::list() -> ListBuilder {
  return parent_->storage_->values_builder->list();
}

template auto ArrayBuilder<List>::ListBuilder::data(bool) -> void;
template auto ArrayBuilder<List>::ListBuilder::data(int64_t) -> void;
template auto ArrayBuilder<List>::ListBuilder::data(uint64_t) -> void;
template auto ArrayBuilder<List>::ListBuilder::data(double) -> void;
template auto
  ArrayBuilder<List>::ListBuilder::data<std::string_view>(std::string_view)
    -> void;
template auto ArrayBuilder<List>::ListBuilder::data(blob_view) -> void;
template auto ArrayBuilder<List>::ListBuilder::data(ip) -> void;
template auto ArrayBuilder<List>::ListBuilder::data(subnet) -> void;
template auto ArrayBuilder<List>::ListBuilder::data<time>(time) -> void;
template auto ArrayBuilder<List>::ListBuilder::data(duration) -> void;

} // namespace tenzir::nova
