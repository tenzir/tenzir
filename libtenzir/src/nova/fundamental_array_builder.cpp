#include "tenzir/nova/fundamental_array_builder.hpp"

#include "tenzir/detail/assert.hpp"

namespace tenzir::nova {

template <fundamental_type Tag>
auto ArrayBuilder<Tag>::data(View v) -> void {
  data_builder.emplace_back(v);
}

template <fundamental_type Tag>
auto ArrayBuilder<Tag>::skip() -> void {
  data_builder.emplace_back(Tag{});
}

template <fundamental_type Tag>
auto ArrayBuilder<Tag>::skip_n(storage::Index count) -> void {
  data_builder.append_n(count, Tag{});
}

template <fundamental_type Tag>
auto ArrayBuilder<Tag>::length() const -> storage::Index {
  return data_builder.size();
}

template <fundamental_type Tag>
auto ArrayBuilder<Tag>::take_last() -> Data {
  auto value = data_builder.back();
  data_builder.pop_back();
  return Data{value};
}

template <fundamental_type Tag>
auto ArrayBuilder<Tag>::finish() -> Array<Tag> {
  return Array<Tag>{
    PrimaryStorage{data_builder.finish()},
  };
}

auto ArrayBuilder<Bool>::data(bool v) -> void {
  data_builder.emplace_back(v);
}

auto ArrayBuilder<Bool>::skip() -> void {
  data_builder.emplace_back(false);
}

auto ArrayBuilder<Bool>::skip_n(storage::Index count) -> void {
  data_builder.append_n(false, count);
}

auto ArrayBuilder<Bool>::length() const -> storage::Index {
  return data_builder.size();
}

auto ArrayBuilder<Bool>::take_last() -> Data {
  return Data{data_builder.pop_back()};
}

auto ArrayBuilder<Bool>::finish() -> Array<Bool> {
  return Array<Bool>{data_builder.finish()};
}

auto ArrayBuilder<Null>::null() -> void {
  ++length_;
}

auto ArrayBuilder<Null>::skip() -> void {
  ++length_;
}

auto ArrayBuilder<Null>::skip_n(storage::Index count) -> void {
  length_ += count;
}

auto ArrayBuilder<Null>::length() const -> storage::Index {
  return length_;
}

auto ArrayBuilder<Null>::take_last() -> Data {
  TENZIR_ASSERT_GT(length_, 0);
  --length_;
  return Data{Null{}};
}

auto ArrayBuilder<Null>::finish() -> Array<Null> {
  auto result = Array<Null>{storage::NullStorage{length_}};
  length_ = 0;
  return result;
}

template <fundamental_type Tag, typename Char>
auto DenseOffsetArrayBuilder<Tag, Char>::data(View v) -> void {
  const auto begin = data_builder.size();
  data_builder.move_append(v.begin(), v.end());
  const auto end = data_builder.size();
  range_builder.emplace_back(begin, end);
}

template <fundamental_type Tag, typename Char>
auto DenseOffsetArrayBuilder<Tag, Char>::skip() -> void {
  range_builder.emplace_back(-1, -1);
}

template <fundamental_type Tag, typename Char>
auto DenseOffsetArrayBuilder<Tag, Char>::skip_n(storage::Index count) -> void {
  range_builder.append_n(count, storage::Span{-1, -1});
}

template <fundamental_type Tag, typename Char>
auto DenseOffsetArrayBuilder<Tag, Char>::length() const -> storage::Index {
  return range_builder.size();
}

template <fundamental_type Tag, typename Char>
auto DenseOffsetArrayBuilder<Tag, Char>::take_last() -> Data {
  const auto span = range_builder.back();
  TENZIR_ASSERT_LEQ(0, span.begin);
  range_builder.pop_back();
  auto result = Tag{};
  result.reserve(static_cast<std::size_t>(span.end - span.begin));
  for (auto i = span.begin; i < span.end; ++i) {
    result.push_back(data_builder[i]);
  }
  data_builder.truncate(span.begin);
  return Data{std::move(result)};
}

template <fundamental_type Tag, typename Char>
auto DenseOffsetArrayBuilder<Tag, Char>::finish() -> Array<Tag> {
  return Array<Tag>{typename Type<Tag>::PrimaryPhysicalStorage{
    data_builder.finish(), range_builder.finish()}};
}

template class DenseOffsetArrayBuilder<String, char>;
template class DenseOffsetArrayBuilder<Blob, std::byte>;

template class ArrayBuilder<Int>;
template class ArrayBuilder<UInt>;
template class ArrayBuilder<Float>;
template class ArrayBuilder<Ip>;
template class ArrayBuilder<Subnet>;
template class ArrayBuilder<Time>;
template class ArrayBuilder<Duration>;

} // namespace tenzir::nova
