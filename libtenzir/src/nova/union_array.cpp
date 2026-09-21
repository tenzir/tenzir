#include "tenzir/nova/union_array.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"

#include <utility>

namespace tenzir::nova {

RowView<Data>::RowView(Data const& value)
  : data_{match(value, []<class T>(T const& x) -> Storage {
      return RowView<T>{x};
    })} {
}

auto repeat(Data const& value, storage::Index length) -> Array<Data> {
  TENZIR_ASSERT_LEQ(0, length);
  return match(value, [length]<class T>(T const& x) -> Array<Data> {
    if constexpr (std::same_as<T, Null>) {
      return Array<Null>{storage::NullStorage{length}};
    } else if constexpr (std::same_as<T, Bool>) {
      return Array<Bool>{storage::BitMap{length, x}};
    } else {
      return Array<T>{
        storage::ConstantStorage<T, typename Type<T>::ViewType>{length, x}};
    }
  });
}

ErasedArray::ErasedArray()
  : ErasedArray{Array<Null>{nova::storage::NullStorage{0}}} {
}

auto ErasedArray::as_unique() const& -> ErasedArray {
  return match(data_, [](auto const& x) -> ErasedArray {
    return ErasedArray{x.as_unique()};
  });
}

auto ErasedArray::as_unique() && -> ErasedArray {
  return match(data_, [](auto& x) -> ErasedArray {
    return ErasedArray{std::move(x).as_unique()};
  });
}

struct UnionArray::Storage {
  Storage(nova::storage::SparseStorage<nova::storage::Index> alternative_indices,
          nova::storage::Vector<MaskedArray> data)
    : alternative_indices{std::move(alternative_indices)},
      data{std::move(data)} {
  }
  Storage(Storage const&) = default;
  Storage(Storage&&) noexcept = default;
  auto operator=(Storage const&) -> Storage& = delete;
  auto operator=(Storage&&) -> Storage& = delete;

  nova::storage::SparseStorage<nova::storage::Index> alternative_indices;
  nova::storage::Vector<MaskedArray> data;
};

UnionArray::UnionArray(
  nova::storage::SparseStorage<nova::storage::Index> indices,
  nova::storage::Vector<MaskedArray> fields)
  : storage_{storage::StructureOwner<Storage>::make(std::move(indices),
                                                    std::move(fields))} {
}

UnionArray::UnionArray(storage::StructureOwner<Storage> storage)
  : storage_{std::move(storage)} {
}

UnionArray::~UnionArray() = default;
UnionArray::UnionArray(const UnionArray&) = default;
UnionArray::UnionArray(UnionArray&&) noexcept = default;
auto UnionArray::operator=(const UnionArray&) -> UnionArray& = default;
auto UnionArray::operator=(UnionArray&&) noexcept -> UnionArray& = default;

auto UnionArray::as_unique() const& -> UnionArray {
  return UnionArray{storage_.as_unique()};
}

auto UnionArray::as_unique() && -> UnionArray {
  return UnionArray{std::move(storage_).as_unique()};
}

auto UnionArray::length() const noexcept -> nova::storage::Index {
  return storage_->alternative_indices.length();
}

auto UnionArray::fields() const -> const nova::storage::Vector<MaskedArray>& {
  return storage_->data;
}

auto UnionArray::alternative_index_at(nova::storage::Index row) const
  -> nova::storage::Index {
  return storage_->alternative_indices.get(row);
}

Array<Data>::Array(ErasedArray array)
  : ImplementErasure{
      match(std::move(array.data_), [](auto&& storage) -> ImplementErasure {
        return ImplementErasure{std::forward<decltype(storage)>(storage)};
      })} {
}

Array<Data>::Array(UnionArray array)
  : ImplementErasure{[&] {
      if (array.storage_->data.size() == 1) {
        return match(array.storage_->data[0].data.data_,
                     [](auto&& storage) -> ImplementErasure {
                       return ImplementErasure{
                         std::forward<decltype(storage)>(storage)};
                     });
      }
      return ImplementErasure{std::move(array)};
    }()} {
}

auto Array<Data>::as_unique() const& -> Array {
  return match(data_, [](auto const& x) -> Array {
    return Array{x.as_unique()};
  });
}

auto Array<Data>::as_unique() && -> Array {
  return match(data_, [](auto& x) -> Array {
    return Array{std::move(x).as_unique()};
  });
}

template <data_type Tag>
Array<Data>::Array(Array<Tag> arr)
  : ImplementErasure{
      match(std::move(arr).storage(), [](auto&& storage) -> ImplementErasure {
        return ImplementErasure{std::forward<decltype(storage)>(storage)};
      })} {
}

namespace {

template <typename Storage>
consteval auto tag_for_physical_storage() -> std::size_t {
  static_assert(ErasedArrayAlternatives::contains<Storage>);
  auto index = std::size_t{0};
  [&]<std::size_t... Is>(std::index_sequence<Is...>) {
    std::ignore
      = ((Type<data_type_list::at<Is>>::PhysicalStorage::template contains<
            Storage>
            ? (static_cast<void>(index = Is), true)
            : false)
         or ...);
  }(data_type_list::index_sequence);
  return index;
}

template <typename Storage>
using TagForPhysicalStorage
  = data_type_list::at<tag_for_physical_storage<Storage>()>;

template <typename T>
consteval auto logical_index_for_physical_alternative() -> std::size_t {
  if constexpr (std::same_as<T, UnionArray>) {
    return DataVariantAlternatives::unique_index_of<UnionArray>;
  } else {
    return ErasedDataAlternatives::unique_index_of<
      Array<TagForPhysicalStorage<T>>>;
  }
}

template <std::size_t I, typename PhysicalVariant>
auto get_logical_alternative(const PhysicalVariant& data_)
  -> UnionArrayAlternativeResultT<ErasedDataAlternatives, I,
                                  const PhysicalVariant> {
  using Alt = ErasedDataAlternatives::at<I>;
  auto result = Option<Alt>{};
  auto try_one = [&]<typename T>(std::type_identity<T>) {
    if constexpr (logical_index_for_physical_alternative<T>() == I) {
      if (const auto* alt = std::get_if<T>(&data_)) {
        result = Alt{*alt};
      }
    }
  };
  [&]<std::size_t... Is>(std::index_sequence<Is...>) {
    (try_one(std::type_identity<ErasedArrayAlternatives::at<Is>>{}), ...);
  }(ErasedArrayAlternatives::index_sequence);
  TENZIR_ASSERT(result);
  return std::move(*result);
}

template <typename T>
auto row_view_for_alternative(nova::storage::Index i, const T& value)
  -> DataRowView {
  using Tag = TagForPhysicalStorage<T>;
  // Borrow directly from the physical storage owned by the erased array.
  // A reconstructed logical array would leave structured views dangling.
  return DataRowView{RowView<Tag>{value.get(i)}};
}

} // namespace

auto UnionArray::get(nova::storage::Index i) const -> DataRowView {
  const auto alternative = storage_->alternative_indices.get(i);
  TENZIR_ASSERT_LEQ_EXPENSIVE(0, alternative);
  return storage_->data[static_cast<std::size_t>(alternative)].data.get(i);
}

auto ErasedArray::get(nova::storage::Index i) const -> DataRowView {
  return match(data_, [i]<typename T>(const T& array) -> DataRowView {
    return row_view_for_alternative(i, array);
  });
}

auto Array<Data>::get(nova::storage::Index i) const -> DataRowView {
  return match(data_, [i]<typename T>(const T& array) -> DataRowView {
    if constexpr (std::same_as<T, UnionArray>) {
      return array.get(i);
    } else {
      return row_view_for_alternative(i, array);
    }
  });
}

auto UnionArray::null_where(nova::storage::BitMap mask) const& -> UnionArray {
  return as_unique().null_where(std::move(mask));
}

auto UnionArray::null_where(nova::storage::BitMap mask) && -> UnionArray {
  const auto length = this->length();
  constexpr auto null_tag
    = ErasedDataAlternatives::unique_index_of<Array<Null>>;
  auto null_alternative = Option<nova::storage::Index>{};
  auto unique = std::move(*this).as_unique();
  auto& data = unique.storage_->data;
  for (auto i = std::size_t{0}; i < data.size(); ++i) {
    if (variant_traits<ErasedArray>::index(data[i].data) == null_tag) {
      static_assert(Type<Null>::PhysicalStorage::size == 1);
      null_alternative = static_cast<nova::storage::Index>(i);
      continue;
    }
    data[i].present = std::move(data[i].present).and_not(mask);
  }
  if (null_alternative) {
    auto& present = data[static_cast<std::size_t>(*null_alternative)].present;
    present = std::move(present) | mask;
  } else {
    null_alternative = static_cast<nova::storage::Index>(data.size());
    data.push_back(UnionArray::MaskedArray{
      .data = ErasedArray{Array<Null>{nova::storage::NullStorage{length}}},
      .present = mask,
    });
  }
  auto new_alternative_indices
    = nova::storage::SparseStorage<nova::storage::Index>::Mutable{
      std::move(unique.storage_->alternative_indices)};
  for (auto row : nova::storage::true_bits(mask)) {
    new_alternative_indices.set(row, *null_alternative);
  }
  return UnionArray{std::move(new_alternative_indices).finish(),
                    std::move(data)};
}

auto Array<Data>::null_where(
  nova::storage::BitMap to_null) const& -> Array<Data> {
  return as_unique().null_where(std::move(to_null));
}

auto Array<Data>::null_where(nova::storage::BitMap to_null) && -> Array<Data> {
  if (auto constant = to_null.as_constant()) {
    if (*constant) {
      return Array<Data>{Array<Null>{nova::storage::NullStorage{length()}}};
    }
    return *this;
  }
  return match(
    std::move(*this),
    [&to_null](UnionArray&& array) -> Array<Data> {
      return Array<Data>{std::move(array).null_where(to_null)};
    },
    [&to_null](auto&& concrete) -> Array<Data> {
      const auto length = concrete.length();
      auto not_to_null = to_null.make_inverted();
      auto const null_count = to_null.true_count();
      auto const null_first = null_count > length - null_count;
      auto alternatives = nova::storage::Vector<UnionArray::MaskedArray>{};
      alternatives.reserve(2);
      if (null_first) {
        alternatives.push_back(UnionArray::MaskedArray{
          .data = ErasedArray{Array<Null>{nova::storage::NullStorage{length}}},
          .present = std::move(to_null),
        });
        alternatives.push_back(UnionArray::MaskedArray{
          .data = ErasedArray{std::move(concrete)},
          .present = std::move(not_to_null),
        });
      } else {
        alternatives.push_back(UnionArray::MaskedArray{
          .data = ErasedArray{std::move(concrete)},
          .present = std::move(not_to_null),
        });
        alternatives.push_back(UnionArray::MaskedArray{
          .data = ErasedArray{Array<Null>{nova::storage::NullStorage{length}}},
          .present = std::move(to_null),
        });
      }
      auto indices
        = nova::storage::SparseStorage<nova::storage::Index>::Mutable{length};
      for (auto row : nova::storage::true_bits(alternatives[1].present)) {
        indices.set(row, 1);
      }
      return Array<Data>{
        UnionArray{std::move(indices).finish(), std::move(alternatives)}};
    });
}

template <data_type Tag>
auto Array<Data>::try_as() const -> Option<Array<Tag>> {
  return match(
    *this,
    [](Array<Tag> arr) -> Option<Array<Tag>> {
      return Option<Array<Tag>>{std::move(arr)};
    },
    [](const auto&) -> Option<Array<Tag>> {
      return None{};
    });
}

template <data_type Tag>
auto Array<Data>::get_alternative() const
  -> Option<nova::MaskedArray<Array<Tag>>> {
  return match(
    *this,
    [](Array<Tag> arr) -> Option<nova::MaskedArray<Array<Tag>>> {
      const auto length = arr.length();
      return nova::MaskedArray<Array<Tag>>{std::move(arr),
                                           nova::storage::BitMap{length, true}};
    },
    [](const UnionArray& array) {
      return array.template get_alternative<Tag>();
    },
    [](const auto&) -> Option<nova::MaskedArray<Array<Tag>>> {
      return None{};
    });
}

template <data_type Tag>
auto UnionArray::map_alternative(
  detail::function_view<auto(nova::MaskedArray<Array<Tag>>)->Array<Tag>> f)
  const& -> UnionArray {
  return UnionArray{*this}.map_alternative<Tag>(f);
}

template <data_type Tag>
auto UnionArray::map_alternative(
  detail::function_view<auto(nova::MaskedArray<Array<Tag>>)->Array<Tag>>
    f) && -> UnionArray {
  constexpr auto wanted = ErasedDataAlternatives::unique_index_of<Array<Tag>>;
  for (auto i = std::size_t{0}; i < fields().size(); ++i) {
    if (variant_traits<ErasedArray>::index(fields()[i].data) != wanted) {
      continue;
    }
    *this = std::move(*this).as_unique();
    auto& field = storage_->data[i];
    field.data = match(
      field.data.data_, [&]<class Storage>(Storage& storage) -> ErasedArray {
        if constexpr (Type<Tag>::PhysicalStorage::template contains<Storage>) {
          auto result = f({Array<Tag>{std::move(storage)}, field.present});
          TENZIR_ASSERT_EQ(result.length(), length());
          return result;
        } else {
          return ErasedArray{std::move(storage)};
        }
      });
    break;
  }
  return std::move(*this);
}

template <data_type Tag>
auto Array<Data>::map_alternative(
  detail::function_view<auto(nova::MaskedArray<Array<Tag>>)->Array<Tag>> f)
  const& -> Array<Data> {
  return Array{*this}.map_alternative<Tag>(f);
}

template <data_type Tag>
auto Array<Data>::map_alternative(
  detail::function_view<auto(nova::MaskedArray<Array<Tag>>)->Array<Tag>>
    f) && -> Array<Data> {
  auto const size = length();
  auto apply = [&](nova::MaskedArray<Array<Tag>> alternative) {
    auto result = f(std::move(alternative));
    TENZIR_ASSERT_EQ(result.length(), size);
    return result;
  };
  return match(data_, [&]<class Physical>(Physical& physical) -> Array<Data> {
    if constexpr (std::same_as<Physical, UnionArray>) {
      return Array<Data>{std::move(physical).template map_alternative<Tag>(f)};
    } else if constexpr (Type<Tag>::PhysicalStorage::template contains<
                           Physical>) {
      return apply(
        {Array<Tag>{std::move(physical)}, nova::storage::BitMap{size, true}});
    } else {
      return Array<Data>{std::move(physical)};
    }
  });
}

template <data_type Tag>
auto UnionArray::get_alternative()
  const& -> Option<nova::MaskedArray<Array<Tag>>> {
  constexpr auto wanted = ErasedDataAlternatives::unique_index_of<Array<Tag>>;
  for (const auto& alternative : storage_->data) {
    if (variant_traits<ErasedArray>::index(alternative.data) == wanted) {
      return nova::MaskedArray<Array<Tag>>{
        variant_traits<ErasedArray>::get<wanted>(alternative.data),
        alternative.present,
      };
    }
  }
  return None{};
}

template <data_type Tag>
auto UnionArray::get_alternative() && -> Option<nova::MaskedArray<Array<Tag>>> {
  storage_ = std::move(storage_).as_unique();
  constexpr auto wanted = ErasedDataAlternatives::unique_index_of<Array<Tag>>;
  for (auto& alternative : storage_->data) {
    if (variant_traits<ErasedArray>::index(alternative.data) == wanted) {
      return nova::MaskedArray<Array<Tag>>{
        std::move(variant_traits<ErasedArray>::get<wanted>(alternative.data)),
        std::move(alternative.present),
      };
    }
  }
  return None{};
}

template <data_type Tag>
auto UnionArray::alternative_mask() const -> storage::BitMap {
  constexpr static auto wanted
    = ErasedDataAlternatives::unique_index_of<Array<Tag>>;
  for (const auto& alternative : storage_->data) {
    if (variant_traits<ErasedArray>::index(alternative.data) == wanted) {
      return alternative.present;
    }
  }
  return storage::BitMap{length(), false};
}

#define TENZIR_INSTANTIATE_ARRAY_DATA_MEMBERS(Tag)                             \
  template auto Array<Data>::try_as<Tag>() const -> Option<Array<Tag>>

TENZIR_INSTANTIATE_ARRAY_DATA_MEMBERS(Null);
TENZIR_INSTANTIATE_ARRAY_DATA_MEMBERS(Bool);
TENZIR_INSTANTIATE_ARRAY_DATA_MEMBERS(Int);
TENZIR_INSTANTIATE_ARRAY_DATA_MEMBERS(UInt);
TENZIR_INSTANTIATE_ARRAY_DATA_MEMBERS(Float);
TENZIR_INSTANTIATE_ARRAY_DATA_MEMBERS(String);
TENZIR_INSTANTIATE_ARRAY_DATA_MEMBERS(Blob);
TENZIR_INSTANTIATE_ARRAY_DATA_MEMBERS(Ip);
TENZIR_INSTANTIATE_ARRAY_DATA_MEMBERS(Subnet);
TENZIR_INSTANTIATE_ARRAY_DATA_MEMBERS(Time);
TENZIR_INSTANTIATE_ARRAY_DATA_MEMBERS(Duration);
TENZIR_INSTANTIATE_ARRAY_DATA_MEMBERS(List);
TENZIR_INSTANTIATE_ARRAY_DATA_MEMBERS(Record);

#undef TENZIR_INSTANTIATE_ARRAY_DATA_MEMBERS

#define TENZIR_INSTANTIATE_DATA_TYPE_MEMBERS(Tag)                              \
  template Array<Data>::Array(Array<Tag>);                                     \
  template auto Array<Data>::get_alternative<Tag>() const                      \
    -> Option<nova::MaskedArray<Array<Tag>>>;                                  \
  template auto UnionArray::get_alternative<Tag>()                             \
    const& -> Option<nova::MaskedArray<Array<Tag>>>;                           \
  template auto UnionArray::get_alternative<Tag>() && -> Option<               \
    nova::MaskedArray<Array<Tag>>>;                                            \
  template auto UnionArray::alternative_mask<Tag>() const -> storage::BitMap;  \
  template auto UnionArray::map_alternative<Tag>(                              \
    detail::function_view<auto(nova::MaskedArray<Array<Tag>>)->Array<Tag>>)    \
    const&->UnionArray;                                                        \
  template auto UnionArray::map_alternative<Tag>(                              \
    detail::function_view<                                                     \
      auto(nova::MaskedArray<Array<Tag>>)->Array<Tag>>) && -> UnionArray;      \
  template auto Array<Data>::map_alternative<Tag>(                             \
    detail::function_view<auto(nova::MaskedArray<Array<Tag>>)->Array<Tag>>)    \
    const&->Array<Data>;                                                       \
  template auto Array<Data>::map_alternative<Tag>(                             \
    detail::function_view<                                                     \
      auto(nova::MaskedArray<Array<Tag>>)->Array<Tag>>) && -> Array<Data>

TENZIR_INSTANTIATE_DATA_TYPE_MEMBERS(Null);
TENZIR_INSTANTIATE_DATA_TYPE_MEMBERS(Bool);
TENZIR_INSTANTIATE_DATA_TYPE_MEMBERS(Int);
TENZIR_INSTANTIATE_DATA_TYPE_MEMBERS(UInt);
TENZIR_INSTANTIATE_DATA_TYPE_MEMBERS(Float);
TENZIR_INSTANTIATE_DATA_TYPE_MEMBERS(String);
TENZIR_INSTANTIATE_DATA_TYPE_MEMBERS(Blob);
TENZIR_INSTANTIATE_DATA_TYPE_MEMBERS(Ip);
TENZIR_INSTANTIATE_DATA_TYPE_MEMBERS(Subnet);
TENZIR_INSTANTIATE_DATA_TYPE_MEMBERS(Time);
TENZIR_INSTANTIATE_DATA_TYPE_MEMBERS(Duration);
TENZIR_INSTANTIATE_DATA_TYPE_MEMBERS(List);
TENZIR_INSTANTIATE_DATA_TYPE_MEMBERS(Record);

#undef TENZIR_INSTANTIATE_DATA_TYPE_MEMBERS

} // namespace tenzir::nova

namespace tenzir {

auto variant_traits<nova::ErasedArray>::index(const nova::ErasedArray& array)
  -> size_t {
  return match(array.data_, []<typename T>(const T&) {
    return nova::logical_index_for_physical_alternative<T>();
  });
}

template <size_t I>
auto variant_traits<nova::ErasedArray>::get(const nova::ErasedArray& array)
  -> nova::UnionArrayAlternativeResultT<nova::ErasedDataAlternatives, I,
                                        const nova::ErasedArray> {
  return nova::get_logical_alternative<I>(array.data_);
}

template <size_t I>
auto variant_traits<nova::ErasedArray>::get(nova::ErasedArray& array)
  -> nova::UnionArrayAlternativeResultT<nova::ErasedDataAlternatives, I,
                                        nova::ErasedArray> {
  return nova::get_logical_alternative<I>(array.data_);
}

auto variant_traits<nova::Array<nova::Data>>::index(
  const nova::Array<nova::Data>& array) -> size_t {
  return match(array.data_, []<typename T>(const T&) {
    return nova::logical_index_for_physical_alternative<T>();
  });
}

template <size_t I>
auto variant_traits<nova::Array<nova::Data>>::get(
  const nova::Array<nova::Data>& array)
  -> nova::UnionArrayAlternativeResultT<nova::DataVariantAlternatives, I,
                                        const nova::Array<nova::Data>> {
  if constexpr (I == union_index) {
    return std::get<nova::UnionArray>(array.data_);
  } else {
    return nova::get_logical_alternative<I>(array.data_);
  }
}

template <size_t I>
auto variant_traits<nova::Array<nova::Data>>::get(nova::Array<nova::Data>& array)
  -> nova::UnionArrayAlternativeResultT<nova::DataVariantAlternatives, I,
                                        nova::Array<nova::Data>> {
  if constexpr (I == union_index) {
    return std::get<nova::UnionArray>(array.data_);
  } else {
    return nova::get_logical_alternative<I>(array.data_);
  }
}

#define TENZIR_INSTANTIATE_ERASED_ARRAY_GET(I)                                 \
  template auto variant_traits<nova::ErasedArray>::get<I>(                     \
    const nova::ErasedArray&)                                                  \
    -> nova::UnionArrayAlternativeResultT<nova::ErasedDataAlternatives, I,     \
                                          const nova::ErasedArray>;            \
  template auto variant_traits<nova::ErasedArray>::get<I>(nova::ErasedArray&)  \
    -> nova::UnionArrayAlternativeResultT<nova::ErasedDataAlternatives, I,     \
                                          nova::ErasedArray>

TENZIR_INSTANTIATE_ERASED_ARRAY_GET(0);
TENZIR_INSTANTIATE_ERASED_ARRAY_GET(1);
TENZIR_INSTANTIATE_ERASED_ARRAY_GET(2);
TENZIR_INSTANTIATE_ERASED_ARRAY_GET(3);
TENZIR_INSTANTIATE_ERASED_ARRAY_GET(4);
TENZIR_INSTANTIATE_ERASED_ARRAY_GET(5);
TENZIR_INSTANTIATE_ERASED_ARRAY_GET(6);
TENZIR_INSTANTIATE_ERASED_ARRAY_GET(7);
TENZIR_INSTANTIATE_ERASED_ARRAY_GET(8);
TENZIR_INSTANTIATE_ERASED_ARRAY_GET(9);
TENZIR_INSTANTIATE_ERASED_ARRAY_GET(10);
TENZIR_INSTANTIATE_ERASED_ARRAY_GET(11);
TENZIR_INSTANTIATE_ERASED_ARRAY_GET(12);

#undef TENZIR_INSTANTIATE_ERASED_ARRAY_GET

#define TENZIR_INSTANTIATE_DATA_ARRAY_GET(I)                                   \
  template auto variant_traits<nova::Array<nova::Data>>::get<I>(               \
    const nova::Array<nova::Data>&)                                            \
    -> nova::UnionArrayAlternativeResultT<nova::DataVariantAlternatives, I,    \
                                          const nova::Array<nova::Data>>;      \
  template auto variant_traits<nova::Array<nova::Data>>::get<I>(               \
    nova::Array<nova::Data>&)                                                  \
    -> nova::UnionArrayAlternativeResultT<nova::DataVariantAlternatives, I,    \
                                          nova::Array<nova::Data>>

TENZIR_INSTANTIATE_DATA_ARRAY_GET(0);
TENZIR_INSTANTIATE_DATA_ARRAY_GET(1);
TENZIR_INSTANTIATE_DATA_ARRAY_GET(2);
TENZIR_INSTANTIATE_DATA_ARRAY_GET(3);
TENZIR_INSTANTIATE_DATA_ARRAY_GET(4);
TENZIR_INSTANTIATE_DATA_ARRAY_GET(5);
TENZIR_INSTANTIATE_DATA_ARRAY_GET(6);
TENZIR_INSTANTIATE_DATA_ARRAY_GET(7);
TENZIR_INSTANTIATE_DATA_ARRAY_GET(8);
TENZIR_INSTANTIATE_DATA_ARRAY_GET(9);
TENZIR_INSTANTIATE_DATA_ARRAY_GET(10);
TENZIR_INSTANTIATE_DATA_ARRAY_GET(11);
TENZIR_INSTANTIATE_DATA_ARRAY_GET(12);
TENZIR_INSTANTIATE_DATA_ARRAY_GET(13);

#undef TENZIR_INSTANTIATE_DATA_ARRAY_GET

} // namespace tenzir
