//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concepts.hpp"
#include "tenzir/nova/array_erasure.hpp"
#include "tenzir/nova/fundamental_array.hpp"
#include "tenzir/nova/list_array.hpp"
#include "tenzir/nova/record_array.hpp"
#include "tenzir/nova/shared_owner.hpp"
#include "tenzir/option.hpp"
#include "tenzir/variant_traits.hpp"

#include <type_traits>

namespace tenzir::nova {

template <typename Type>
struct take_physical_storage
  : std::type_identity<typename Type::PhysicalStorage> {};

using ErasedArrayAlternatives
  = data_type_list::wrap<Type>::transform<take_physical_storage>::flatten;

class ErasedArray : private ImplementErasure<ErasedArrayAlternatives> {
  friend class Array<Data>;
  template <concepts::unqualified>
  friend class ::tenzir::variant_traits;

public:
  using ImplementErasure::ImplementErasure;

  ErasedArray();

  template <data_type Tag>
  ErasedArray(Array<Tag> arr)
    : ImplementErasure{
        match(std::move(arr).storage(), [](auto&& storage) -> ImplementErasure {
          return ImplementErasure{std::forward<decltype(storage)>(storage)};
        })} {
  }

  auto as_unique() const& -> ErasedArray;
  auto as_unique() && -> ErasedArray;
  auto get(nova::storage::Index i) const -> RowView<Data>;
};

class UnionArray {
public:
  using MaskedArray = nova::MaskedArray<ErasedArray>;

  UnionArray(storage::SparseStorage<storage::Index> alternative_indices,
             storage::Vector<MaskedArray> data);
  ~UnionArray();

  UnionArray(const UnionArray&);
  UnionArray(UnionArray&&) noexcept;
  auto operator=(const UnionArray&) -> UnionArray&;
  auto operator=(UnionArray&&) noexcept -> UnionArray&;

  [[nodiscard]] auto as_unique() const& -> UnionArray;
  [[nodiscard]] auto as_unique() && -> UnionArray;
  [[nodiscard]] auto length() const noexcept -> storage::Index;
  [[nodiscard]] auto get(storage::Index i) const -> RowView<Data>;

  template <data_type Tag>
  [[nodiscard]] auto
  get_alternative() const& -> Option<nova::MaskedArray<Array<Tag>>>;

  template <data_type Tag>
  [[nodiscard]] auto
  get_alternative() && -> Option<nova::MaskedArray<Array<Tag>>>;

  /// Rows whose active alternative is `Tag`; all-false if there is none.
  template <data_type Tag>
  [[nodiscard]] auto alternative_mask() const -> storage::BitMap;

  [[nodiscard]] auto fields() const -> const storage::Vector<MaskedArray>&;
  [[nodiscard]] auto alternative_index_at(storage::Index row) const
    -> storage::Index;
  [[nodiscard]] auto null_where(storage::BitMap mask) && -> UnionArray;
  [[nodiscard]] auto null_where(storage::BitMap mask) const& -> UnionArray;

private:
  friend class Array<Data>;
  struct Storage;
  explicit UnionArray(storage::StructureOwner<Storage> storage);
  storage::StructureOwner<Storage> storage_;
};

using ArrayAlternatives = ErasedArrayAlternatives::join<UnionArray>;
using ErasedDataAlternatives = data_type_list::wrap<Array>;
using DataVariantAlternatives = ErasedDataAlternatives::join<UnionArray>;

template <typename Tag>
struct take_view_type : std::type_identity<typename Type<Tag>::ViewType> {};

using fundamental_view_list = fundamental_type_list::transform<take_view_type>;
template <typename Tag>
struct take_row_view_type
  : std::type_identity<decltype(std::declval<Array<Tag> const&>().get(
      std::declval<storage::Index>()))> {};

using data_view_list = data_type_list::transform<take_row_view_type>;

template <>
class RowView<Data> {
  template <concepts::unqualified>
  friend class ::tenzir::variant_traits;

public:
  RowView(Data const& value);

  template <typename T>
  RowView(RowView<T> view) : data_{std::move(view)} {
  }

private:
  using Storage = data_view_list::apply<variant>;
  Storage data_;
};

using DataRowView = RowView<Data>;

template <>
class Array<Data> : private ImplementErasure<ArrayAlternatives> {
  friend class UnionArray;
  template <concepts::unqualified>
  friend class ::tenzir::variant_traits;

public:
  explicit Array(ErasedArray arr);
  explicit Array(UnionArray array);
  using ImplementErasure::ImplementErasure;
  using ImplementErasure::length;

  template <data_type Tag>
  Array(Array<Tag> arr);

  auto as_unique() const& -> Array;
  auto as_unique() && -> Array;
  /// Gets a view into the i-th row
  [[nodiscard]] auto get(nova::storage::Index i) const -> RowView<Data>;
  /// Creates a new array that is null where `to_null` is true
  [[nodiscard]] auto
  null_where(nova::storage::BitMap to_null) const& -> Array<Data>;
  [[nodiscard]] auto
  null_where(nova::storage::BitMap to_null) && -> Array<Data>;

  template <data_type Tag>
  [[nodiscard]] auto try_as() const -> Option<Array<Tag>>;

  template <data_type Tag>
  [[nodiscard]] auto get_alternative() const
    -> Option<nova::MaskedArray<Array<Tag>>>;
};

static_assert(storage::unique_ownership<ErasedArray>);
static_assert(storage::unique_ownership<UnionArray>);
static_assert(storage::unique_ownership<Array<Data>>);

// Explicit instantiation declarations for the member templates that
// `union_array.cpp` explicitly instantiates. These tell every including
// translation unit that a definition exists elsewhere, so the compiler
// neither instantiates them again nor warns about the definition being
// unavailable (`-Wundefined-func-template`).
#define TENZIR_DECLARE_ARRAY_DATA_MEMBERS(Tag)                                 \
  extern template auto Array<Data>::try_as<Tag>() const -> Option<Array<Tag>>

TENZIR_DECLARE_ARRAY_DATA_MEMBERS(Null);
TENZIR_DECLARE_ARRAY_DATA_MEMBERS(Bool);
TENZIR_DECLARE_ARRAY_DATA_MEMBERS(Int);
TENZIR_DECLARE_ARRAY_DATA_MEMBERS(UInt);
TENZIR_DECLARE_ARRAY_DATA_MEMBERS(Float);
TENZIR_DECLARE_ARRAY_DATA_MEMBERS(String);
TENZIR_DECLARE_ARRAY_DATA_MEMBERS(Blob);
TENZIR_DECLARE_ARRAY_DATA_MEMBERS(Ip);
TENZIR_DECLARE_ARRAY_DATA_MEMBERS(Subnet);
TENZIR_DECLARE_ARRAY_DATA_MEMBERS(Time);
TENZIR_DECLARE_ARRAY_DATA_MEMBERS(Duration);
TENZIR_DECLARE_ARRAY_DATA_MEMBERS(List);
TENZIR_DECLARE_ARRAY_DATA_MEMBERS(Record);

#undef TENZIR_DECLARE_ARRAY_DATA_MEMBERS

#define TENZIR_DECLARE_DATA_TYPE_MEMBERS(Tag)                                  \
  extern template Array<Data>::Array(Array<Tag>);                              \
  extern template auto Array<Data>::get_alternative<Tag>() const               \
    -> Option<nova::MaskedArray<Array<Tag>>>;                                  \
  extern template auto UnionArray::get_alternative<Tag>()                      \
    const& -> Option<nova::MaskedArray<Array<Tag>>>;                           \
  extern template auto UnionArray::get_alternative<Tag>() && -> Option<        \
    nova::MaskedArray<Array<Tag>>>;                                            \
  extern template auto UnionArray::alternative_mask<Tag>() const               \
    -> storage::BitMap

TENZIR_DECLARE_DATA_TYPE_MEMBERS(Null);
TENZIR_DECLARE_DATA_TYPE_MEMBERS(Bool);
TENZIR_DECLARE_DATA_TYPE_MEMBERS(Int);
TENZIR_DECLARE_DATA_TYPE_MEMBERS(UInt);
TENZIR_DECLARE_DATA_TYPE_MEMBERS(Float);
TENZIR_DECLARE_DATA_TYPE_MEMBERS(String);
TENZIR_DECLARE_DATA_TYPE_MEMBERS(Blob);
TENZIR_DECLARE_DATA_TYPE_MEMBERS(Ip);
TENZIR_DECLARE_DATA_TYPE_MEMBERS(Subnet);
TENZIR_DECLARE_DATA_TYPE_MEMBERS(Time);
TENZIR_DECLARE_DATA_TYPE_MEMBERS(Duration);
TENZIR_DECLARE_DATA_TYPE_MEMBERS(List);
TENZIR_DECLARE_DATA_TYPE_MEMBERS(Record);

#undef TENZIR_DECLARE_DATA_TYPE_MEMBERS

/// Constructs an array repeating one owning value, including nested values.
auto repeat(Data const& value, storage::Index length) -> Array<Data>;

auto equal(const RowView<Data>& lhs, const RowView<Data>& rhs) -> bool;

/// Hashes a row consistently with `equal`.
auto hash(const RowView<Data>& row) noexcept -> std::size_t;

template <class Alternatives, std::size_t I, class Qualifier>
struct UnionArrayAlternativeResult {
  using Alternative = Alternatives::template at<I>;
  using type
    = std::conditional_t<std::same_as<Alternative, UnionArray>,
                         std::conditional_t<std::is_const_v<Qualifier>,
                                            const Alternative&, Alternative&>,
                         Alternative>;
};

template <class Alternatives, std::size_t I, class Qualifier>
using UnionArrayAlternativeResultT =
  typename UnionArrayAlternativeResult<Alternatives, I, Qualifier>::type;

} // namespace tenzir::nova

namespace tenzir {

template <>
class variant_traits<nova::ErasedArray> {
public:
  static constexpr auto count = nova::ErasedDataAlternatives::size;

  [[nodiscard]] static auto index(const nova::ErasedArray& array) -> size_t;

  template <size_t I>
  [[nodiscard]] static auto get(const nova::ErasedArray& array)
    -> nova::UnionArrayAlternativeResultT<nova::ErasedDataAlternatives, I,
                                          const nova::ErasedArray>;

  template <size_t I>
  [[nodiscard]] static auto get(nova::ErasedArray& array)
    -> nova::UnionArrayAlternativeResultT<nova::ErasedDataAlternatives, I,
                                          nova::ErasedArray>;
};

template <>
class variant_traits<nova::Array<nova::Data>> {
public:
  static constexpr auto count = nova::DataVariantAlternatives::size;
  static constexpr auto union_index
    = nova::DataVariantAlternatives::unique_index_of<nova::UnionArray>;

  [[nodiscard]] static auto index(const nova::Array<nova::Data>& array)
    -> size_t;

  template <size_t I>
  [[nodiscard]] static auto get(const nova::Array<nova::Data>& array)
    -> nova::UnionArrayAlternativeResultT<nova::DataVariantAlternatives, I,
                                          const nova::Array<nova::Data>>;

  template <size_t I>
  [[nodiscard]] static auto get(nova::Array<nova::Data>& array)
    -> nova::UnionArrayAlternativeResultT<nova::DataVariantAlternatives, I,
                                          nova::Array<nova::Data>>;
};

template <>
class variant_traits<nova::RowView<nova::Data>> {
public:
  static constexpr auto count = nova::data_view_list::size;

  [[nodiscard]] static auto index(const nova::RowView<nova::Data>& view)
    -> size_t {
    return variant_traits<decltype(view.data_)>::index(view.data_);
  }

  template <size_t I>
  [[nodiscard]] static auto get(const nova::RowView<nova::Data>& view)
    -> decltype(auto) {
    return variant_traits<decltype(view.data_)>::template get<I>(view.data_);
  }
};

// Explicit instantiation declarations matching the explicit instantiation
// definitions in `union_array.cpp` (see the note in `tenzir::nova` above).
#define TENZIR_DECLARE_ERASED_ARRAY_GET(I)                                     \
  extern template auto variant_traits<nova::ErasedArray>::get<I>(              \
    const nova::ErasedArray&)                                                  \
    -> nova::UnionArrayAlternativeResultT<nova::ErasedDataAlternatives, I,     \
                                          const nova::ErasedArray>;            \
  extern template auto variant_traits<nova::ErasedArray>::get<I>(              \
    nova::ErasedArray&)                                                        \
    -> nova::UnionArrayAlternativeResultT<nova::ErasedDataAlternatives, I,     \
                                          nova::ErasedArray>

TENZIR_DECLARE_ERASED_ARRAY_GET(0);
TENZIR_DECLARE_ERASED_ARRAY_GET(1);
TENZIR_DECLARE_ERASED_ARRAY_GET(2);
TENZIR_DECLARE_ERASED_ARRAY_GET(3);
TENZIR_DECLARE_ERASED_ARRAY_GET(4);
TENZIR_DECLARE_ERASED_ARRAY_GET(5);
TENZIR_DECLARE_ERASED_ARRAY_GET(6);
TENZIR_DECLARE_ERASED_ARRAY_GET(7);
TENZIR_DECLARE_ERASED_ARRAY_GET(8);
TENZIR_DECLARE_ERASED_ARRAY_GET(9);
TENZIR_DECLARE_ERASED_ARRAY_GET(10);
TENZIR_DECLARE_ERASED_ARRAY_GET(11);
TENZIR_DECLARE_ERASED_ARRAY_GET(12);

#undef TENZIR_DECLARE_ERASED_ARRAY_GET

#define TENZIR_DECLARE_DATA_ARRAY_GET(I)                                       \
  extern template auto variant_traits<nova::Array<nova::Data>>::get<I>(        \
    const nova::Array<nova::Data>&)                                            \
    -> nova::UnionArrayAlternativeResultT<nova::DataVariantAlternatives, I,    \
                                          const nova::Array<nova::Data>>;      \
  extern template auto variant_traits<nova::Array<nova::Data>>::get<I>(        \
    nova::Array<nova::Data>&)                                                  \
    -> nova::UnionArrayAlternativeResultT<nova::DataVariantAlternatives, I,    \
                                          nova::Array<nova::Data>>

TENZIR_DECLARE_DATA_ARRAY_GET(0);
TENZIR_DECLARE_DATA_ARRAY_GET(1);
TENZIR_DECLARE_DATA_ARRAY_GET(2);
TENZIR_DECLARE_DATA_ARRAY_GET(3);
TENZIR_DECLARE_DATA_ARRAY_GET(4);
TENZIR_DECLARE_DATA_ARRAY_GET(5);
TENZIR_DECLARE_DATA_ARRAY_GET(6);
TENZIR_DECLARE_DATA_ARRAY_GET(7);
TENZIR_DECLARE_DATA_ARRAY_GET(8);
TENZIR_DECLARE_DATA_ARRAY_GET(9);
TENZIR_DECLARE_DATA_ARRAY_GET(10);
TENZIR_DECLARE_DATA_ARRAY_GET(11);
TENZIR_DECLARE_DATA_ARRAY_GET(12);
TENZIR_DECLARE_DATA_ARRAY_GET(13);

#undef TENZIR_DECLARE_DATA_ARRAY_GET

} // namespace tenzir
