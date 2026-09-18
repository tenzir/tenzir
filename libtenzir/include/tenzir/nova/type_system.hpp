//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/blob.hpp"
#include "tenzir/concepts.hpp"
#include "tenzir/detail/stable_map.hpp"
#include "tenzir/ip.hpp"
#include "tenzir/nova/bitmap.hpp"
#include "tenzir/nova/storage.hpp"
#include "tenzir/nova/type_list.hpp"
#include "tenzir/subnet.hpp"
#include "tenzir/time.hpp"
#include "tenzir/view.hpp"

#include <concepts>
#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>
#include <variant>
#include <vector>

namespace tenzir::nova::storage {

using DenseStringOffsetStorage
  = DenseOffsetBytesStorage<char, std::string_view>;
using DenseBlobOffsetStorage
  = DenseOffsetBytesStorage<std::byte, tenzir::blob_view>;

static_assert(storage<DenseStringOffsetStorage>);
static_assert(storage<DenseBlobOffsetStorage>);

} // namespace tenzir::nova::storage

namespace tenzir::nova {

template <typename T>
struct StrongType {
  T value;

  constexpr operator T() noexcept {
    return value;
  }
};

using Null = std::monostate;
using Bool = bool;
using Int = std::int64_t;
using UInt = std::uint64_t;
using Float = double;
using String = std::string;
using Ip = tenzir::ip;
using Subnet = tenzir::subnet;
using Time = tenzir::time;
using Duration = tenzir::duration;
using Blob = tenzir::blob;
using BlobView = tenzir::blob_view;
class Data;
class List : public std::vector<Data> {
public:
  using vector::vector;
};
class Record : public detail::stable_map<std::string, Data> {
public:
  using Base = detail::stable_map<std::string, Data>;
  using Base::Base;
};
template <class T>
class RowView;
template <>
class RowView<List>;
template <>
class RowView<Record>;
namespace storage {
class ListStorage;
class RecordStorage;
} // namespace storage

using fundamental_type_list = TypeList<Null, Bool, Int, UInt, Float, String,
                                       Blob, Ip, Subnet, Time, Duration>;

using structured_type_list = TypeList<List, Record>;

using data_type_list = fundamental_type_list::join<structured_type_list>;

template <typename T>
concept fundamental_type = fundamental_type_list::contains<T>;

template <typename T>
concept structured_type = structured_type_list::contains<T>;

template <typename T>
concept data_type = fundamental_type<T> or structured_type<T>;

template <typename T>
concept concrete_or_erased_type = data_type<T> or std::same_as<T, Data>;

template <concrete_or_erased_type T>
class Type;

class Data : public data_type_list::apply<variant> {
public:
  using Base = data_type_list::apply<variant>;
  using Base::Base;
};

template <typename Type>
concept fully_implemented_common = requires {
  // The type must be complete
  requires concepts::complete<Type>;

  typename Type::DataType;
  requires concepts::complete<typename Type::DataType>;
  requires data_type<typename Type::DataType>;

  // The type must provide ways to access its data
  { Type::static_name } -> std::same_as<const std::string_view&>;
};

template <typename View>
struct storage_view_convertible_to {
  template <typename Storage>
  struct predicate
    : std::bool_constant<std::convertible_to<typename Storage::ViewType, View>> {
  };
};

template <typename Type>
concept fully_implemented_type = fully_implemented_common<Type> and requires {
  typename Type::ViewType;
  requires concepts::complete<typename Type::ViewType>;
  requires std::convertible_to<typename Type::DataType const&,
                               typename Type::ViewType>;
  typename Type::PhysicalStorage;
  requires concepts::instantiation_of<typename Type::PhysicalStorage, TypeList>;
  requires Type::PhysicalStorage::size > 0;
  requires Type::PhysicalStorage::template all_of<storage::is_storage_api>;
  requires Type::PhysicalStorage::template all_of<
    storage_view_convertible_to<typename Type::ViewType>::template predicate>;
  typename Type::PrimaryPhysicalStorage;
  requires Type::PhysicalStorage::template contains<
    typename Type::PrimaryPhysicalStorage>;
};

} // namespace tenzir::nova

#define DEFINE_FUNDAMENTAL_TYPE(NAME, CPP_STRONG_TYPE, CPP_VIEW_TYPE, PRIMARY, \
                                ...)                                           \
  template <>                                                                  \
  class Type<CPP_STRONG_TYPE> {                                                \
  public:                                                                      \
    constexpr static std::string_view static_name = NAME;                      \
    using DataType = CPP_STRONG_TYPE;                                          \
    using ViewType = CPP_VIEW_TYPE;                                            \
    using PhysicalStorage = __VA_ARGS__;                                       \
    using PrimaryPhysicalStorage = PRIMARY;                                    \
  };                                                                           \
  static_assert(fully_implemented_type<Type<CPP_STRONG_TYPE>>)

namespace tenzir::nova {

DEFINE_FUNDAMENTAL_TYPE("null", Null, Null, storage::NullStorage,
                        TypeList<storage::NullStorage>);
DEFINE_FUNDAMENTAL_TYPE("bool", Bool, bool, storage::BitMap,
                        TypeList<storage::BitMap>);
DEFINE_FUNDAMENTAL_TYPE(
  "uint", UInt, std::uint64_t, storage::SparseStorage<std::uint64_t>,
  TypeList<std::uint8_t, std::uint16_t, std::uint32_t, std::uint64_t>::wrap<
    storage::SparseStorage>::append<storage::ConstantStorage<std::uint64_t>>);
DEFINE_FUNDAMENTAL_TYPE(
  "int", Int, std::int64_t, storage::SparseStorage<std::int64_t>,
  TypeList<std::int8_t, std::int16_t, std::int32_t, std::int64_t>::wrap<
    storage::SparseStorage>::append<storage::ConstantStorage<std::int64_t>>);
DEFINE_FUNDAMENTAL_TYPE("float", Float, double, storage::SparseStorage<double>,
                        TypeList<float, double>::wrap<storage::SparseStorage>::
                          append<storage::ConstantStorage<double>>);
DEFINE_FUNDAMENTAL_TYPE(
  "ip", Ip, Ip, storage::SparseStorage<Ip>,
  TypeList<storage::SparseStorage<Ip>>::append<storage::ConstantStorage<Ip>>);
DEFINE_FUNDAMENTAL_TYPE("subnet", Subnet, Subnet,
                        storage::SparseStorage<Subnet>,
                        TypeList<storage::SparseStorage<Subnet>>::append<
                          storage::ConstantStorage<Subnet>>);
DEFINE_FUNDAMENTAL_TYPE("time", Time, Time, storage::SparseStorage<Time>,
                        TypeList<storage::SparseStorage<Time>>::append<
                          storage::ConstantStorage<Time>>);
DEFINE_FUNDAMENTAL_TYPE("duration", Duration, Duration,
                        storage::SparseStorage<Duration>,
                        TypeList<storage::SparseStorage<Duration>>::append<
                          storage::ConstantStorage<Duration>>);
DEFINE_FUNDAMENTAL_TYPE(
  "string", String, std::string_view, storage::DenseStringOffsetStorage,
  TypeList<storage::DenseStringOffsetStorage>::append<
    storage::ConstantStorage<std::string, std::string_view>>);
DEFINE_FUNDAMENTAL_TYPE("blob", Blob, BlobView, storage::DenseBlobOffsetStorage,
                        TypeList<storage::DenseBlobOffsetStorage>::append<
                          storage::ConstantStorage<Blob, BlobView>>);

template <>
class Type<List> {
public:
  constexpr static std::string_view static_name = "list";
  using data_type = List;
  using DataType = List;
  using ViewType = RowView<List>;
  using PrimaryPhysicalStorage = storage::ListStorage;
  using PhysicalStorage
    = TypeList<PrimaryPhysicalStorage, storage::ConstantStorage<List, ViewType>>;
};

template <>
class Type<Record> {
public:
  constexpr static std::string_view static_name = "record";
  using data_type = Record;
  using DataType = Record;
  using ViewType = RowView<Record>;
  using PrimaryPhysicalStorage = storage::RecordStorage;
  using PhysicalStorage = TypeList<PrimaryPhysicalStorage,
                                   storage::ConstantStorage<Record, ViewType>>;
};

} // namespace tenzir::nova
