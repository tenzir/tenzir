//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/nova/array_builder_base.hpp"
#include "tenzir/nova/fundamental_array.hpp"
#include "tenzir/nova/type_system.hpp"

#include <concepts>
#include <type_traits>

namespace tenzir::nova {

template <typename V, typename WrappedType>
struct ViewTypeMatches
  : std::bool_constant<std::same_as<V, typename WrappedType::ViewType>> {};

template <typename V>
concept fundamental_view_type
  = fundamental_type_list::wrap<Type>::one_of<V, ViewTypeMatches>;

template <typename V>
consteval auto tag_index_for_view_type() -> std::size_t {
  auto index = std::size_t{0};
  [&]<std::size_t... Is>(std::index_sequence<Is...>) {
    std::ignore
      = ((std::same_as<V, typename Type<fundamental_type_list::at<Is>>::ViewType>
            ? (static_cast<void>(index = Is), true)
            : false)
         or ...);
  }(fundamental_type_list::index_sequence);
  return index;
}

template <fundamental_view_type V>
using TagForViewType = fundamental_type_list::at<tag_index_for_view_type<V>()>;

template <fundamental_type Tag>
class ArrayBuilder<Tag> {
public:
  using View = Type<Tag>::ViewType;
  using PrimaryStorage = Type<Tag>::PrimaryPhysicalStorage;

  auto data(View v) -> void;
  auto skip() -> void;
  auto skip_n(storage::Index count) -> void;
  auto length() const -> storage::Index;
  /// Removes the last row, which must hold a value, and returns it.
  auto take_last() -> Data;
  auto finish() -> Array<Tag>;

private:
  storage::SharedOwner<View[]>::Builder data_builder;
};

template <>
class ArrayBuilder<Bool> {
public:
  auto data(bool v) -> void;
  auto skip() -> void;
  auto skip_n(storage::Index count) -> void;
  auto length() const -> storage::Index;
  auto take_last() -> Data;
  auto finish() -> Array<Bool>;

private:
  storage::BitMap::Builder data_builder;
};

template <>
class ArrayBuilder<Null> {
public:
  auto null() -> void;
  auto skip() -> void;
  auto skip_n(storage::Index count) -> void;
  auto length() const -> storage::Index;
  auto take_last() -> Data;
  auto finish() -> Array<Null>;

private:
  storage::Index length_ = 0;
};

/// Shared implementation for the variable-length tags (`String`, `Blob`),
/// whose payloads are appended to one flat buffer plus a per-row `Span`.
template <fundamental_type Tag, typename Char>
class DenseOffsetArrayBuilder {
public:
  using View = Type<Tag>::ViewType;

  auto data(View v) -> void;
  auto skip() -> void;
  auto skip_n(storage::Index count) -> void;
  auto length() const -> storage::Index;
  auto take_last() -> Data;
  auto finish() -> Array<Tag>;

private:
  storage::SharedOwner<Char[]>::Builder data_builder;
  storage::SharedOwner<storage::Span[]>::Builder range_builder;
};

template <>
class ArrayBuilder<String> : public DenseOffsetArrayBuilder<String, char> {};

template <>
class ArrayBuilder<Blob> : public DenseOffsetArrayBuilder<Blob, std::byte> {};

extern template class DenseOffsetArrayBuilder<String, char>;
extern template class DenseOffsetArrayBuilder<Blob, std::byte>;

extern template class ArrayBuilder<Int>;
extern template class ArrayBuilder<UInt>;
extern template class ArrayBuilder<Float>;
extern template class ArrayBuilder<Ip>;
extern template class ArrayBuilder<Subnet>;
extern template class ArrayBuilder<Time>;
extern template class ArrayBuilder<Duration>;

} // namespace tenzir::nova
