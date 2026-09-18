//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <concepts>
#include <cstdint>
#include <type_traits>
#include <utility>

namespace tenzir::nova::storage {

using Index = std::int32_t;

struct Span {
  Index begin;
  Index end;
};

template <class T>
class SharedOwner;

template <class T>
class SparseStorage;

template <class T>
concept unique_ownership = requires(T x, T const cx) {
  { cx.as_unique() } -> std::same_as<T>;
  { std::move(x).as_unique() } -> std::same_as<T>;
};

template <class Storage>
concept storage = unique_ownership<Storage>
                  and requires(Storage s, Storage const cs, Index i) {
                        typename Storage::ViewType;
                        { cs.length() } noexcept -> std::same_as<Index>;
                        {
                          cs.get(i)
                        } -> std::same_as<typename Storage::ViewType>;
                      };

template <class Storage>
struct is_storage_api
  : std::conditional_t<storage<Storage>, std::true_type, std::false_type> {};

} // namespace tenzir::nova::storage
