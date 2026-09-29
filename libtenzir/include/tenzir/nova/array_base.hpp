//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/nova/bitmap.hpp"
#include "tenzir/nova/type_system.hpp"

#include <cstddef>
#include <utility>

namespace tenzir::nova {

class Data;
class List;
class Record;

template <concrete_or_erased_type T>
class Array;

class ErasedArray;

template <class T>
class RowView {
public:
  using ViewType = typename Type<T>::ViewType;

  explicit RowView(ViewType value) : value_{std::move(value)} {
  }

  auto operator*() const -> ViewType const& {
    return value_;
  }

private:
  ViewType value_;
};

template <class Array>
struct MaskedArray {
  Array data;
  storage::BitMap present;

  /// Heap bytes owned by the data and the presence bitmap.
  auto approx_bytes() const noexcept -> std::size_t {
    return data.approx_bytes() + present.approx_bytes();
  }
};

} // namespace tenzir::nova
