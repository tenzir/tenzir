//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/nova/array_base.hpp"
#include "tenzir/nova/fundamental_array_builder.hpp"
#include "tenzir/nova/record_array_builder.hpp"

#include <memory>

namespace tenzir::nova {

template <>
class ArrayBuilder<List> {
public:
  class ListBuilder {
  public:
    template <fundamental_view_type V>
    auto data(V v) -> void;

    auto data(Time v) -> void {
      data<Time>(v);
    }

    auto data(std::string_view v) -> void {
      data<std::string_view>(v);
    }

    auto null() -> void;
    auto record() -> ArrayBuilder<Record>::RecordBuilder;
    auto list() -> ListBuilder;

  private:
    friend class ArrayBuilder;
    explicit ListBuilder(ArrayBuilder* parent);
    ArrayBuilder* parent_ = nullptr;
  };

  ArrayBuilder();
  ArrayBuilder(ArrayBuilder&&) noexcept;
  auto operator=(ArrayBuilder&&) noexcept -> ArrayBuilder&;
  ArrayBuilder(const ArrayBuilder&) = delete;
  auto operator=(const ArrayBuilder&) -> ArrayBuilder& = delete;
  ~ArrayBuilder();

  auto list() -> ListBuilder;
  auto skip() -> void;
  auto skip_n(storage::Index count) -> void;
  /// Returns the number of lists, including a list that is still being built.
  auto length() const -> storage::Index;
  /// Removes the last list, closing it first if still open, and returns it.
  auto take_last() -> Data;
  auto finish() -> Array<List>;

private:
  auto finish_last_row() -> void;

  struct Storage;
  std::unique_ptr<Storage> storage_;
};

// Explicit instantiation declarations matching the explicit instantiation
// definitions in `list_array_builder.cpp`, so including translation units
// know a definition exists elsewhere (`-Wundefined-func-template`).
extern template auto ArrayBuilder<List>::ListBuilder::data(bool) -> void;
extern template auto ArrayBuilder<List>::ListBuilder::data(int64_t) -> void;
extern template auto ArrayBuilder<List>::ListBuilder::data(uint64_t) -> void;
extern template auto ArrayBuilder<List>::ListBuilder::data(double) -> void;
extern template auto
  ArrayBuilder<List>::ListBuilder::data<std::string_view>(std::string_view)
    -> void;
extern template auto ArrayBuilder<List>::ListBuilder::data(blob_view) -> void;
extern template auto ArrayBuilder<List>::ListBuilder::data(ip) -> void;
extern template auto ArrayBuilder<List>::ListBuilder::data(subnet) -> void;
extern template auto ArrayBuilder<List>::ListBuilder::data<time>(time) -> void;
extern template auto ArrayBuilder<List>::ListBuilder::data(duration) -> void;

} // namespace tenzir::nova
