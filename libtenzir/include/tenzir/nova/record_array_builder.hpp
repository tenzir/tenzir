//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/nova/array_base.hpp"
#include "tenzir/nova/array_builder_base.hpp"

#include <memory>
#include <string_view>

namespace tenzir::nova {

class FieldBuilder;

template <>
class ArrayBuilder<Record> {
public:
  class RecordBuilder {
  public:
    /// Returns the slot for `name` in the open row. Repeating a name within
    /// the same row upgrades the existing value instead of adding a column
    /// entry, see `FieldBuilder`.
    auto field(std::string_view name) -> FieldBuilder;

  private:
    friend class ArrayBuilder;
    explicit RecordBuilder(ArrayBuilder* parent);
    ArrayBuilder* parent_ = nullptr;
  };

  ArrayBuilder();
  ArrayBuilder(ArrayBuilder&&) noexcept;
  auto operator=(ArrayBuilder&&) noexcept -> ArrayBuilder&;
  ArrayBuilder(const ArrayBuilder&) = delete;
  auto operator=(const ArrayBuilder&) -> ArrayBuilder& = delete;
  ~ArrayBuilder();

  auto record() -> RecordBuilder;
  auto skip() -> void;
  auto skip_n(storage::Index count) -> void;
  /// Returns the number of rows, including a row that is still being built.
  auto length() const -> storage::Index;
  /// Removes the last row, closing it first if still open, and returns it.
  /// The row must hold a record.
  auto take_last() -> Data;
  auto finish() -> Array<Record>;

private:
  auto finish_last_row() -> void;

  struct Storage;
  std::unique_ptr<Storage> storage_;
};

} // namespace tenzir::nova
