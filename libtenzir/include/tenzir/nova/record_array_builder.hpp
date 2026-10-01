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
#include "tenzir/option.hpp"

#include <memory>
#include <string_view>

namespace tenzir::nova {

class EventBuilder;
class FieldBuilder;

template <>
class ArrayBuilder<Record> {
public:
  class RecordBuilder {
  public:
    /// Returns the slot for `name` in the open row. Requesting a field that
    /// the open row already holds replaces its value.
    auto field(std::string_view name) -> FieldBuilder;

  private:
    friend class ArrayBuilder;
    friend class EventBuilder;
    explicit RecordBuilder(ArrayBuilder* parent);
    /// Returns the slot for `name` in the open row. If the row already holds
    /// the field, removes its value and stores it in `previous`.
    auto take_field(std::string_view name, Option<Data>& previous)
      -> FieldBuilder;
    /// Returns the record that the open row holds for `name`, if it is still
    /// being built.
    auto open_record_field(std::string_view name) -> Option<RecordBuilder>;
    /// Returns the record that ends the list which the open row holds for
    /// `name`, if both are still being built.
    auto open_list_record_field(std::string_view name) -> Option<RecordBuilder>;
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
  auto pop_skipped(storage::Index count) -> void;
  /// Returns the number of rows, including a row that is still being built.
  auto length() const -> storage::Index;
  /// Removes the last row, closing it first if still open, and returns it.
  /// The row must hold a record.
  auto take_last() -> Data;
  auto finish() -> Array<Record>;

private:
  friend class EventBuilder;
  friend class UnionArrayBuilder;

  /// Returns the last row if it is still being built.
  auto reopen() -> Option<RecordBuilder>;

  auto finish_last_row() -> void;
  /// Removes the absent rows of fields that extend past the finished rows.
  auto pop_padded_fields() -> void;

  struct Storage;
  std::unique_ptr<Storage> storage_;
};

} // namespace tenzir::nova
