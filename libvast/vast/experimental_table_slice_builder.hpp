//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"

#include <arrow/type.h>
#include <flatbuffers/flatbuffers.h>

#include <memory>
#include <span>
#include <vector>

namespace vast {

/// A builder for table slices that store elements encoded in the
/// [Arrow](https://arrow.apache.org) format.
class experimental_table_slice_builder final : public table_slice_builder {
public:
  // -- member types -----------------------------------------------------------

  /// Wraps a type-specific Arrow builder.
  struct column_builder {
    /// Destroys an Arrow column builder.
    virtual ~column_builder() noexcept;

    /// Adds data to the column builder.
    /// @param x The data to add.
    /// @returns `true` on success.
    virtual bool add(data_view x) = 0;

    /// @returns An Arrow array from the accumulated calls to add.
    [[nodiscard]] virtual std::shared_ptr<arrow::Array> finish() = 0;

    /// @returns The underlying array builder.
    [[nodiscard]] virtual std::shared_ptr<arrow::ArrayBuilder>
    arrow_builder() const = 0;

    /// Constructs an Arrow column builder.
    /// @param t A type to create a column builder for.
    /// @param pool The Arrow memory pool to use.
    /// @returns A builder for columns of type `t`.
    static std::unique_ptr<column_builder>
    make(const type& t, arrow::MemoryPool* pool);
  };

  // -- constructors, destructors, and assignment operators --------------------

  /// Constructs an Arrow table slice builder instance.
  /// @param layout The layout of the slice.
  /// @param initial_buffer_size The buffer size the builder starts with.
  /// @returns A table_slice_builder instance.
  static table_slice_builder_ptr
  make(type layout, size_t initial_buffer_size = default_buffer_size);

  /// Destroys an Arrow table slice builder.
  ~experimental_table_slice_builder() noexcept override;

  // -- properties -------------------------------------------------------------

  [[nodiscard]] table_slice finish() override;

  /// @pre `record_batch->schema()->Equals(make_experimental_schema(layout))``
  [[nodiscard]] table_slice static create(
    const std::shared_ptr<arrow::RecordBatch>& record_batch, const type& layout,
    size_t initial_buffer_size = default_buffer_size);

  /// @returns The number of columns in the table slice.
  size_t columns() const noexcept;

  /// @returns The current number of rows in the table slice.
  size_t rows() const noexcept override;

  /// @returns An identifier for the implementing class.
  table_slice_encoding implementation_id() const noexcept override;

  /// Allows The table slice builder to allocate sufficient storage.
  /// @param `num_rows` The number of rows to allocate storage for.
  void reserve(size_t num_rows) override;

private:
  // -- implementation details -------------------------------------------------

  /// Constructs an Arrow table slice.
  /// @param layout The layout of the slice.
  /// @param initial_buffer_size The buffer size the builder starts with.
  explicit experimental_table_slice_builder(type layout,
                                            size_t initial_buffer_size
                                            = default_buffer_size);

  /// Adds data to the builder.
  /// @param x The data to add.
  /// @returns `true` on success.
  bool add_impl(data_view x) override;

  /// Current column index.
  size_t column_ = 0;

  /// Number of filled rows.
  size_t rows_ = 0;

  /// The serialized layout can be cached because every builder instance only
  /// produces slices of a single layout.
  mutable std::vector<char> serialized_layout_cache_;

  /// Schema of the Record Batch corresponding to the layout.
  std::shared_ptr<arrow::Schema> schema_ = {};

  /// Builders for columnar Arrow arrays.
  std::vector<std::unique_ptr<column_builder>> column_builders_ = {};

  /// The underlying FlatBuffers builder.
  flatbuffers::FlatBufferBuilder builder_;
};

// -- utility functions --------------------------------------------------------

/// Converts a VAST `record_type` to an Arrow `Schema`.
/// @pre `caf::holds_alternative<record_type>(t)`
/// @param t The record type to convert.
/// @returns An arrow representation of `t`.
std::shared_ptr<arrow::Schema> make_experimental_schema(const type& t);

/// Converts a VAST `type` to an Arrow `DataType`.
/// @param t The type to convert.
/// @returns An arrow representation of `t`.
std::shared_ptr<arrow::DataType> make_experimental_type(const type& t);

/// Converts a VAST `type` to an Arrow `Field`.
//  @param name The field name.
/// @param t The type to convert.
/// @param nullable Is the field nullable.
/// @returns An arrow representation of `t`.
std::shared_ptr<arrow::Field>
make_experimental_field(const record_type::field_view& field, bool nullable
                                                              = true);

/// Converts an Arrow `Schema` to a VAST `type`.
/// @param arrow_schema The Arrow schema to convert.
/// @returns A VAST type representation of `arrow_schema`.
type make_vast_type(const arrow::Schema& arrow_schema);

/// Converts an Arrow `Field` to a VAST `type`
/// @param arrow_field The arrow type to convert.
/// @return A VAST type representation of `arrow_field`
type make_vast_type(const arrow::Field& arrow_field);

} // namespace vast
