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
    const std::shared_ptr<arrow::RecordBatch>& record_batch,
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

  /// A flattened representation of the schema that is iterated over when
  /// calling add.
  std::vector<record_type::leaf_view> leaves_;
  std::vector<record_type::leaf_view>::iterator current_leaf_;

  /// Number of filled rows.
  size_t num_rows_ = 0;

  /// The serialized layout can be cached because every builder instance only
  /// produces slices of a single layout.
  mutable std::vector<char> serialized_layout_cache_;

  /// Schema of the Record Batch corresponding to the layout.
  std::shared_ptr<arrow::Schema> schema_ = {};

  /// Underlying Arrow builder for record batches.
  std::shared_ptr<arrow::ArrayBuilder> arrow_builder_;

  /// The underlying FlatBuffers builder.
  flatbuffers::FlatBufferBuilder builder_;
};

} // namespace vast
