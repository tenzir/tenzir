//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
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
class arrow_table_slice_builder final : public table_slice_builder {
public:
  // -- constructors, destructors, and assignment operators --------------------

  /// Constructs an Arrow table slice builder instance.
  /// @param layout The layout of the slice.
  /// @param initial_buffer_size The buffer size the builder starts with.
  /// @returns A table_slice_builder instance.
  static table_slice_builder_ptr
  make(type layout, size_t initial_buffer_size = default_buffer_size);

  /// Destroys an Arrow table slice builder.
  ~arrow_table_slice_builder() noexcept override;

  // -- properties -------------------------------------------------------------

  [[nodiscard]] table_slice finish() override;

  /// @pre `record_batch->schema()->Equals(make_experimental_schema(layout))``
  [[nodiscard]] table_slice static create(
    const std::shared_ptr<arrow::RecordBatch>& record_batch, type schema = {},
    table_slice::serialize serialize = table_slice::serialize::no,
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
  explicit arrow_table_slice_builder(type layout, size_t initial_buffer_size
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

// -- column builder helpers --------------------------------------------------

arrow::Status
append_builder(const bool_type&, type_to_arrow_builder_t<bool_type>& builder,
               const view<type_to_data_t<bool_type>>& view) noexcept;

arrow::Status
append_builder(const integer_type&,
               type_to_arrow_builder_t<integer_type>& builder,
               const view<type_to_data_t<integer_type>>& view) noexcept;

arrow::Status
append_builder(const count_type&, type_to_arrow_builder_t<count_type>& builder,
               const view<type_to_data_t<count_type>>& view) noexcept;

arrow::Status
append_builder(const real_type&, type_to_arrow_builder_t<real_type>& builder,
               const view<type_to_data_t<real_type>>& view) noexcept;

arrow::Status
append_builder(const duration_type&,
               type_to_arrow_builder_t<duration_type>& builder,
               const view<type_to_data_t<duration_type>>& view) noexcept;

arrow::Status
append_builder(const time_type&, type_to_arrow_builder_t<time_type>& builder,
               const view<type_to_data_t<time_type>>& view) noexcept;

arrow::Status
append_builder(const string_type&,
               type_to_arrow_builder_t<string_type>& builder,
               const view<type_to_data_t<string_type>>& view) noexcept;

arrow::Status
append_builder(const pattern_type&,
               type_to_arrow_builder_t<pattern_type>& builder,
               const view<type_to_data_t<pattern_type>>& view) noexcept;

arrow::Status
append_builder(const address_type&,
               type_to_arrow_builder_t<address_type>& builder,
               const view<type_to_data_t<address_type>>& view) noexcept;

arrow::Status
append_builder(const subnet_type&,
               type_to_arrow_builder_t<subnet_type>& builder,
               const view<type_to_data_t<subnet_type>>& view) noexcept;

arrow::Status
append_builder(const enumeration_type&,
               type_to_arrow_builder_t<enumeration_type>& builder,
               const view<type_to_data_t<enumeration_type>>& view) noexcept;

arrow::Status
append_builder(const list_type& hint,
               type_to_arrow_builder_t<list_type>& builder,
               const view<type_to_data_t<list_type>>& view) noexcept;

arrow::Status
append_builder(const map_type& hint, type_to_arrow_builder_t<map_type>& builder,
               const view<type_to_data_t<map_type>>& view) noexcept;

arrow::Status
append_builder(const record_type& hint,
               type_to_arrow_builder_t<record_type>& builder,
               const view<type_to_data_t<record_type>>& view) noexcept;

template <type_or_concrete_type Type>
arrow::Status
append_builder(const Type& hint,
               std::same_as<arrow::ArrayBuilder> auto& builder,
               const std::same_as<data_view> auto& view) noexcept {
  if (caf::holds_alternative<caf::none_t>(view))
    return builder.AppendNull();
  if constexpr (concrete_type<Type>) {
    return append_builder(hint,
                          caf::get<type_to_arrow_builder_t<Type>>(builder),
                          caf::get<vast::view<type_to_data_t<Type>>>(view));
  } else {
    auto f
      = [&]<concrete_type ResolvedType>(const ResolvedType& hint) noexcept {
          return append_builder(
            hint, caf::get<type_to_arrow_builder_t<ResolvedType>>(builder),
            caf::get<vast::view<type_to_data_t<ResolvedType>>>(view));
        };
    return caf::visit(f, hint);
  }
}

} // namespace vast
