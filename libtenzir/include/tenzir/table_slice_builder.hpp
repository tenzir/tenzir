//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/table_slice.hpp"

#include <arrow/type.h>
#include <flatbuffers/flatbuffers.h>

#include <memory>
#include <span>
#include <vector>

namespace tenzir {

/// A builder for table slices that store elements encoded in the
/// [Arrow](https://arrow.apache.org) format.
class table_slice_builder final {
public:
  /// The default size of the buffer that the builder works with.
  static constexpr size_t default_buffer_size = 8192;

  // -- constructors, destructors, and assignment operators --------------------

  /// Constructs an Arrow table slice builder instance.
  /// @param schema The schema of the slice.
  /// @param initial_buffer_size The buffer size the builder starts with.
  /// @returns A table_slice_builder instance.
  explicit table_slice_builder(type schema, size_t initial_buffer_size
                                            = default_buffer_size);

  /// Destroys an Arrow table slice builder.
  ~table_slice_builder() noexcept;

  table_slice_builder(table_slice_builder const&) = delete;
  table_slice_builder& operator=(table_slice_builder const&) = delete;
  table_slice_builder(table_slice_builder&&) = default;
  table_slice_builder& operator=(table_slice_builder&&) = default;

  // -- properties -------------------------------------------------------------

  /// Calls `add(x)` as long as `x` is not a vector, otherwise calls `add(y)`
  /// for each `y` in `x`.
  [[nodiscard]] bool recursive_add(const data& x, const type& t);

  /// Adds data to the builder.
  /// @param x The data to add.
  /// @param xs... The data to add.
  /// @returns `true` on success.
  template <class T, class... Ts>
  [[nodiscard]] bool add(const T& x, const Ts&... xs) {
    if constexpr (sizeof...(xs) == 0) {
      if constexpr (std::is_same_v<T, data_view>) {
        return add(x);
      } else if constexpr (caf::detail::tl_contains<data_view::types, T>::value) {
        return add(data_view{x});
      } else {
        return add(make_view(x));
      }
    } else {
      return add(x) && (add(xs) && ...);
    }
  }

  /// Adds data to the builder.
  /// @param x The data to add.
  /// @returns `true` on success.
  bool add(data_view x);

  [[nodiscard]] table_slice finish();

  /// Creates a table slice from a record batch.
  /// @pre `record_batch->schema()->Equals(make_experimental_schema(schema))``
  /// @param batch A pre-existing record batch.
  /// @param schema Tenzir schema matching the record batch schema. Parameter
  ///     is optional and derived from the record batch if not provided.
  [[nodiscard]] table_slice static create(
    const std::shared_ptr<arrow::RecordBatch>& record_batch, type schema = {},
    table_slice::serialize serialize = table_slice::serialize::no,
    size_t initial_buffer_size = default_buffer_size);

  /// @returns The number of columns in the table slice.
  size_t columns() const noexcept;

  /// @returns The current number of rows in the table slice.
  size_t rows() const noexcept;

  /// Allows The table slice builder to allocate sufficient storage.
  /// @param `num_rows` The number of rows to allocate storage for.
  void reserve(size_t num_rows);

  /// @returns The table schema.
  const type& schema() const noexcept;

private:
  // -- implementation details -------------------------------------------------

  // The Tenzir schema this builder was created from.
  type schema_;

  /// A flattened representation of the schema that is iterated over when
  /// calling add.
  std::vector<record_type::leaf_view> leaves_;
  std::vector<record_type::leaf_view>::iterator current_leaf_;

  /// Number of filled rows.
  size_t num_rows_ = 0;

  /// The serialized schema can be cached because every builder instance only
  /// produces slices of a single schema.
  mutable std::vector<char> serialized_schema_cache_;

  /// Schema of the Record Batch corresponding to the schema.
  std::shared_ptr<arrow::Schema> arrow_schema_ = {};

  /// Underlying Arrow builder for record batches.
  std::shared_ptr<arrow::ArrayBuilder> arrow_builder_;

  /// The underlying FlatBuffers builder.
  flatbuffers::FlatBufferBuilder builder_;
};

using table_slice_builder_ptr = std::shared_ptr<table_slice_builder>;

// -- column builder helpers --------------------------------------------------

arrow::Status
append_builder(const null_type&, type_to_arrow_builder_t<null_type>& builder,
               const view<type_to_data_t<null_type>>& view) noexcept;

arrow::Status
append_builder(const bool_type&, type_to_arrow_builder_t<bool_type>& builder,
               const view<type_to_data_t<bool_type>>& view) noexcept;

arrow::Status
append_builder(const int64_type&, type_to_arrow_builder_t<int64_type>& builder,
               const view<type_to_data_t<int64_type>>& view) noexcept;

arrow::Status
append_builder(const uint64_type&,
               type_to_arrow_builder_t<uint64_type>& builder,
               const view<type_to_data_t<uint64_type>>& view) noexcept;

arrow::Status
append_builder(const double_type&,
               type_to_arrow_builder_t<double_type>& builder,
               const view<type_to_data_t<double_type>>& view) noexcept;

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
append_builder(const blob_type&, type_to_arrow_builder_t<blob_type>& builder,
               const view<type_to_data_t<blob_type>>& view) noexcept;

arrow::Status
append_builder(const ip_type&, type_to_arrow_builder_t<ip_type>& builder,
               const view<type_to_data_t<ip_type>>& view) noexcept;

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
  if (is<caf::none_t>(view)) {
    return builder.AppendNull();
  }
  if constexpr (concrete_type<Type>) {
    return append_builder(hint, as<type_to_arrow_builder_t<Type>>(builder),
                          as<tenzir::view<type_to_data_t<Type>>>(view));
  } else {
    auto f
      = [&]<concrete_type ResolvedType>(const ResolvedType& hint) noexcept {
          return append_builder(
            hint, as<type_to_arrow_builder_t<ResolvedType>>(builder),
            as<tenzir::view<type_to_data_t<ResolvedType>>>(view));
        };
    return match(hint, f);
  }
}

auto append_array_slice(arrow::ArrayBuilder& builder, const type& ty,
                        const arrow::Array& array, int64_t begin, int64_t count)
  -> arrow::Status;

template <concrete_type Ty>
auto append_array_slice(type_to_arrow_builder_t<Ty>& builder, const Ty& ty,
                        const type_to_arrow_array_t<Ty>& array, int64_t begin,
                        int64_t count) -> arrow::Status;

template <type_or_concrete_type Ty>
auto append_array(type_to_arrow_builder_t<Ty>& builder, const Ty& ty,
                  const type_to_arrow_array_t<Ty>& array) -> arrow::Status {
  return append_array_slice(builder, ty, array, 0, array.length());
}

} // namespace tenzir
