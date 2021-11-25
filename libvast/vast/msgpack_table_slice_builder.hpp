//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/msgpack_builder.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/type.hpp"

#include <flatbuffers/flatbuffers.h>

#include <cstddef>
#include <span>
#include <vector>

namespace vast {

/// A builder for table slices that store elements encoded in the
/// [MessagePack](https://msgpack.org) format.
class msgpack_table_slice_builder final : public table_slice_builder {
public:
  // -- member types -----------------------------------------------------------

#if VAST_ENABLE_ASSERTIONS
  using input_validation = msgpack::input_validation;
#else
  using input_validation = msgpack::no_input_validation;
#endif

  // -- constructors, destructors, and assignment operators --------------------

  /// Constructs a MessagePack table slice builder instance.
  /// @param layout The layout of the slice.
  /// @param initial_buffer_size The buffer size the builder starts with.
  /// @returns A table_slice_builder instance.
  static table_slice_builder_ptr
  make(type layout, size_t initial_buffer_size = default_buffer_size);

  /// Destroys a MessagePack table slice builder.
  ~msgpack_table_slice_builder() override;

  // -- properties -------------------------------------------------------------

  /// Constructs a MessagePack-encoded table slice.
  /// @returns A table slice from the accumulated calls to add.
  [[nodiscard]] table_slice finish() override;

  /// @returns The number of columns in the table slice.
  size_t columns() const noexcept;

  /// @returns The current number of rows in the table slice.
  size_t rows() const noexcept override;

  /// @returns An identifier for the implementing class.
  table_slice_encoding implementation_id() const noexcept override;

  /// Allows The table slice builder to allocate sufficient storage.
  /// @param `num_rows` The number of rows to allocate storage for.
  void reserve(size_t num_rows) override;

  template <class Inspector>
  friend auto inspect(Inspector& f, msgpack_table_slice_builder& x) ->
    typename Inspector::result_type {
    return f(caf::meta::type_name("vast.msgpack_table_slice_builder"),
             static_cast<table_slice_builder&>(x), x.column_, x.offset_table_,
             x.data_, x.msgpack_builder_, x.builder_);
  }

private:
  // -- implementation details--------------------------------------------------

  /// Constructs a MessagePack table slice builder.
  /// @param layout The layout of the slice.
  /// @param initial_buffer_size The buffer size the builder starts with.
  explicit msgpack_table_slice_builder(type layout, size_t initial_buffer_size
                                                    = default_buffer_size);

  /// Adds data to the builder.
  /// @param x The data to add.
  /// @returns `true` on success.
  bool add_impl(data_view x) override;

  /// Current column index.
  size_t column_ = 0;

  /// The flattened layout.
  type flat_layout_;

  /// Offsets from the beginning of the buffer to each row.
  std::vector<uint64_t> offset_table_ = {};

  /// Elements encoded in MessagePack format.
  std::vector<std::byte> data_ = {};

  /// The underlying MessagePack builder.
  msgpack::builder<input_validation> msgpack_builder_;

  /// The underlying FlatBuffers builder.
  flatbuffers::FlatBufferBuilder builder_;
};

} // namespace vast
