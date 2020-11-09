/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include "vast/byte.hpp"
#include "vast/msgpack_builder.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"

#include <vector>

namespace vast {

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
  static table_slice_builder_ptr
  make(record_type layout, size_t initial_buffer_size = default_buffer_size);

  /// Destroys a MessagePack table slice builder.
  ~msgpack_table_slice_builder() override;

  // -- properties -------------------------------------------------------------

  /// @returns A table slice from the accumulated calls to add.
  [[nodiscard]] table_slice finish() override;

  /// @returns The current number of rows in the table slice.
  size_t rows() const noexcept override;

  /// @returns An identifier for the implementing class.
  caf::atom_value implementation_id() const noexcept override;

  /// Allows The table slice builder to allocate sufficient storage.
  /// @param `num_rows` The number of rows to allocate storage for.
  virtual void reserve(size_t num_rows) override;

  template <class Inspector>
  friend auto inspect(Inspector& f, msgpack_table_slice_builder& x) ->
    typename Inspector::result_type {
    return f(caf::meta::type_name("vast.msgpack_table_slice_builder"),
             static_cast<table_slice_builder&>(x), x.column_, x.offset_table_,
             x.buffer_, x.msgpack_builder_);
  }

private:
  // -- implementation details--------------------------------------------------

  /// Constructs a MessagePack table slice builder.
  /// @param layout The layout of the slice.
  /// @param initial_buffer_size The buffer size the builder starts with.
  explicit msgpack_table_slice_builder(record_type layout,
                                       size_t initial_buffer_size
                                       = default_buffer_size);

  /// Adds data to the builder.
  /// @param x The data to add.
  /// @returns `true` on success.
  bool add_impl(data_view x) override;

  /// Current column index.
  size_t column_ = 0;

  /// Offsets from the beginning of the buffer to each row.
  std::vector<size_t> offset_table_ = {};

  /// Elements encoded in MessagePack format.
  std::vector<byte> buffer_ = {};

  /// The underlying MessagePack builder.
  msgpack::builder<input_validation> msgpack_builder_;
};

} // namespace vast
