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

// -- v1 includes --------------------------------------------------------------

#include "vast/byte.hpp"
#include "vast/fwd.hpp"
#include "vast/msgpack_builder.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/table_slice_encoding.hpp"

#include <vector>

// -- v0 includes --------------------------------------------------------------

#include "vast/msgpack_builder.hpp"

#include <vector>

#include <vast/byte.hpp>
#include <vast/table_slice.hpp>
#include <vast/table_slice_builder.hpp>

namespace vast {

namespace v1 {

class msgpack_table_slice_builder final : public table_slice_builder {
public:
  // -- types and constants ----------------------------------------------------

#if VAST_ENABLE_ASSRTIONS
  using input_validation_policy = msgpack::input_validation;
#else
  using input_validation_policy = msgpack::no_input_validation;
#endif

  // -- constructors, destructors, and assignment operators --------------------

  /// Constructs a table slice builder from a layout.
  explicit msgpack_table_slice_builder(record_type layout) noexcept;

  /// Destroys a MessagePack table slice builder.
  ~msgpack_table_slice_builder() noexcept override;

  // -- factory facade ---------------------------------------------------------

  /// This implementation builds MessagePack-encoded table slices.
  static constexpr inline auto implementation_id
    = table_slice_encoding::msgpack;

  // -- properties -------------------------------------------------------------

  /// @returns The current number of rows in the table slice.
  table_slice::size_type rows() const noexcept override;

  /// @returns An identifier for the implementing class.
  table_slice_encoding encoding() const noexcept override;

  /// Enables the table slice builder to allocate sufficient storage.
  /// @param num_rows The number of rows to allocate storage for.
  void reserve(table_slice::size_type num_rows) override;

private:
  // -- implementation details -------------------------------------------------

  /// Adds data to the builder.
  /// @param x The data to add.
  /// @returns `true` on success.
  bool add_impl(data_view x) override;

  /// Constructs a table_slice from the currently accumulated state.
  /// @returns A table slice from the accumulated calls to add, or an error.
  caf::expected<chunk_ptr> finish_impl() override;

  /// Reset the builder state.
  void reset_impl() override;

  /// The current column index.
  table_slice::size_type column_ = 0;

  /// Offses from the beginning of the buffer to each row.
  std::vector<uint64_t> offset_table_ = {};

  /// The buffer that contains the MessagePack data.
  std::vector<byte> buffer_ = {};

  /// The underlying MessagePack builder.
  msgpack::builder<input_validation_policy> msgpack_builder_;
};

} // namespace v1

inline namespace v0 {

class msgpack_table_slice_builder final : public vast::table_slice_builder {
public:
  // -- member types -----------------------------------------------------------

  using super = vast::table_slice_builder;

  // -- class properties -------------------------------------------------------

  /// The default size of the buffer that the builder works with.
  static constexpr size_t default_buffer_size = 8192;

  /// @returns `msgpack_table_slice::class_id`
  static caf::atom_value get_implementation_id() noexcept;

  // -- factory functions ------------------------------------------------------

  /// @returns a table_slice_builder instance.
  static vast::table_slice_builder_ptr
  make(vast::record_type layout, size_t initial_buffer_size
                                 = default_buffer_size);

  // -- constructors, destructors, and assignment operators --------------------

  /// Constructs a MessagePack table slice.
  /// @param layout The layout of the slice.
  /// @param initial_buffer_size The buffer size the builder starts with.
  explicit msgpack_table_slice_builder(vast::record_type layout,
                                       size_t initial_buffer_size
                                       = default_buffer_size);

  ~msgpack_table_slice_builder() override;

  // -- properties -------------------------------------------------------------

  [[nodiscard]] vast::table_slice_ptr finish() override;

  size_t rows() const noexcept override;

  caf::atom_value implementation_id() const noexcept override;

  template <class Inspector>
  friend auto inspect(Inspector& f, msgpack_table_slice_builder& x) {
    return f(caf::meta::type_name("vast.msgpack_table_slice_builder"), x.col_,
             x.offset_table_, x.buffer_, x.builder_);
  }

protected:
  bool add_impl(vast::data_view x) override;

private:
  // -- member variables -------------------------------------------------------

  /// Current column index.
  size_t col_;

  /// Offsets from the beginning of the buffer to each row.
  std::vector<size_t> offset_table_;

  /// Elements encoded in MessagePack format.
  std::vector<vast::byte> buffer_;

#if VAST_ENABLE_ASSERTIONS
  msgpack::builder<msgpack::input_validation> builder_;
#else
  msgpack::builder<msgpack::no_input_validation> builder_;
#endif
};

} // namespace v0

} // namespace vast
