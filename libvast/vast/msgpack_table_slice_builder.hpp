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

#include "vast/msgpack_builder.hpp"

#include <vector>

#include <vast/byte.hpp>
#include <vast/table_slice.hpp>
#include <vast/table_slice_builder.hpp>

namespace vast {

class msgpack_table_slice_builder final : public vast::table_slice_builder {
public:
  // -- member types -----------------------------------------------------------

  using super = vast::table_slice_builder;

  // -- class properties -------------------------------------------------------

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

  [[nodiscard]] vast::table_slice finish() override;

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

} // namespace vast
