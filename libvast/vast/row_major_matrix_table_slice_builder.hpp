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

#include <vector>

#include "vast/data.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"

namespace vast {

class row_major_matrix_table_slice_builder final : public table_slice_builder {
public:
  // -- member types -----------------------------------------------------------

  using super = table_slice_builder;

  // -- class properties -------------------------------------------------------

  static caf::atom_value get_implementation_id() noexcept;

  // -- constructors, destructors, and assignment operators --------------------

  row_major_matrix_table_slice_builder(record_type layout);

  ~row_major_matrix_table_slice_builder() override;

  // -- factory functions ------------------------------------------------------

  /// @returns a table slice builder instance.
  static table_slice_builder_ptr make(record_type layout);

  // -- properties -------------------------------------------------------------

  bool append(data x);

  bool add(data_view x) override;

  table_slice_ptr finish() override;

  size_t rows() const noexcept override;

  void reserve(size_t num_rows) override;

  caf::atom_value implementation_id() const noexcept override;

private:
  // -- member variables -------------------------------------------------------

  /// Current row index.
  size_t col_;

  /// Elements in row-major order.
  std::vector<data> elements_;
};

} // namespace vast
