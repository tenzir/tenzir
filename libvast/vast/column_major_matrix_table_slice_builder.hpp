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

#include "vast/aliases.hpp"
#include "vast/data.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"

namespace vast {

class column_major_matrix_table_slice_builder final
  : public table_slice_builder {
public:
  // -- member types -----------------------------------------------------------

  using super = table_slice_builder;

  column_major_matrix_table_slice_builder(record_type layout);

  virtual ~column_major_matrix_table_slice_builder();

  // -- factory functions ------------------------------------------------------

  /// @returns a table slice builder instance.
  static table_slice_builder_ptr make(record_type layout);

  /// @returns a default-constructed table slice instance.
  static table_slice_ptr make_slice(record_type layout, table_slice::size_type);

  // -- properties -------------------------------------------------------------

  bool append(data x);

  bool add(data_view x) override;

  table_slice_ptr finish() override;

  size_t rows() const noexcept override;

  void reserve(size_t num_rows) override;

  caf::atom_value implementation_id() const noexcept override;

  static caf::atom_value get_implementation_id() noexcept;

private:
  // -- member variables -------------------------------------------------------

  /// Current row index.
  size_t col_;

  /// Number of complete rows.
  size_t rows_;

  /// Elements by column.
  std::vector<vector> columns_;
};

} // namespace vast
