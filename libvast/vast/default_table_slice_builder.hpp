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

#include "vast/data.hpp"
#include "vast/default_table_slice.hpp"
#include "vast/table_slice_builder.hpp"

namespace vast {

/// The default implementation of `table_slice_builder`.
class default_table_slice_builder : public table_slice_builder {
public:
  // -- member types -----------------------------------------------------------

  using super = table_slice_builder;

  // -- constructors, destructors, and assignment operators --------------------

  default_table_slice_builder(record_type layout);

  // -- factory functions ------------------------------------------------------

  static table_slice_builder_ptr make(record_type layout);

  // -- properties -------------------------------------------------------------

  bool append(data x);

  bool add(data_view x) override;

  table_slice_ptr finish() override;

  size_t rows() const noexcept override;

  void reserve(size_t num_rows) override;

  caf::atom_value implementation_id() const noexcept override;

protected:
  // -- utility functions ------------------------------------------------------

  /// Allocates `slice_` and resets related state if necessary.
  void lazy_init();

  // -- member variables -------------------------------------------------------

  std::vector<data> row_;
  size_t col_;
  std::unique_ptr<default_table_slice> slice_;
};

} // namespace vast
