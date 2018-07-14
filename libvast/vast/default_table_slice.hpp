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
#include "vast/fwd.hpp"
#include "vast/table_slice.hpp"

namespace vast {

/// The default implementation of `table_slice`.
class default_table_slice final : public table_slice {
public:
  // -- friends ----------------------------------------------------------------

  friend default_table_slice_builder;

  // -- constructors, destructors, and assignment operators --------------------

  default_table_slice(const default_table_slice&) = default;

  explicit default_table_slice(record_type layout);

  // -- factory functions ------------------------------------------------------

  table_slice_ptr clone() const final;

  // -- persistence ------------------------------------------------------------

  caf::error save(caf::serializer& sink) const final;

  caf::error load(caf::deserializer& source) final;

  // -- static factory functions -----------------------------------------------

  /// Constructs a builder that generates a default_table_slice.
  /// @param layout The layout of the table_slice.
  /// @returns The builder instance.
  static table_slice_builder_ptr make_builder(record_type layout);

  static table_slice_ptr make(record_type layout,
                              const std::vector<vector>& rows);

  // -- properties -------------------------------------------------------------

  caf::optional<data_view> at(size_type row, size_type col) const final;

  caf::atom_value implementation_id() const noexcept final;

private:
  // -- member variables -------------------------------------------------------

  std::vector<data> xs_;
};

/// @relates default_table_slice
using default_table_slice_ptr = caf::intrusive_ptr<default_table_slice>;

} // namespace vast
