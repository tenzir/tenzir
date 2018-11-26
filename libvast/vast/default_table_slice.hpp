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

#include <caf/atom.hpp>

#include "vast/aliases.hpp"
#include "vast/data.hpp"
#include "vast/fwd.hpp"
#include "vast/table_slice.hpp"

namespace vast {

/// The default implementation of `table_slice`.
class default_table_slice : public table_slice {
public:
  // -- friends ----------------------------------------------------------------

  friend class default_table_slice_builder;

  // -- constants --------------------------------------------------------------

  static constexpr caf::atom_value class_id = caf::atom("TS_Default");

  // -- constructors, destructors, and assignment operators --------------------

  default_table_slice(const default_table_slice&) = default;

  explicit default_table_slice(record_type layout);

  // -- factory functions ------------------------------------------------------

  default_table_slice* copy() const final;

  // -- persistence ------------------------------------------------------------

  caf::error serialize(caf::serializer& sink) const final;

  caf::error deserialize(caf::deserializer& source) final;

  // -- static factory functions -----------------------------------------------

  static table_slice_ptr make(record_type layout,
                              const std::vector<vector>& rows);

  // -- properties -------------------------------------------------------------

  data_view at(size_type row, size_type col) const final;

  caf::atom_value implementation_id() const noexcept;

  /// @returns the container for storing table slice rows.
  const vector& container() const noexcept {
    return xs_;
  }

private:
  // -- member variables -------------------------------------------------------

  vector xs_;
};

/// @relates default_table_slice
using default_table_slice_ptr = caf::intrusive_cow_ptr<default_table_slice>;

} // namespace vast
