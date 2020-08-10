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
#include "vast/fwd.hpp"
#include "vast/table_slice.hpp"

namespace vast {

/// An implementation of `table_slice` that uses CAF for serialization.
class caf_table_slice : public table_slice {
public:
  // -- friends ----------------------------------------------------------------

  friend class caf_table_slice_builder;

  // -- constants --------------------------------------------------------------

  static constexpr caf::atom_value class_id = caf::atom("caf");

  // -- static factory functions -----------------------------------------------

  static table_slice_ptr make(table_slice_header header);

  static table_slice_ptr
  make(record_type layout, const std::vector<list>& rows);

  // -- factory functions ------------------------------------------------------

  caf_table_slice* copy() const final;

  // -- persistence ------------------------------------------------------------

  caf::error serialize(caf::serializer& sink) const final;

  caf::error deserialize(caf::deserializer& source) final;

  // -- visitation -------------------------------------------------------------

  /// Applies all values in column `col` to `idx`.
  void append_column_to_index(size_type col, value_index& idx) const final;

  // -- properties -------------------------------------------------------------

  data_view at(size_type row, size_type col) const final;

  caf::atom_value implementation_id() const noexcept override;

  /// @returns the container for storing table slice rows.
  const list& container() const noexcept {
    return xs_;
  }

protected:
  explicit caf_table_slice(table_slice_header header);

private:
  list xs_;
};

/// @relates caf_table_slice
using caf_table_slice_ptr = caf::intrusive_cow_ptr<caf_table_slice>;

} // namespace vast
