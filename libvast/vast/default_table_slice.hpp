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

#include "vast/aliases.hpp"     // for vector
#include "vast/fwd.hpp"         // for table_slice_header, value_index
#include "vast/table_slice.hpp" // for table_slice
#include "vast/type.hpp"        // for record_type
#include "vast/view.hpp"        // for data_view

#include <caf/atom.hpp>              // for atom, atom_value
#include <caf/error.hpp>             // for error
#include <caf/intrusive_cow_ptr.hpp> // for intrusive_cow_ptr

#include <vector> // for vector

namespace caf {
class deserializer;
}
namespace caf {
class serializer;
}
namespace vast {

/// The default implementation of `table_slice`.
class default_table_slice : public table_slice {
public:
  // -- friends ----------------------------------------------------------------

  friend class default_table_slice_builder;

  // -- constants --------------------------------------------------------------

  static constexpr caf::atom_value class_id = caf::atom("default");

  // -- static factory functions -----------------------------------------------

  static table_slice_ptr make(table_slice_header header);

  static table_slice_ptr make(record_type layout,
                              const std::vector<vector>& rows);

  // -- factory functions ------------------------------------------------------

  default_table_slice* copy() const final;

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
  const vector& container() const noexcept {
    return xs_;
  }

protected:
  explicit default_table_slice(table_slice_header header);

private:
  vector xs_;
};

/// @relates default_table_slice
using default_table_slice_ptr = caf::intrusive_cow_ptr<default_table_slice>;

} // namespace vast
