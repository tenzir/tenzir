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

#include <unordered_map>

#include "vast/format/reader.hpp"

namespace vast::format {

/// Base class for readers that deal with multiple layouts.
class multi_layout_reader : public reader {
public:
  explicit multi_layout_reader(caf::atom_value table_slice_type);

  ~multi_layout_reader() override;

protected:
  /// Finishes the current slices in `builder_ptr` before returning with
  /// `result`. Returns a different error `builder_ptr->finish()` fails.
  caf::error finish(consumer& f, table_slice_builder_ptr& builder_ptr,
                    caf::error result = caf::none);

  /// Finishes all current slices before returning with `result`. Returns a
  /// different error if any `finish()` on a table slice builder fails.
  caf::error finish(consumer& f, caf::error result = caf::none);

  /// @returns a table slice builder for given type, creating it on-the-fly is
  ///          necessary.
  table_slice_builder_ptr builder(const type& t);

  std::unordered_map<type, table_slice_builder_ptr> builders_;
};

} // namespace vast::format
