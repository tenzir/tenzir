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

#include "vast/format/reader.hpp"
#include "vast/table_slice_builder.hpp"

namespace vast::format {

/// Base class for readers that only have a single layout at any point in time.
class single_layout_reader : public reader {
public:
  explicit single_layout_reader(caf::atom_value table_slice_type);

  ~single_layout_reader() override;

protected:
  /// Convenience function for finishing our current table slice in `builder_`
  /// before reporting an error. Usually simply returns `result` after
  /// finishing the slice, however, an error in finishing the slice "overrides"
  /// `result`.
  /// @param f Consumer for the finished slice.
  /// @param result Current status of the parent context, usually returned
  ///               unmodified.
  /// @returns `result`, unless any `finish()` call fails.
  caf::error finish(consumer& f, caf::error result = caf::none);

  /// Tries to create a new table slice builder from given layout.
  bool reset_builder(record_type layout);

  /// Stores the current builder instance.
  table_slice_builder_ptr builder_;
};

} // namespace vast::format
