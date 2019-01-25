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

namespace vast::format {

/// Base class for readers that only have a single layout at any point in time.
class single_layout_reader : public reader {
public:
  explicit single_layout_reader(caf::atom_value table_slice_type);

  ~single_layout_reader() override;

protected:
  /// Finishes the current slice before returning with `result`. Returns a
  /// different error if `builder_->finish()` fails.
  caf::error finish(consumer& f, caf::error result = caf::none);

  /// Tries to create a new table slice builder from given layout.
  bool reset_builder(record_type layout);

  /// Stores the current builder instance.
  table_slice_builder_ptr builder_;
};

} // namespace vast::format
